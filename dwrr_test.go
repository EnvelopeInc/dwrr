package dwrr

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDWRR(t *testing.T) {
	dwrr := NewDWRR[int](10, 5)
	assert.Equal(t, 10, len(dwrr.queues))
	assert.Equal(t, 10, len(dwrr.quantums))
	assert.Equal(t, 10, len(dwrr.deficits))
	assert.Equal(t, uint(1), dwrr.quantums[0])
	assert.Equal(t, uint(5), dwrr.maxTake)
}

func TestAddQueue(t *testing.T) {
	dwrr := NewDWRR[int](0, 5)
	dwrr.AddQueue()
	assert.Equal(t, 1, len(dwrr.queues))
	assert.Equal(t, 1, len(dwrr.quantums))
	assert.Equal(t, 1, len(dwrr.deficits))
	assert.Equal(t, uint(1), dwrr.quantums[0])
}

func TestRemoveQueue(t *testing.T) {
	dwrr := NewDWRR[int](2, 5)
	dwrr.SetQuantum(1, 7)
	dwrr.deficits[1] = 4
	dwrr.RemoveQueue()
	assert.Equal(t, 1, len(dwrr.queues))
	assert.Equal(t, 1, len(dwrr.quantums))
	assert.Equal(t, 1, len(dwrr.deficits))
	assert.Equal(t, uint(1), dwrr.quantums[0])
	dwrr.RemoveQueue()
	assert.Empty(t, dwrr.queues)
	assert.Empty(t, dwrr.quantums)
	assert.Empty(t, dwrr.deficits)
	dwrr.RemoveQueue() // Removing from empty is a no-op.
}

func TestEnqueueDequeue(t *testing.T) {
	dwrr := NewDWRR[int](1, 5)
	dwrr.SetQuantum(0, 4)
	dwrr.Enqueue(0, []int{1, 2, 3})
	assert.Equal(t, []int{1, 2, 3}, dwrr.queues[0])
	assert.Equal(t, uint(4), dwrr.quantums[0]) // Backlog does not change quantum.

	item := dwrr.Dequeue(0)
	assert.NotNil(t, item)
	assert.Equal(t, 1, *item)
	assert.Equal(t, []int{2, 3}, dwrr.queues[0])

	dwrr.Dequeue(0)
	dwrr.Dequeue(0)
	item = dwrr.Dequeue(0)
	assert.Nil(t, item)
	assert.Zero(t, dwrr.deficits[0])
}

func TestDequeueClearsRemovedItem(t *testing.T) {
	value := 42
	dwrr := NewDWRR[*int](1, 1)
	dwrr.Enqueue(0, []*int{&value})
	backing := dwrr.queues[0]

	item := dwrr.Dequeue(0)

	assert.NotNil(t, item)
	assert.Equal(t, &value, *item)
	assert.Nil(t, backing[0])
}

func TestDequeueAll(t *testing.T) {
	dwrr := NewDWRR[int](1, 5)
	dwrr.Enqueue(0, []int{1, 2, 3})
	dwrr.deficits[0] = 2
	items := dwrr.DequeueAll(0)
	assert.Equal(t, []int{1, 2, 3}, items)
	assert.Empty(t, dwrr.queues[0])
	assert.Zero(t, dwrr.deficits[0])
}

func TestDoMultipleRounds(t *testing.T) {
	dwrr := NewDWRR[int](2, 2)
	dwrr.Enqueue(0, []int{1, 2, 3})
	dwrr.Enqueue(1, []int{4, 5, 6})

	assert.Equal(t, [][]int{{1}, {4}}, dwrr.Do())
	assert.Equal(t, [][]int{{2}, {5}}, dwrr.Do())
	assert.Equal(t, [][]int{{3}, {6}}, dwrr.Do())
	assert.Equal(t, [][]int{nil, nil}, dwrr.Do())
}

func TestWeightedService(t *testing.T) {
	dwrr := NewDWRR[int](2, 10)
	dwrr.SetQuantum(1, 2)
	dwrr.Enqueue(0, []int{1, 2, 3, 4, 5, 6})
	dwrr.Enqueue(1, []int{11, 12, 13, 14, 15, 16})

	got := dwrr.Do()
	assert.Equal(t, [][]int{{1}, {11, 12}}, got)
	got = dwrr.Do()
	assert.Equal(t, [][]int{{2}, {13, 14}}, got)
}

func TestDeficitCarryoverWithMaxTake(t *testing.T) {
	dwrr := NewDWRR[int](1, 1)
	dwrr.SetQuantum(0, 3)
	dwrr.Enqueue(0, []int{1, 2, 3, 4, 5, 6, 7, 8})

	assert.Equal(t, [][]int{{1}}, dwrr.Do()) // deficit 2 remains
	assert.Equal(t, [][]int{{2}}, dwrr.Do()) // deficit grows to 4
	assert.Equal(t, [][]int{{3}}, dwrr.Do()) // deficit grows to 6
	assert.Equal(t, uint(5), dwrr.deficits[0])
}

func TestEmptyQueueResetsDeficit(t *testing.T) {
	dwrr := NewDWRR[int](1, 1)
	dwrr.SetQuantum(0, 3)
	dwrr.Enqueue(0, []int{1, 2, 3, 4})
	assert.Equal(t, [][]int{{1}}, dwrr.Do())
	assert.Equal(t, uint(2), dwrr.deficits[0])

	dwrr.DequeueAll(0)
	assert.Zero(t, dwrr.deficits[0])
	dwrr.Enqueue(0, []int{5, 6, 7, 8})
	assert.Equal(t, [][]int{{5}}, dwrr.Do())
	assert.Equal(t, uint(2), dwrr.deficits[0])
}

func TestSetQuantumResetsDeficit(t *testing.T) {
	dwrr := NewDWRR[int](1, 1)
	dwrr.SetQuantum(0, 5)
	dwrr.Enqueue(0, []int{1, 2, 3, 4, 5, 6, 7, 8})

	assert.Equal(t, [][]int{{1}}, dwrr.Do())
	assert.Equal(t, uint(4), dwrr.deficits[0])

	dwrr.SetQuantum(0, 0)
	assert.Zero(t, dwrr.deficits[0])
	assert.Equal(t, [][]int{nil}, dwrr.Do())

	dwrr.SetQuantum(0, 1)
	assert.Equal(t, [][]int{{2}}, dwrr.Do())
}

func TestZeroMaxTake(t *testing.T) {
	dwrr := NewDWRR[int](1, 0)
	dwrr.Enqueue(0, []int{1, 2})
	assert.Equal(t, [][]int{nil}, dwrr.Do())
	assert.Equal(t, []int{1, 2}, dwrr.queues[0])
}

func TestMaxQuantumSaturatesAtBacklog(t *testing.T) {
	dwrr := NewDWRR[int](1, 1)
	dwrr.SetQuantum(0, ^uint(0))
	dwrr.Enqueue(0, []int{1, 2, 3})

	assert.Equal(t, [][]int{{1}}, dwrr.Do())
	assert.Equal(t, uint(2), dwrr.deficits[0])
	assert.Equal(t, [][]int{{2}}, dwrr.Do())
	assert.Equal(t, uint(1), dwrr.deficits[0])
	assert.Equal(t, [][]int{{3}}, dwrr.Do())
	assert.Zero(t, dwrr.deficits[0])
}

func TestIndexedOperationsPanicOnInvalidQueue(t *testing.T) {
	operations := []struct {
		name string
		call func(*DWRR[int])
	}{
		{name: "SetQuantum", call: func(dwrr *DWRR[int]) { dwrr.SetQuantum(1, 1) }},
		{name: "Enqueue", call: func(dwrr *DWRR[int]) { dwrr.Enqueue(1, []int{1}) }},
		{name: "Dequeue", call: func(dwrr *DWRR[int]) { dwrr.Dequeue(1) }},
		{name: "DequeueAll", call: func(dwrr *DWRR[int]) { dwrr.DequeueAll(1) }},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			assert.Panics(t, func() { operation.call(NewDWRR[int](1, 1)) })
		})
	}
}

func TestConcurrentOperationsConserveItems(t *testing.T) {
	const queueCount = 4
	const itemsPerQueue = 300
	dwrr := NewDWRR[int](queueCount, 5)
	start := make(chan struct{})

	var collectedMu sync.Mutex
	collected := make([]int, 0, queueCount*itemsPerQueue)
	collect := func(items []int) {
		collectedMu.Lock()
		collected = append(collected, items...)
		collectedMu.Unlock()
	}
	collectRound := func(round [][]int) {
		for _, items := range round {
			collect(items)
		}
	}

	var producers sync.WaitGroup
	for queue := 0; queue < queueCount; queue++ {
		producers.Add(1)
		go func(queue int) {
			defer producers.Done()
			<-start
			for item := 0; item < itemsPerQueue; item++ {
				id := queue*itemsPerQueue + item
				dwrr.Enqueue(uint(queue), []int{id})
				if item%7 == 0 {
					dwrr.SetQuantum(uint(queue), uint(item%5+1))
				}
			}
		}(queue)
	}
	producersDone := make(chan struct{})

	var consumers sync.WaitGroup
	consumers.Add(1)
	go func() {
		defer consumers.Done()
		<-start
		for {
			select {
			case <-producersDone:
				return
			default:
				collectRound(dwrr.Do())
			}
		}
	}()
	for queue := 0; queue < queueCount; queue++ {
		consumers.Add(2)
		go func(queue int) {
			defer consumers.Done()
			<-start
			for {
				select {
				case <-producersDone:
					return
				default:
					if item := dwrr.Dequeue(uint(queue)); item != nil {
						collect([]int{*item})
					}
				}
			}
		}(queue)
		go func(queue int) {
			defer consumers.Done()
			<-start
			for i := 0; i < itemsPerQueue; i++ {
				collect(dwrr.DequeueAll(uint(queue)))
			}
		}(queue)
	}
	close(start)
	producers.Wait()
	close(producersDone)
	consumers.Wait()

	// Once concurrent producers and removers have stopped, drain any remaining
	// backlog through both public bulk and round operations.
	for queue := 0; queue < queueCount; queue++ {
		collect(dwrr.DequeueAll(uint(queue)))
	}
	collectRound(dwrr.Do())

	assert.Len(t, collected, queueCount*itemsPerQueue)
	seen := make(map[int]bool, len(collected))
	for _, item := range collected {
		if assert.False(t, seen[item], "item %d was returned more than once", item) {
			seen[item] = true
		}
	}
	for item := 0; item < queueCount*itemsPerQueue; item++ {
		assert.True(t, seen[item], "item %d was not returned", item)
	}
}

func TestConcurrentStructuralQueueOperations(t *testing.T) {
	const workers = 8
	const iterations = 500
	dwrr := NewDWRR[int](2, 1)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				dwrr.AddQueue()
				dwrr.Do()
				dwrr.RemoveQueue()
			}
		}()
	}
	close(start)
	wg.Wait()

	// Do is the public, lock-protected way to observe the final queue count.
	assert.Len(t, dwrr.Do(), 2)
}
