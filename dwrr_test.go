package dwrr

import (
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
