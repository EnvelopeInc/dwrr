// Package dwrr implements a Deficit Weighted Round Robin (DWRR) scheduler.
// Items have uniform cost of one, so service is measured in item counts.
package dwrr

import "sync"

// DWRR represents a Deficit Weighted Round Robin scheduler for any type.
// Each item has uniform cost one, and all methods are safe for concurrent use.
type DWRR[T any] struct {
	quantums []uint     // Configured item-count service quantum for each queue.
	deficits []uint     // Unused item-count service credit carried between rounds.
	queues   [][]T      // Array of queues, where each queue holds items of type T.
	maxTake  uint       // Maximum number of items allowed to take from each queue in one round.
	mu       sync.Mutex // Mutex protecting all scheduler state.
}

// NewDWRR creates a DWRR scheduler with count queues and a per-queue maxTake limit.
// Each queue starts with an item-count quantum of 1. A maxTake of 0 prevents
// Do from emitting items; otherwise it caps each queue's output per round.
func NewDWRR[T any](count uint, maxTake uint) *DWRR[T] {
	quantums := make([]uint, count)
	for i := range quantums {
		quantums[i] = 1
	}
	return &DWRR[T]{
		quantums: quantums,
		deficits: make([]uint, count),
		queues:   make([][]T, count),
		maxTake:  maxTake,
	}
}

// AddQueue appends a new empty queue with the default item-count quantum of 1.
// Existing queues retain their indices and scheduling state.
func (dwrr *DWRR[T]) AddQueue() {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	dwrr.queues = append(dwrr.queues, nil)
	dwrr.quantums = append(dwrr.quantums, 1)
	dwrr.deficits = append(dwrr.deficits, 0)
}

// RemoveQueue removes the last queue from the scheduler. If there are no queues,
// the function does nothing. It does not change the indices or state of the
// remaining queues.
func (dwrr *DWRR[T]) RemoveQueue() {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	if len(dwrr.queues) == 0 {
		return
	}

	last := len(dwrr.queues) - 1
	dwrr.queues[last] = nil
	dwrr.queues = dwrr.queues[:last]
	dwrr.quantums = dwrr.quantums[:last]
	dwrr.deficits = dwrr.deficits[:last]
}

// SetQuantum sets a queue's item-count service quantum, discarding any
// accumulated deficit. A zero quantum leaves the queue without service from Do
// until its quantum is changed. Invalid queue indices panic, like the other
// indexed queue operations.
func (dwrr *DWRR[T]) SetQuantum(queue uint, quantum uint) {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	dwrr.quantums[queue] = quantum
	dwrr.deficits[queue] = 0
}

// Enqueue adds items to a specific queue in FIFO order. The queue's quantum and
// existing deficit are unchanged. An invalid queue index panics.
func (dwrr *DWRR[T]) Enqueue(queue uint, items []T) {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	dwrr.queues[queue] = append(dwrr.queues[queue], items...)
}

// Dequeue removes and returns the first item from a specified queue. If the
// queue is empty, it returns nil. Removing the final item resets its deficit.
// An invalid queue index panics.
func (dwrr *DWRR[T]) Dequeue(queue uint) *T {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	if len(dwrr.queues[queue]) == 0 {
		return nil
	}

	item := dwrr.queues[queue][0]
	dwrr.queues[queue] = dwrr.queues[queue][1:]
	if len(dwrr.queues[queue]) == 0 {
		dwrr.deficits[queue] = 0
	}

	return &item
}

// DequeueAll removes and returns all items from a specified queue and resets
// its deficit to zero. An invalid queue index panics.
func (dwrr *DWRR[T]) DequeueAll(queue uint) []T {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	items := dwrr.queues[queue]
	dwrr.queues[queue] = nil
	dwrr.deficits[queue] = 0

	return items
}

// Do performs one round, visiting each queue once in index order. Each non-empty
// queue receives its configured item-count quantum, capped at its current backlog;
// it emits up to its deficit and maxTake. Unused deficit carries over while the
// queue remains non-empty and resets when it empties. Items have uniform cost 1,
// so quantum weights service by item count, not by item size or another cost.
func (dwrr *DWRR[T]) Do() [][]T {
	dwrr.mu.Lock()
	defer dwrr.mu.Unlock()

	take := make([][]T, len(dwrr.queues))

	for i, queue := range dwrr.queues {
		if len(queue) == 0 {
			dwrr.deficits[i] = 0
			continue
		}

		qlen := uint(len(queue))
		// Saturate at backlog without overflowing, and discard credit made
		// excessive by direct dequeue operations.
		if dwrr.deficits[i] >= qlen || dwrr.quantums[i] >= qlen-dwrr.deficits[i] {
			dwrr.deficits[i] = qlen
		} else {
			dwrr.deficits[i] += dwrr.quantums[i]
		}

		split := dwrr.deficits[i]
		if split > dwrr.maxTake {
			split = dwrr.maxTake
		}

		if split == 0 {
			continue
		}
		take[i] = make([]T, split)
		copy(take[i], queue[:split])
		copy(queue, queue[split:])
		clear(queue[qlen-split:])
		dwrr.queues[i] = queue[:qlen-split]
		dwrr.deficits[i] -= split
		if len(dwrr.queues[i]) == 0 {
			dwrr.deficits[i] = 0
		}
	}

	return take
}
