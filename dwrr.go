// Package dwrr implements a cost-aware Deficit Weighted Round Robin scheduler.
// Quantum and item costs are positive uint64 values. Scheduling order is
// determined by queue visits and FIFO heads, not by downstream execution time.
package dwrr

import (
	"errors"
	"sync"
)

const (
	// MaxCost is the largest accepted item cost.
	MaxCost = uint64(1<<63 - 1)
	// MaxQuantum is the largest accepted queue quantum.
	MaxQuantum = uint64(1<<63 - 1)
)

var (
	// ErrZeroQuantum is returned when SetQuantum is given a zero quantum.
	ErrZeroQuantum = errors.New("dwrr: quantum must be positive")
	// ErrZeroCost is returned when Enqueue receives an item with zero cost.
	ErrZeroCost = errors.New("dwrr: item cost must be positive")
	// ErrCostTooLarge is returned when Enqueue receives a cost above MaxCost.
	ErrCostTooLarge = errors.New("dwrr: item cost exceeds MaxCost")
	// ErrQuantumTooLarge is returned when SetQuantum receives a value above MaxQuantum.
	ErrQuantumTooLarge = errors.New("dwrr: quantum exceeds MaxQuantum")
)

// Item is a payload and its positive scheduling cost. Enqueue copies the item
// record, so later changes to the caller's Cost do not affect scheduling.
type Item[T any] struct {
	Value T      // Payload returned by Take.
	Cost  uint64 // Positive scheduling cost, fixed when Enqueue succeeds.
}

// Scheduled is an item selected from Queue in scheduler service order.
type Scheduled[T any] struct {
	Queue uint   // Index of the source queue.
	Value T      // Scheduled payload.
	Cost  uint64 // The item's declared scheduling cost.
}

// DWRR is a concurrent-safe cost-aware deficit round-robin scheduler. Do not
// copy it after first use. Its zero value is unusable; use NewDWRR.
type DWRR[T any] struct {
	mu       sync.Mutex
	queues   [][]Item[T]
	quantums []uint64
	// Outside an active visit, deficit is below 2^63: a visit closes below
	// its head's cost (at most MaxCost), and emptying a queue resets credit.
	// RemoveHead can expose a cheaper head but never increases deficit.
	// A grant adds at most MaxQuantum, so deficit never exceeds 2^64 - 2.
	// Fast-forward adds credit only up to the head's cost minus 1.
	// Plain uint64 arithmetic is therefore exact.
	deficits []uint64
	cursor   int  // Next queue when idle; current queue while a visit is active.
	active   bool // Whether the current cursor visit has already received quantum.
}

// NewDWRR creates count empty queues, each with quantum 1. Queue indices start
// at zero and remain stable while their queues exist; AddQueue and RemoveQueue
// change the count, with RemoveQueue affecting only the last queue.
func NewDWRR[T any](count uint) *DWRR[T] {
	n := int(count)
	d := &DWRR[T]{queues: make([][]Item[T], n), quantums: make([]uint64, n), deficits: make([]uint64, n)}
	for i := range d.quantums {
		d.quantums[i] = 1
	}
	return d
}

// SetQuantum changes a queue's quantum while keeping its deficit and any
// active visit. The new quantum is used at the next grant, not when a paused
// visit resumes. Setting the current quantum is a no-op. Zero quantum returns
// ErrZeroQuantum; a quantum above MaxQuantum returns ErrQuantumTooLarge.
// Both errors leave state unchanged. Invalid indices panic first.
func (d *DWRR[T]) SetQuantum(queue uint, quantum uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	configuredQuantum := &d.quantums[queue] // Preserve slice-index panic behavior.
	if quantum == 0 {
		return ErrZeroQuantum
	}
	if quantum > MaxQuantum {
		return ErrQuantumTooLarge
	}
	if *configuredQuantum == quantum {
		return nil
	}
	*configuredQuantum = quantum
	return nil
}

// Enqueue validates the complete input before appending it atomically. New
// items join an active visit at the queue tail. Item records, including costs,
// are copied; values of T are shallow-copied. Zero cost returns ErrZeroCost;
// a cost above MaxCost returns ErrCostTooLarge. Invalid queue indices panic.
func (d *DWRR[T]) Enqueue(queue uint, items []Item[T]) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	queueItems := &d.queues[queue] // Preserve slice-index panic behavior.
	for _, item := range items {
		if item.Cost == 0 {
			return ErrZeroCost
		}
		if item.Cost > MaxCost {
			return ErrCostTooLarge
		}
	}
	*queueItems = append(*queueItems, items...)
	return nil
}

// AddQueue appends an empty queue with quantum 1 and returns its index. The
// index is stable while the queue exists; if the last queue is removed, a later
// AddQueue may reuse its index.
func (d *DWRR[T]) AddQueue() uint {
	d.mu.Lock()
	defer d.mu.Unlock()
	index := uint(len(d.queues))
	d.queues = append(d.queues, nil)
	d.quantums = append(d.quantums, 1)
	d.deficits = append(d.deficits, 0)
	return index
}

// RemoveQueue removes the last queue and its contents. Existing queue indices
// remain stable. Removing the active queue cancels that visit; cursor is kept
// in range (or reset to zero when no queues remain). Empty removal is a no-op.
func (d *DWRR[T]) RemoveQueue() {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.queues)
	if n == 0 {
		return
	}
	last := n - 1
	if d.active && d.cursor == last {
		d.active = false
		d.cursor = 0
	}
	d.queues[last] = nil
	d.deficits[last] = 0
	d.queues = d.queues[:last]
	d.quantums = d.quantums[:last]
	d.deficits = d.deficits[:last]
	if last == 0 {
		d.cursor = 0
	} else if d.cursor >= last {
		d.cursor %= last
	}
}

// RemoveHead removes and returns a copy of the FIFO head. It returns the zero
// Item and false when empty. If items remain, deficit and active-visit state are
// preserved; emptying the queue resets deficit and cancels an active visit.
// Invalid indices panic.
func (d *DWRR[T]) RemoveHead(queue uint) (Item[T], bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := int(queue)
	if len(d.queues[q]) == 0 {
		return Item[T]{}, false
	}
	item := d.queues[q][0]
	clear(d.queues[q][:1])
	d.queues[q] = d.queues[q][1:]
	if len(d.queues[q]) == 0 {
		d.deficits[q] = 0
		if d.active && d.cursor == q {
			d.active = false
			d.cursor = (d.cursor + 1) % len(d.queues)
		}
	}
	return item, true
}

// DrainQueue removes and returns a copy of every queued item, resets deficit,
// and cancels an active visit. Invalid indices panic.
func (d *DWRR[T]) DrainQueue(queue uint) []Item[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := int(queue)
	items := append([]Item[T](nil), d.queues[q]...)
	clear(d.queues[q])
	d.queues[q] = nil
	d.deficits[q] = 0
	if d.active && d.cursor == q {
		d.active = false
		d.cursor = (d.cursor + 1) % len(d.queues)
	}
	return items
}

// Take returns up to n items in service order. A limit can pause an active
// visit; its credit and position persist for the next call. Take(0) returns a
// nonnil empty slice without changing state. Negative n panics. The returned
// slice is always nonnil and never aliases internal queue storage.
func (d *DWRR[T]) Take(n int) []Scheduled[T] {
	if n < 0 {
		panic("dwrr: negative Take limit")
	}
	if n == 0 {
		return make([]Scheduled[T], 0)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.queues) == 0 {
		return make([]Scheduled[T], 0)
	}
	// Bound result capacity by both the request and queued work. A one-item
	// result needs at most one append allocation, so avoid scanning queues.
	capacity := 0
	if n > 1 {
		for _, queue := range d.queues {
			remaining := n - capacity
			if len(queue) >= remaining {
				capacity = n
				break
			}
			capacity += len(queue)
		}
	}
	out := make([]Scheduled[T], 0, capacity)
	// Count new visits since the most recent emitted item. A visit that was
	// already active at the start of this call must not count as a new visit.
	visitsSinceService := 0
	for len(out) < n {
		if !d.active {
			if len(d.queues) == 0 {
				break
			}
			q := d.cursor
			if len(d.queues[q]) == 0 {
				d.deficits[q] = 0
				d.advance()
				visitsSinceService++
			} else {
				d.deficits[q] += d.quantums[q]
				d.active = true
				visitsSinceService++
			}
		} else {
			q := d.cursor
			if len(d.queues[q]) == 0 {
				d.deficits[q] = 0
				d.closeVisit()
				continue
			}
			head := d.queues[q][0]
			if d.deficits[q] < head.Cost {
				d.closeVisit()
			} else {
				d.deficits[q] -= head.Cost
				out = append(out, Scheduled[T]{Queue: uint(q), Value: head.Value, Cost: head.Cost})
				clear(d.queues[q][:1])
				d.queues[q] = d.queues[q][1:]
				if len(d.queues[q]) == 0 {
					d.deficits[q] = 0
					d.closeVisit()
				}
				visitsSinceService = 0
			}
		}
		if !d.active && visitsSinceService >= len(d.queues) {
			if !d.fastForward() {
				break
			}
			visitsSinceService = 0
		}
	}
	return out
}

func (d *DWRR[T]) advance() { d.cursor = (d.cursor + 1) % len(d.queues) }
func (d *DWRR[T]) closeVisit() {
	d.cursor = (d.cursor + 1) % len(d.queues)
	d.active = false
}

// fastForward skips complete no-service cycles without making a head eligible.
func (d *DWRR[T]) fastForward() bool {
	skip := ^uint64(0)
	found := false
	for i, q := range d.queues {
		if len(q) == 0 {
			continue
		}
		if d.deficits[i] >= q[0].Cost {
			return true
		}
		gap := q[0].Cost - d.deficits[i]
		cycles := (gap - 1) / d.quantums[i]
		if cycles < skip {
			skip = cycles
		}
		found = true
	}
	if !found {
		return false
	}
	if skip == 0 {
		return true
	}
	for i, q := range d.queues {
		if len(q) > 0 {
			// The minimum skip ensures skip*quantum <= gap-1 < 2^63
			// for every queue. Neither multiplication nor addition overflows.
			d.deficits[i] += skip * d.quantums[i]
		}
	}
	return true
}
