// Package dwrr implements a cost-aware Deficit Weighted Round Robin scheduler.
// Quantum and item costs are positive uint64 values. Scheduling order is
// determined by queue visits and FIFO heads, not by downstream execution time.
package dwrr

import (
	"errors"
	"math/big"
	"sync"
)

var (
	// ErrZeroQuantum is returned when SetQuantum is given a zero quantum.
	ErrZeroQuantum = errors.New("dwrr: quantum must be positive")
	// ErrZeroCost is returned when Enqueue receives an item with zero cost.
	ErrZeroCost = errors.New("dwrr: item cost must be positive")
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
	deficits []*big.Int // Exact nonnegative credits; never limited to uint64.
	cursor   int        // Next queue when idle; current queue while a visit is active.
	active   bool       // Whether the current cursor visit has already received quantum.
}

// NewDWRR creates count empty queues, each with quantum 1. Queue indices start
// at zero and remain stable while their queues exist; AddQueue and RemoveQueue
// change the count, with RemoveQueue affecting only the last queue.
func NewDWRR[T any](count uint) *DWRR[T] {
	n := int(count)
	d := &DWRR[T]{queues: make([][]Item[T], n), quantums: make([]uint64, n), deficits: make([]*big.Int, n)}
	for i := range d.quantums {
		d.quantums[i] = 1
		d.deficits[i] = new(big.Int)
	}
	return d
}

// SetQuantum changes a queue's quantum while keeping its deficit and any
// active visit. The new quantum is used at the next grant, not when a paused
// visit resumes. Setting the current quantum is a no-op. Zero quantum returns
// ErrZeroQuantum without changing state. Invalid indices panic.
func (d *DWRR[T]) SetQuantum(queue uint, quantum uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	configuredQuantum := &d.quantums[queue] // Preserve slice-index panic behavior.
	if quantum == 0 {
		return ErrZeroQuantum
	}
	if *configuredQuantum == quantum {
		return nil
	}
	*configuredQuantum = quantum
	return nil
}

// Enqueue validates the complete input before appending it atomically. New
// items join an active visit at the queue tail. Item records, including costs,
// are copied; values of T are shallow-copied. Zero cost returns ErrZeroCost.
// Invalid queue indices panic.
func (d *DWRR[T]) Enqueue(queue uint, items []Item[T]) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	queueItems := &d.queues[queue] // Preserve slice-index panic behavior.
	for _, item := range items {
		if item.Cost == 0 {
			return ErrZeroCost
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
	d.deficits = append(d.deficits, new(big.Int))
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
	d.deficits[last] = nil
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
		d.deficits[q].SetUint64(0)
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
	d.deficits[q].SetUint64(0)
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
	// Reuse arithmetic operands within this locked call rather than allocating
	// a temporary big.Int for each visit and FIFO head.
	var scratch big.Int
	for len(out) < n {
		if !d.active {
			if len(d.queues) == 0 {
				break
			}
			q := d.cursor
			if len(d.queues[q]) == 0 {
				d.deficits[q].SetUint64(0)
				d.advance()
				visitsSinceService++
			} else {
				scratch.SetUint64(d.quantums[q])
				d.deficits[q].Add(d.deficits[q], &scratch)
				d.active = true
				visitsSinceService++
			}
		} else {
			q := d.cursor
			if len(d.queues[q]) == 0 {
				d.deficits[q].SetUint64(0)
				d.closeVisit()
				continue
			}
			head := d.queues[q][0]
			scratch.SetUint64(head.Cost)
			if d.deficits[q].Cmp(&scratch) < 0 {
				d.closeVisit()
			} else {
				d.deficits[q].Sub(d.deficits[q], &scratch)
				out = append(out, Scheduled[T]{Queue: uint(q), Value: head.Value, Cost: head.Cost})
				clear(d.queues[q][:1])
				d.queues[q] = d.queues[q][1:]
				if len(d.queues[q]) == 0 {
					d.deficits[q].SetUint64(0)
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

// fastForward skips complete no-service cycles. Credits use arbitrary-precision
// integers, so multiplying a uint64 quantum by an arbitrarily large cycle count
// is exact and cannot silently saturate or overflow.
func (d *DWRR[T]) fastForward() bool {
	// At this boundary every queue has had one unproductive visit. For each
	// head, skip floor((cost-deficit-1)/quantum) additional whole cycles.
	// The subtraction of one avoids an overflow-prone ceil numerator.
	skip := new(big.Int)
	found := false
	for i, q := range d.queues {
		if len(q) == 0 {
			continue
		}
		gap := new(big.Int).Sub(new(big.Int).SetUint64(q[0].Cost), d.deficits[i])
		if gap.Sign() <= 0 {
			return true
		}
		gap.Sub(gap, big.NewInt(1))
		cycles := gap.Quo(gap, new(big.Int).SetUint64(d.quantums[i]))
		if !found || cycles.Cmp(skip) < 0 {
			skip.Set(cycles)
			found = true
		}
	}
	if !found {
		return false
	}
	if skip.Sign() == 0 {
		return true
	}
	for i, q := range d.queues {
		if len(q) > 0 {
			credit := new(big.Int).Mul(new(big.Int).SetUint64(d.quantums[i]), skip)
			d.deficits[i].Add(d.deficits[i], credit)
		}
	}
	return true
}
