// Package dwrr implements a cost-aware Deficit Weighted Round Robin scheduler.
// Quantum and item costs are positive uint64 values. Scheduling order is
// determined by queue visits and FIFO heads, not by downstream execution time.
package dwrr

import (
	"errors"
	"math/bits"
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
	// Outside an active visit, deficit is at most MaxCost-1: a visit closes
	// below its head's cost (at most MaxCost), and emptying resets credit.
	// RemoveHead can expose a cheaper head but never increases deficit.
	// A grant adds at most MaxQuantum, yielding MaxCost-1+MaxQuantum = 2^64-3,
	// attained by TestBoundaryPeakCreditAfterReweight. Fast-forward adds credit
	// only up to the head's cost minus 1. Plain uint64 arithmetic is exact.
	deficits  []uint64
	busy      []uint64 // One bit per nonempty queue; unused high bits are zero.
	busyCount int
	total     int  // Number of queued items, independent of payload type.
	cursor    int  // Next queue when idle; current queue while a visit is active.
	active    bool // Whether the current cursor visit has already received quantum.
}

// NewDWRR creates count empty queues, each with quantum 1. Queue indices start
// at zero and remain stable while their queues exist; AddQueue and RemoveQueue
// change the count, with RemoveQueue affecting only the last queue.
func NewDWRR[T any](count uint) *DWRR[T] {
	n := int(count)
	d := &DWRR[T]{queues: make([][]Item[T], n), quantums: make([]uint64, n), deficits: make([]uint64, n), busy: make([]uint64, (n+63)/64)}
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
	if len(items) == 0 {
		return nil
	}
	wasEmpty := len(*queueItems) == 0
	*queueItems = append(*queueItems, items...)
	d.total += len(items)
	if wasEmpty {
		d.busy[queue/64] |= uint64(1) << (queue % 64)
		d.busyCount++
	}
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
	if index%64 == 0 {
		d.busy = append(d.busy, 0)
	}
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
	if len(d.queues[last]) > 0 {
		d.total -= len(d.queues[last])
		d.busy[last/64] &^= uint64(1) << (last % 64)
		d.busyCount--
	}
	d.busy = d.busy[:(last+63)/64]
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
	return d.popHead(q), true
}

// popHead removes the head of nonempty queue q. The caller holds d.mu.
func (d *DWRR[T]) popHead(q int) Item[T] {
	item := d.queues[q][0]
	clear(d.queues[q][:1])
	if len(d.queues[q]) == 1 {
		d.queues[q] = d.queues[q][:0]
	} else {
		d.queues[q] = d.queues[q][1:]
	}
	d.total--
	if len(d.queues[q]) == 0 {
		d.busy[q/64] &^= uint64(1) << (q % 64)
		d.busyCount--
		d.deficits[q] = 0
		if d.active && d.cursor == q {
			d.closeVisit()
		}
	}
	return item
}

// DrainQueue removes and returns a copy of every queued item, resets deficit,
// and cancels an active visit. Invalid indices panic.
func (d *DWRR[T]) DrainQueue(queue uint) []Item[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := int(queue)
	items := append([]Item[T](nil), d.queues[q]...)
	if len(d.queues[q]) > 0 {
		d.total -= len(d.queues[q])
		d.busy[q/64] &^= uint64(1) << (q % 64)
		d.busyCount--
	}
	clear(d.queues[q])
	d.queues[q] = nil
	d.deficits[q] = 0
	if d.active && d.cursor == q {
		d.closeVisit()
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
	out := make([]Scheduled[T], 0, min(n, d.total))
	// Count grants since service, excluding a visit already active on entry.
	grantsSinceService := 0
	for len(out) < n && d.busyCount > 0 {
		if !d.active {
			// Empty queues have zero credit, so their visits only advance the
			// cursor. Skip them without changing the order of busy visits.
			d.cursor = d.nextBusy(d.cursor)
			d.deficits[d.cursor] += d.quantums[d.cursor]
			d.active = true
			grantsSinceService++
		} else {
			q := d.cursor
			head := d.queues[q][0]
			if d.deficits[q] < head.Cost {
				d.closeVisit()
			} else {
				d.deficits[q] -= head.Cost
				// Keep removal local on this hot path: calling popHead per item
				// regressed scheduling benchmarks when the helper did not inline.
				head = d.queues[q][0]
				clear(d.queues[q][:1])
				if len(d.queues[q]) == 1 {
					d.queues[q] = d.queues[q][:0]
				} else {
					d.queues[q] = d.queues[q][1:]
				}
				d.total--
				if len(d.queues[q]) == 0 {
					d.busy[q/64] &^= uint64(1) << (q % 64)
					d.busyCount--
					d.deficits[q] = 0
					if d.active && d.cursor == q {
						d.closeVisit()
					}
				}
				out = append(out, Scheduled[T]{Queue: uint(q), Value: head.Value, Cost: head.Cost})
				grantsSinceService = 0
			}
		}
		if !d.active && grantsSinceService > 0 && grantsSinceService >= d.busyCount {
			d.fastForward()
			grantsSinceService = 0
		}
	}
	return out
}

// nextBusy finds the first nonempty queue at or after from, wrapping once.
func (d *DWRR[T]) nextBusy(from int) int {
	if d.busyCount == 0 {
		return -1
	}
	word := from / 64
	if candidates := d.busy[word] & (^uint64(0) << (from % 64)); candidates != 0 {
		return word*64 + bits.TrailingZeros64(candidates)
	}
	for i := word + 1; i < len(d.busy); i++ {
		if d.busy[i] != 0 {
			return i*64 + bits.TrailingZeros64(d.busy[i])
		}
	}
	for i := 0; i < word; i++ {
		if d.busy[i] != 0 {
			return i*64 + bits.TrailingZeros64(d.busy[i])
		}
	}
	// With busyCount > 0, the remaining bit must precede from in this word.
	return word*64 + bits.TrailingZeros64(d.busy[word])
}
func (d *DWRR[T]) closeVisit() {
	d.cursor = (d.cursor + 1) % len(d.queues)
	d.active = false
}

// fastForward skips complete no-service cycles without making a head eligible.
// The caller guarantees busyCount > 0.
func (d *DWRR[T]) fastForward() {
	skip := ^uint64(0)
	for word, busy := range d.busy {
		for busy != 0 {
			i := word*64 + bits.TrailingZeros64(busy)
			busy &= busy - 1
			cost := d.queues[i][0].Cost
			if d.deficits[i] >= cost {
				return
			}
			gap := cost - d.deficits[i]
			cycles := (gap - 1) / d.quantums[i]
			if cycles < skip {
				skip = cycles
			}
		}
	}
	if skip == 0 {
		return
	}
	for word, busy := range d.busy {
		for busy != 0 {
			i := word*64 + bits.TrailingZeros64(busy)
			busy &= busy - 1
			// The minimum skip ensures skip*quantum <= gap-1 < 2^63
			// for every queue. Neither multiplication nor addition overflows.
			d.deficits[i] += skip * d.quantums[i]
		}
	}
}
