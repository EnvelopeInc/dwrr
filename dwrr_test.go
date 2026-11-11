package dwrr

import (
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
)

func mustEnqueue[T any](t *testing.T, d *DWRR[T], q uint, items []Item[T]) {
	t.Helper()
	if err := d.Enqueue(q, items); err != nil {
		t.Fatal(err)
	}
}
func mustQuantum[T any](t *testing.T, d *DWRR[T], q uint, v uint64) {
	t.Helper()
	if err := d.SetQuantum(q, v); err != nil {
		t.Fatal(err)
	}
}

// referenceTake intentionally simulates one ordinary queue visit at a time and
// does not use production arithmetic or fast-forward logic.
type reference struct {
	queues           [][]Item[int]
	quantum, deficit []uint64
	cursor           int
	active           bool
}

func (r *reference) take(n int) []Scheduled[int] {
	out := make([]Scheduled[int], 0)
	if n == 0 || len(r.queues) == 0 {
		return out
	}
	for len(out) < n {
		allEmpty := true
		for _, q := range r.queues {
			if len(q) > 0 {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			return out
		}
		if !r.active {
			q := r.cursor
			if len(r.queues[q]) == 0 {
				r.deficit[q] = 0
				r.cursor = (q + 1) % len(r.queues)
				continue
			}
			r.deficit[q] += r.quantum[q]
			r.active = true
		}
		q := r.cursor
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.active = false
			r.cursor = (q + 1) % len(r.queues)
			continue
		}
		item := r.queues[q][0]
		if item.Cost > r.deficit[q] {
			r.active = false
			r.cursor = (q + 1) % len(r.queues)
			continue
		}
		r.deficit[q] -= item.Cost
		out = append(out, Scheduled[int]{uint(q), item.Value, item.Cost})
		r.queues[q] = r.queues[q][1:]
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.active = false
			r.cursor = (q + 1) % len(r.queues)
		}
	}
	return out
}
func TestReferenceGeneratedScenarios(t *testing.T) {
	rng := rand.New(rand.NewSource(9123))
	for scenario := 0; scenario < 100; scenario++ {
		n := 1 + rng.Intn(4)
		d := NewDWRR[int](uint(n))
		r := &reference{queues: make([][]Item[int], n), quantum: make([]uint64, n), deficit: make([]uint64, n)}
		for q := 0; q < n; q++ {
			r.quantum[q] = uint64(1 + rng.Intn(5))
			mustQuantum(t, d, uint(q), r.quantum[q])
			for j := 0; j < 1+rng.Intn(8); j++ {
				item := Item[int]{scenario*100 + q*10 + j, uint64(1 + rng.Intn(12))}
				r.queues[q] = append(r.queues[q], item)
			}
			mustEnqueue(t, d, uint(q), r.queues[q])
		}
		// Compare a full result, then repeat with arbitrary retrieval boundaries.
		fullD := cloneForTest(d)
		fullR := cloneReference(r)
		want := fullR.take(1000)
		got := fullD.Take(1000)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("scenario %d full mismatch\ngot=%v\nwant=%v", scenario, got, want)
		}
		for k := 0; k < 60; k++ {
			limit := 1 + rng.Intn(4)
			g := d.Take(limit)
			w := r.take(limit)
			if !reflect.DeepEqual(g, w) {
				t.Fatalf("scenario %d step %d got=%v want=%v", scenario, k, g, w)
			}
		}
		if !reflect.DeepEqual(d.queues, r.queues) || d.cursor != r.cursor || d.active != r.active {
			t.Fatalf("scenario %d state mismatch", scenario)
		}
		for q := 0; q < n; q++ {
			if d.deficits[q].Uint64() != r.deficit[q] {
				t.Fatalf("scenario %d deficit q%d", scenario, q)
			}
		}
	}
}
func cloneReference(r *reference) *reference {
	x := *r
	x.queues = make([][]Item[int], len(r.queues))
	for i := range r.queues {
		x.queues[i] = append([]Item[int](nil), r.queues[i]...)
	}
	x.quantum = append([]uint64(nil), r.quantum...)
	x.deficit = append([]uint64(nil), r.deficit...)
	return &x
}
func cloneForTest(d *DWRR[int]) *DWRR[int] {
	x := NewDWRR[int](uint(len(d.queues)))
	for i := range d.queues {
		x.queues[i] = append([]Item[int](nil), d.queues[i]...)
		x.quantums[i] = d.quantums[i]
		x.deficits[i].Set(d.deficits[i])
	}
	x.cursor, x.active = d.cursor, d.active
	return x
}

func TestEqualCostsAndQuantums(t *testing.T) {
	d := NewDWRR[int](2)
	for q := uint(0); q < 2; q++ {
		mustEnqueue(t, d, q, []Item[int]{{int(q)*10 + 1, 2}, {int(q)*10 + 2, 2}, {int(q)*10 + 3, 2}})
	}
	want := []Scheduled[int]{
		{0, 1, 2}, {1, 11, 2},
		{0, 2, 2}, {1, 12, 2},
		{0, 3, 2}, {1, 13, 2},
	}
	if got := d.Take(len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("equal cost/quantum order: got %v want %v", got, want)
	}
}

func TestCostsDeficitAndTakePersistence(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 2)
	mustQuantum(t, d, 1, 3)
	mustEnqueue(t, d, 0, []Item[int]{{1, 2}, {2, 2}})
	mustEnqueue(t, d, 1, []Item[int]{{11, 4}, {12, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 2}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 2}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 11, 4}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 12, 1}}) {
		t.Fatal(got)
	}
}
func TestTakeZeroPreservesPausedVisit(t *testing.T) {
	pending, control := NewDWRR[int](2), NewDWRR[int](2)
	items := []Item[int]{{1, 1}, {2, 1}}
	mustEnqueue(t, pending, 0, items)
	mustEnqueue(t, pending, 1, []Item[int]{{3, 1}})
	mustEnqueue(t, control, 0, items)
	mustEnqueue(t, control, 1, []Item[int]{{3, 1}})
	if !reflect.DeepEqual(pending.Take(1), control.Take(1)) {
		t.Fatal("initial scheduler state differs")
	}
	if got := pending.Take(0); got == nil || len(got) != 0 {
		t.Fatalf("Take(0) result %#v", got)
	}
	if got, want := pending.Take(1), control.Take(1); !reflect.DeepEqual(got, want) {
		t.Fatalf("Take(0) changed active visit: got %v, want %v", got, want)
	}
	if !reflect.DeepEqual(pending.queues, control.queues) || pending.cursor != control.cursor || pending.active != control.active {
		t.Fatal("Take(0) changed scheduler state")
	}
	for i := range pending.deficits {
		if pending.deficits[i].Cmp(control.deficits[i]) != 0 {
			t.Fatal("Take(0) changed deficit")
		}
	}
}

func TestTakeLimitEquivalentToRepeatedSingleItemCalls(t *testing.T) {
	d := NewDWRR[int](2)
	mustEnqueue(t, d, 0, []Item[int]{{1, 5}, {2, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{3, 2}})
	a := cloneForTest(d)
	one := a.Take(17)
	b := cloneForTest(d)
	var many []Scheduled[int]
	for i := 0; i < 17; i++ {
		many = append(many, b.Take(1)...)
	}
	if !reflect.DeepEqual(one, many) || !reflect.DeepEqual(a.queues, b.queues) || a.cursor != b.cursor || a.active != b.active {
		t.Fatal("Take(n) differs from repeated Take(1) state")
	}
	for i := range a.deficits {
		if a.deficits[i].Cmp(b.deficits[i]) != 0 {
			t.Fatal("Take(n) differs from repeated Take(1) deficit")
		}
	}
	for name, s := range map[string]*DWRR[int]{"none": NewDWRR[int](0), "empty": NewDWRR[int](2), "drained": a} {
		if name == "drained" {
			s.Take(100)
		}
		got := s.Take(math.MaxInt)
		if got == nil || len(got) != 0 {
			t.Fatalf("%s empty result %#v", name, got)
		}
	}
	pending := NewDWRR[int](1)
	mustEnqueue(t, pending, 0, []Item[int]{{1, 1}})
	before := pending.deficits[0].String()
	if x := pending.Take(0); x == nil || len(x) != 0 || before != pending.deficits[0].String() {
		t.Fatal("Take(0) altered state")
	}
	got := pending.Take(math.MaxInt)
	if !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) || cap(got) > 1 {
		t.Fatalf("MaxInt request exceeded the small backlog: len/cap=%d/%d items=%v", len(got), cap(got), got)
	}
}
func TestTakeMaxIntCapacityIsBoundedByBacklog(t *testing.T) {
	d := NewDWRR[int](2)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{3, 1}})

	got := d.Take(math.MaxInt)
	if got == nil || len(got) != 3 || cap(got) > 3 {
		t.Fatalf("MaxInt request allocated beyond queued work: len/cap=%d/%d", len(got), cap(got))
	}
}

func TestEnqueueCopiesItemCost(t *testing.T) {
	d := NewDWRR[int](1)
	items := []Item[int]{{1, 2}, {2, 1}}
	mustEnqueue(t, d, 0, items)
	items[0].Cost = 0
	items[1].Cost = 100

	want := []Scheduled[int]{{0, 1, 2}, {0, 2, 1}}
	if got := d.Take(len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("queued costs changed after Enqueue: got %v want %v", got, want)
	}
}

func TestRemoveHeadClearsStorageAndReturnsCopy(t *testing.T) {
	first, second := 42, 43
	d := NewDWRR[*int](1)
	mustEnqueue(t, d, 0, []Item[*int]{{&first, 1}, {&second, 1}})
	backing := d.queues[0]

	item, ok := d.RemoveHead(0)
	if !ok || item.Value != &first || item.Cost != 1 {
		t.Fatalf("unexpected direct removal: %v", item)
	}
	if backing[0].Value != nil || backing[0].Cost != 0 {
		t.Fatal("dequeued value remains in queue storage")
	}
	item.Value = nil
	if next, ok := d.RemoveHead(0); !ok || next.Value != &second {
		t.Fatalf("returned copy changed remaining FIFO item: %v", next)
	}
	if empty, ok := d.RemoveHead(0); ok || !reflect.DeepEqual(empty, Item[*int]{}) {
		t.Fatalf("empty removal = (%v, %t), want zero item and false", empty, ok)
	}
}

func TestDrainQueueClearsStorageAndReturnsOwnedItems(t *testing.T) {
	first, second := 42, 43
	d := NewDWRR[*int](1)
	mustEnqueue(t, d, 0, []Item[*int]{{&first, 1}, {&second, 2}})
	backing := d.queues[0]
	got := d.DrainQueue(0)
	if !reflect.DeepEqual(got, []Item[*int]{{&first, 1}, {&second, 2}}) {
		t.Fatalf("unexpected drained items: %v", got)
	}
	for i, item := range backing {
		if item != (Item[*int]{}) {
			t.Fatalf("removed reference remains at storage index %d: %v", i, item)
		}
	}
	got[0].Value = nil
	if got[1].Value != &second {
		t.Fatal("returned drain slice does not own its item records")
	}
}

func TestRemoveHeadPreservesActiveVisitWhenItemsRemain(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 4)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 8}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) || !d.active {
		t.Fatalf("failed to prepare active visit: %v", got)
	}
	deficit, cursor := new(big.Int).Set(d.deficits[0]), d.cursor
	if item, ok := d.RemoveHead(0); !ok || item.Value != 2 {
		t.Fatalf("unexpected direct removal: %v, %t", item, ok)
	}
	if !d.active || d.cursor != cursor || d.deficits[0].Cmp(deficit) != 0 {
		t.Fatalf("removing a nonempty head changed visit: cursor=%d active=%t deficit=%s", d.cursor, d.active, d.deficits[0])
	}
}

func TestValidationAtomicityAndPanics(t *testing.T) {
	d := NewDWRR[int](1)
	for _, operation := range []func(){
		func() { _ = d.SetQuantum(1, 0) },
		func() { _ = d.Enqueue(1, []Item[int]{{Value: 1, Cost: 0}}) },
		func() { d.RemoveHead(1) },
		func() { d.DrainQueue(1) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid queue index did not panic")
				}
			}()
			operation()
		}()
	}
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}})
	if err := d.SetQuantum(0, 0); err != ErrZeroQuantum {
		t.Fatal(err)
	}
	if err := d.Enqueue(0, []Item[int]{{2, 2}, {3, 0}}); err != ErrZeroCost {
		t.Fatal(err)
	}
	if got := d.Take(2); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); got == nil || len(got) != 0 {
		t.Fatal("expected nonnil empty after drain")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("negative Take did not panic")
			}
		}()
		d.Take(-1)
	}()
}
func TestSetQuantumUnchangedIsNoOp(t *testing.T) {
	cases := []struct {
		name  string
		setup func() *DWRR[int]
	}{
		{
			name: "active remaining-credit visit",
			setup: func() *DWRR[int] {
				d := NewDWRR[int](2)
				mustQuantum(t, d, 0, 3)
				mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 10}})
				if got := d.Take(1); len(got) != 1 || !d.active {
					t.Fatalf("failed to prepare active visit: %v", got)
				}
				return d
			},
		},
		{
			name: "inactive accrued deficit",
			setup: func() *DWRR[int] {
				d := NewDWRR[int](2)
				mustQuantum(t, d, 0, 2)
				mustEnqueue(t, d, 0, []Item[int]{{1, 9}})
				mustEnqueue(t, d, 1, []Item[int]{{2, 1}})
				d.Take(1)
				return d
			},
		},
		{
			name: "after fast-forward",
			setup: func() *DWRR[int] {
				d := NewDWRR[int](2)
				mustEnqueue(t, d, 0, []Item[int]{{1, 20}})
				mustEnqueue(t, d, 1, []Item[int]{{2, 5}})
				d.Take(1)
				return d
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.setup()
			control := cloneForTest(d)
			if err := d.SetQuantum(0, d.quantums[0]); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(d.quantums, control.quantums) || d.cursor != control.cursor || d.active != control.active {
				t.Fatalf("unchanged quantum altered state: cursor=%d active=%t deficits=%v", d.cursor, d.active, d.deficits)
			}
			for q := range d.queues {
				if len(d.queues[q]) != len(control.queues[q]) {
					t.Fatalf("unchanged quantum altered queue %d", q)
				}
				for i := range d.queues[q] {
					if d.queues[q][i] != control.queues[q][i] {
						t.Fatalf("unchanged quantum altered queue %d item %d", q, i)
					}
				}
			}
			for q := range d.deficits {
				if d.deficits[q].Cmp(control.deficits[q]) != 0 {
					t.Fatalf("unchanged quantum altered deficit %d: %s != %s", q, d.deficits[q], control.deficits[q])
				}
			}
			for i := 0; i < 12; i++ {
				if got, want := d.Take(1), control.Take(1); !reflect.DeepEqual(got, want) {
					t.Fatalf("service order changed at %d: got %v want %v", i, got, want)
				}
			}
		})
	}
}

func TestQuantumAccumulatesUntilHeadIsAffordable(t *testing.T) {
	d := NewDWRR[int](1)
	mustQuantum(t, d, 0, 2)
	mustEnqueue(t, d, 0, []Item[int]{{1, 5}, {2, 100}})

	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 5}}) {
		t.Fatalf("large-cost head was not made eligible: %v", got)
	}
	if d.deficits[0].Uint64() != 1 || !d.active {
		t.Fatalf("visit did not preserve remainder: deficit=%s active=%t", d.deficits[0], d.active)
	}
}

func TestEmptyQueueResetsDeficitAndReenqueue(t *testing.T) {
	d := NewDWRR[int](1)
	mustQuantum(t, d, 0, 3)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 5}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) || d.deficits[0].Uint64() != 2 {
		t.Fatalf("unexpected first service/deficit: %v %s", got, d.deficits[0])
	}
	d.DrainQueue(0)
	if d.deficits[0].Sign() != 0 {
		t.Fatal("DrainQueue did not reset deficit")
	}
	mustEnqueue(t, d, 0, []Item[int]{{3, 4}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 3, 4}}) {
		t.Fatalf("unexpected service after clean re-enqueue: %v", got)
	}
}

func TestActiveDrainQueueCancelsVisitAndAdvances(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 4)
	mustEnqueue(t, d, 0, []Item[int]{{Value: 1, Cost: 1}, {Value: 2, Cost: 4}})
	mustEnqueue(t, d, 1, []Item[int]{{Value: 9, Cost: 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{Queue: 0, Value: 1, Cost: 1}}) || !d.active {
		t.Fatalf("expected paused active visit, got %v active=%t", got, d.active)
	}
	if got := d.DrainQueue(0); !reflect.DeepEqual(got, []Item[int]{{Value: 2, Cost: 4}}) {
		t.Fatalf("DrainQueue result %v", got)
	}
	if d.active || d.cursor != 1 || d.deficits[0].Sign() != 0 {
		t.Fatalf("active visit was not canceled: cursor=%d active=%t deficit=%s", d.cursor, d.active, d.deficits[0])
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{Queue: 1, Value: 9, Cost: 1}}) {
		t.Fatalf("next queue did not run after active DrainQueue: %v", got)
	}
}

func TestRemoveHeadEmptyResetsDeficitBeforeReenqueue(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 3)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 5}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) {
		t.Fatal(got)
	}
	if item, ok := d.RemoveHead(0); !ok || item.Value != 2 {
		t.Fatalf("unexpected direct removal: %v", item)
	}
	mustEnqueue(t, d, 0, []Item[int]{{3, 4}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}, {10, 1}})

	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 10, 1}}) {
		t.Fatalf("empty-queue deficit was retained: %v", got)
	}
}

func TestEnqueueAwaitingCreditPreservesFIFOAndDeficit(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 1)
	mustQuantum(t, d, 1, 1)
	mustEnqueue(t, d, 0, []Item[int]{{1, 2}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}, {10, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) || d.deficits[0].Uint64() != 1 {
		t.Fatalf("unexpected first result/credit: %v deficit=%s", got, d.deficits[0])
	}
	mustEnqueue(t, d, 0, []Item[int]{{2, 1}}) // FIFO tail; retain the existing credit.
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 2}}) {
		t.Fatalf("enqueue bypassed the unaffordable FIFO head or lost deficit: %v", got)
	}
}

func TestFIFOBlockingAndDeficitCarry(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 2)
	mustQuantum(t, d, 1, 1)
	mustEnqueue(t, d, 0, []Item[int]{{1, 5}, {2, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	got := d.Take(1)
	if !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) {
		t.Fatal(got)
	}
	got = d.Take(1)
	if !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 5}}) {
		t.Fatal(got)
	}
	got = d.Take(1)
	if !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 1}}) {
		t.Fatal(got)
	}
}
func TestMutationsDuringVisit(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 3)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) {
		t.Fatal(got)
	}
	mustEnqueue(t, d, 0, []Item[int]{{3, 1}}) // Appends to the active visit.
	d.AddQueue()
	if got := d.Take(0); got == nil || len(got) != 0 {
		t.Fatalf("Take(0) result %#v", got)
	}
	d.RemoveQueue() // Removing an inactive last queue preserves the active visit.
	d.AddQueue()
	mustEnqueue(t, d, 2, []Item[int]{{20, 1}})
	if item, ok := d.RemoveHead(0); !ok || item.Value != 2 {
		t.Fatalf("unexpected direct removal: %v", item)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 3, 1}}) {
		t.Fatalf("active visit/credit was not preserved: %v", got)
	}
	if got := d.Take(2); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}, {2, 20, 1}}) {
		t.Fatalf("appended queue order changed: %v", got)
	}

	quantumChange := NewDWRR[int](2)
	mustQuantum(t, quantumChange, 0, 3)
	mustEnqueue(t, quantumChange, 0, []Item[int]{{1, 1}, {2, 1}})
	mustEnqueue(t, quantumChange, 1, []Item[int]{{9, 1}})
	quantumChange.Take(1) // Queue 0's visit is active with carried credit.
	mustQuantum(t, quantumChange, 0, 1)
	if got := quantumChange.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) {
		t.Fatalf("SetQuantum did not cancel the active visit: %v", got)
	}
	if got := quantumChange.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 1}}) {
		t.Fatalf("new quantum was not used on the next visit: %v", got)
	}

	fastForward := NewDWRR[int](2)
	mustQuantum(t, fastForward, 0, 1)
	mustEnqueue(t, fastForward, 0, []Item[int]{{0, 8}})
	mustEnqueue(t, fastForward, 1, []Item[int]{{1, 3}, {2, 5}})
	if got := fastForward.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 1, 3}}) {
		t.Fatalf("unexpected initial eligible head: %v", got)
	}
	mustQuantum(t, fastForward, 0, 2) // Change a waiting queue immediately before skipping.
	if got := fastForward.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 0, 8}}) {
		t.Fatalf("fast-forward did not use the new quantum: %v", got)
	}

	removeActive := NewDWRR[int](2)
	mustEnqueue(t, removeActive, 1, []Item[int]{{10, 1}, {11, 1}})
	if got := removeActive.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 10, 1}}) {
		t.Fatal(got)
	}
	removeActive.RemoveQueue() // Cancels the active last queue and discards item 11.
	mustEnqueue(t, removeActive, 0, []Item[int]{{5, 1}})
	if got := removeActive.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 5, 1}}) {
		t.Fatalf("cursor was invalid after removing active queue: %v", got)
	}
}
func TestHugeAdvanceAndUint64Boundary(t *testing.T) {
	// A dequeue exposes a cheap head while retaining deficit. The next grant
	// makes credit exceed uint64, and Take pauses that active visit afterward.
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, math.MaxUint64-1)
	mustEnqueue(t, d, 0, []Item[int]{{1, math.MaxUint64}, {2, 1}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) {
		t.Fatal(got)
	}
	if d.deficits[0].Uint64() != math.MaxUint64-1 {
		t.Fatalf("unexpected accumulated credit: %s", d.deficits[0])
	}
	if item, ok := d.RemoveHead(0); !ok || item.Value != 1 {
		t.Fatalf("unexpected dequeued head: %v", item)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 1}}) {
		t.Fatal(got)
	}
	if d.deficits[0].Cmp(new(big.Int).SetUint64(math.MaxUint64)) <= 0 || !d.active {
		t.Fatalf("paused visit lost over-uint64 credit: %s", d.deficits[0])
	}

	d = NewDWRR[int](1)
	mustQuantum(t, d, 0, 1)
	mustEnqueue(t, d, 0, []Item[int]{{1, math.MaxUint64}})
	if got := d.Take(1); len(got) != 1 || got[0].Value != 1 {
		t.Fatal(got)
	}
	d = NewDWRR[int](1)
	mustQuantum(t, d, 0, math.MaxUint64)
	mustEnqueue(t, d, 0, []Item[int]{{1, math.MaxUint64}, {2, math.MaxUint64}})
	got := d.Take(2)
	if len(got) != 2 || got[0].Value != 1 || got[1].Value != 2 {
		t.Fatal(got)
	}
}
func TestFastForwardTieAndNearTieOrder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cost0    uint64
		cost1    uint64
		winner   uint
		wantDef0 uint64
		wantDef1 uint64
	}{
		{name: "same round ties by cursor", cost0: 10, cost1: 10, winner: 0, wantDef1: 9},
		{name: "one-round threshold difference", cost0: 13, cost1: 10, winner: 1, wantDef0: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDWRR[int](2)
			mustQuantum(t, d, 0, 3)
			mustQuantum(t, d, 1, 3)
			mustEnqueue(t, d, 0, []Item[int]{{Value: 0, Cost: tc.cost0}})
			mustEnqueue(t, d, 1, []Item[int]{{Value: 1, Cost: tc.cost1}})
			want := []Scheduled[int]{{Queue: tc.winner, Value: int(tc.winner), Cost: []uint64{tc.cost0, tc.cost1}[tc.winner]}}
			if got := d.Take(1); !reflect.DeepEqual(got, want) {
				t.Fatalf("wrong fast-forward winner: got %v want %v", got, want)
			}
			if d.deficits[0].Uint64() != tc.wantDef0 || d.deficits[1].Uint64() != tc.wantDef1 || d.cursor != int(1-tc.winner) || d.active {
				t.Fatalf("unexpected post-tie state: deficits=%v cursor=%d active=%t", d.deficits, d.cursor, d.active)
			}
		})
	}
}

func TestFastForwardMaxCostWithTwoActiveQueues(t *testing.T) {
	const cost = uint64(math.MaxUint64)
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 1)
	mustQuantum(t, d, 1, 2)
	mustEnqueue(t, d, 0, []Item[int]{{Value: 0, Cost: cost}, {Value: 2, Cost: 1}})
	mustEnqueue(t, d, 1, []Item[int]{{Value: 1, Cost: cost}})

	// After the initial visit each, queue 1 has 2 credit. The exact number of
	// additional no-service cycles is floor((MaxUint64-3)/2); queue 1
	// then receives its next quantum and reaches MaxUint64 first.
	wantQ0 := new(big.Int).Add(new(big.Int).SetUint64(2), new(big.Int).SetUint64((cost-3)/2))
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{Queue: 1, Value: 1, Cost: cost}}) {
		t.Fatalf("unexpected MaxUint64 winner: %v", got)
	}
	if d.deficits[0].Cmp(wantQ0) != 0 || d.deficits[1].Sign() != 0 || d.cursor != 0 || d.active {
		t.Fatalf("unexpected state after MaxUint64 fast-forward: deficits=%v cursor=%d active=%t want q0=%s", d.deficits, d.cursor, d.active, wantQ0)
	}

	// One more bounded result advances queue 0 through its remaining fast-forward
	// and consumes its MaxUint64 head; the cheap tail remains in the active visit.
	got := d.Take(1)
	if !reflect.DeepEqual(got, []Scheduled[int]{{Queue: 0, Value: 0, Cost: cost}}) {
		t.Fatalf("unexpected bounded follow-up: %v", got)
	}
	if d.deficits[0].Sign() != 0 || d.deficits[1].Sign() != 0 || d.cursor != 0 || !d.active || !reflect.DeepEqual(d.queues[0], []Item[int]{{Value: 2, Cost: 1}}) {
		t.Fatalf("unexpected state after bounded follow-up: queues=%v deficits=%v cursor=%d active=%t", d.queues, d.deficits, d.cursor, d.active)
	}
}

func TestRemoveFinalQueueWithCreditAndReAdd(t *testing.T) {
	d := NewDWRR[int](1)
	mustQuantum(t, d, 0, 3)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 5}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) || d.deficits[0].Uint64() != 2 {
		t.Fatalf("expected nonzero carried credit: %v deficit=%s", got, d.deficits[0])
	}
	d.RemoveQueue()
	if len(d.queues) != 0 || d.cursor != 0 || d.active {
		t.Fatalf("final queue removal left invalid state: cursor=%d active=%t", d.cursor, d.active)
	}
	d.AddQueue()
	d.AddQueue()
	mustEnqueue(t, d, 0, []Item[int]{{30, 1}, {31, 2}})
	mustEnqueue(t, d, 1, []Item[int]{{40, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 30, 1}}) {
		t.Fatal(got)
	}
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 40, 1}}) {
		t.Fatalf("new queue inherited removed deficit: %v", got)
	}
}

func TestFastForwardAcrossMultipleBackloggedQueues(t *testing.T) {
	const cost = uint64(1_000_000_000)
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 1)
	mustQuantum(t, d, 1, 2)
	mustEnqueue(t, d, 0, []Item[int]{{0, cost}})
	mustEnqueue(t, d, 1, []Item[int]{{1, cost}})

	got := d.Take(1)
	if !reflect.DeepEqual(got, []Scheduled[int]{{Queue: 1, Value: 1, Cost: cost}}) {
		t.Fatalf("wrong first eligible queue after fast-forward: %v", got)
	}
	if d.deficits[0].Uint64() != 500_000_000 {
		t.Fatalf("credits for other active queue were not advanced: %s", d.deficits[0])
	}
}

func TestSustainedBacklogCostFairness(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 2)
	mustQuantum(t, d, 1, 5)
	items0, items1 := make([]Item[int], 2000), make([]Item[int], 2000)
	for i := range items0 {
		items0[i] = Item[int]{i, 1}
		items1[i] = Item[int]{2000 + i, 2}
	}
	mustEnqueue(t, d, 0, items0)
	mustEnqueue(t, d, 1, items1)
	got := d.Take(900) // 100 complete pairs of visits; both queues stay backlogged.
	totals := [2]uint64{}
	for _, item := range got {
		totals[item.Queue] += item.Cost
	}
	if len(got) != 900 || totals[0] == 0 || totals[1] == 0 {
		t.Fatalf("incomplete fairness window: items=%d costs=%v", len(got), totals)
	}
	// Quantum 2:5 produces cost totals near 2:5. Queue 1's cost-2 items make
	// its item count differ from its cost share; allow finite-window deviation.
	difference := int64(totals[1]*2) - int64(totals[0]*5)
	if difference < 0 {
		difference = -difference
	}
	if difference > 10 {
		t.Fatalf("cost share outside finite-run tolerance: %v", totals)
	}
}

func TestConcurrentAddQueueReturnsDistinctConsecutiveIndices(t *testing.T) {
	const count = 128
	d := NewDWRR[int](0)
	indices := make(chan uint, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			indices <- d.AddQueue()
		}()
	}
	wg.Wait()
	close(indices)
	got := make([]uint, 0, count)
	for index := range indices {
		got = append(got, index)
	}
	if len(got) != count {
		t.Fatalf("got %d indices, want %d", len(got), count)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i, index := range got {
		if index != uint(i) {
			t.Fatalf("indices are not distinct consecutive valid indices: %v", got)
		}
	}
}

func TestConcurrentFIFOConservation(t *testing.T) {
	const count = 500
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, 1)
	mustQuantum(t, d, 1, 2)
	for q := uint(0); q < 2; q++ {
		items := make([]Item[int], count)
		for i := range items {
			items[i] = Item[int]{int(q)*count + i + 1, 1}
		}
		mustEnqueue(t, d, q, items)
	}
	var wg sync.WaitGroup
	got := make([]Scheduled[int], 0, 2*count)
	var mu sync.Mutex
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				part := d.Take(7)
				if len(part) == 0 {
					return
				}
				lastByQueue := make(map[uint]int)
				for _, item := range part {
					if last, ok := lastByQueue[item.Queue]; ok && item.Value <= last {
						t.Errorf("Take returned queue %d out of FIFO order: %d after %d", item.Queue, item.Value, last)
					}
					lastByQueue[item.Queue] = item.Value
				}
				mu.Lock()
				got = append(got, part...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(got) != 2*count {
		t.Fatalf("lost items: %d", len(got))
	}
	seen := make(map[int]bool)
	for _, v := range got {
		q := int(v.Queue)
		if q < 0 || q > 1 || v.Value < q*count+1 || v.Value > (q+1)*count || seen[v.Value] {
			t.Fatalf("conservation failure: %+v", v)
		}
		seen[v.Value] = true
	}
}

func TestConcurrentEnqueueQuantumAndTakeFIFO(t *testing.T) {
	const count = 400
	d := NewDWRR[int](2)
	start := make(chan struct{})
	producersDone := make(chan struct{})
	var producers sync.WaitGroup
	for queue := 0; queue < 2; queue++ {
		producers.Add(1)
		go func(queue int) {
			defer producers.Done()
			<-start
			for i := 0; i < count; i++ {
				if err := d.Enqueue(uint(queue), []Item[int]{{Value: queue*count + i, Cost: 1}}); err != nil {
					t.Errorf("enqueue queue %d item %d: %v", queue, i, err)
				}
				if err := d.SetQuantum(uint(queue), uint64(i%3+1)); err != nil {
					t.Errorf("set quantum for queue %d: %v", queue, err)
				}
			}
		}(queue)
	}

	seen := make([]bool, 2*count)
	var consumer sync.WaitGroup
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		<-start
		lastByQueue := [2]int{-1, -1}
		for {
			items := d.Take(9)
			if len(items) == 0 {
				select {
				case <-producersDone:
					return
				default:
					runtime.Gosched()
					continue
				}
			}
			for _, item := range items {
				queue := int(item.Queue)
				if queue < 0 || queue >= len(lastByQueue) || item.Value < 0 || item.Value >= len(seen) {
					t.Errorf("invalid item returned: %+v", item)
					continue
				}
				if item.Value/count != queue || item.Value <= lastByQueue[queue] || seen[item.Value] {
					t.Errorf("FIFO/conservation failure: item %+v after %d", item, lastByQueue[queue])
				}
				lastByQueue[queue] = item.Value
				seen[item.Value] = true
			}
		}
	}()

	close(start)
	producers.Wait()
	close(producersDone)
	consumer.Wait()
	for item, returned := range seen {
		if !returned {
			t.Errorf("item %d was not returned", item)
		}
	}
}

func TestConcurrentStructuralQueueOperations(t *testing.T) {
	const workers, iterations = 4, 100
	d := NewDWRR[int](2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				d.AddQueue()
				d.Take(1)
				d.RemoveQueue()
			}
		}()
	}
	close(start)
	wg.Wait()

	mustEnqueue(t, d, 0, []Item[int]{{10, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{20, 1}})
	got := d.Take(2)
	if !reflect.DeepEqual(got, []Scheduled[int]{{0, 10, 1}, {1, 20, 1}}) &&
		!reflect.DeepEqual(got, []Scheduled[int]{{1, 20, 1}, {0, 10, 1}}) {
		t.Fatalf("structural operations corrupted queues or cursor: %v", got)
	}
}
