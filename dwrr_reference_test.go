package dwrr

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

// naiveDRR deliberately visits one queue at a time, with no skipping or
// production helpers. Values equal costs so its FIFO storage is costs only.
type naiveDRR struct {
	queues           [][]uint64
	quantum, deficit []uint64
	cursor           int
	active           bool
}

func newNaive(n int) *naiveDRR {
	r := &naiveDRR{queues: make([][]uint64, n), quantum: make([]uint64, n), deficit: make([]uint64, n)}
	for i := range r.quantum {
		r.quantum[i] = 1
	}
	return r
}
func (r *naiveDRR) close() { r.active = false; r.cursor = (r.cursor + 1) % len(r.queues) }
func (r *naiveDRR) take(n int) []Scheduled[uint64] {
	out := make([]Scheduled[uint64], 0)
	for len(out) < n {
		empty := true
		for _, q := range r.queues {
			if len(q) > 0 {
				empty = false
				break
			}
		}
		if empty {
			break
		}
		q := r.cursor
		if !r.active {
			if len(r.queues[q]) == 0 {
				r.deficit[q] = 0
				r.cursor = (q + 1) % len(r.queues)
				continue
			}
			r.deficit[q] += r.quantum[q]
			r.active = true
		}
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.close()
			continue
		}
		cost := r.queues[q][0]
		if r.deficit[q] < cost {
			r.close()
			continue
		}
		r.deficit[q] -= cost
		out = append(out, Scheduled[uint64]{uint(q), cost, cost})
		r.queues[q] = r.queues[q][1:]
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.close()
		}
	}
	return out
}
func (r *naiveDRR) setQuantum(q int, x uint64) error {
	if x == 0 {
		return ErrZeroQuantum
	}
	r.quantum[q] = x // Credit and an active visit survive reweighting.
	return nil
}
func (r *naiveDRR) remove(q int) (Item[uint64], bool) {
	if len(r.queues[q]) == 0 {
		return Item[uint64]{}, false
	}
	cost := r.queues[q][0]
	r.queues[q] = r.queues[q][1:]
	if len(r.queues[q]) == 0 {
		r.deficit[q] = 0
		if r.active && r.cursor == q {
			r.close()
		}
	}
	return Item[uint64]{cost, cost}, true
}
func (r *naiveDRR) drain(q int) []Item[uint64] {
	var out []Item[uint64]
	for _, cost := range r.queues[q] {
		out = append(out, Item[uint64]{cost, cost})
	}
	r.queues[q] = nil
	r.deficit[q] = 0
	if r.active && r.cursor == q {
		r.close()
	}
	return out
}
func (r *naiveDRR) add() {
	r.queues = append(r.queues, nil)
	r.quantum = append(r.quantum, 1)
	r.deficit = append(r.deficit, 0)
}
func (r *naiveDRR) removeQueue() {
	if len(r.queues) == 0 {
		return
	}
	last := len(r.queues) - 1
	if r.active && r.cursor == last {
		r.active = false
		r.cursor = 0
	}
	r.queues = r.queues[:last]
	r.quantum = r.quantum[:last]
	r.deficit = r.deficit[:last]
	if last == 0 {
		r.cursor = 0
	} else if r.cursor >= last {
		r.cursor %= last
	}
}

// Every decoded operation is bounded: at most six queues and two appended
// costs in [1,32]. No reference execution needs arithmetic fast-forwarding.
func runDifferential(t *testing.T, data []byte) {
	t.Helper()
	if len(data) == 0 {
		return
	}
	n := 1 + int(data[0]%6)
	d, r := NewDWRR[uint64](uint(n)), newNaive(n)
	trace := []string{fmt.Sprintf("New(%d)", n)}
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s\noperations:\n%v", fmt.Sprintf(format, args...), trace)
	}
	check := func() {
		t.Helper()
		if len(d.queues) > 0 && (d.cursor < 0 || d.cursor >= len(d.queues)) {
			fail("real cursor out of range")
		}
		if len(r.queues) > 0 && (r.cursor < 0 || r.cursor >= len(r.queues)) {
			fail("reference cursor out of range")
		}
		if d.active && (len(d.queues) == 0 || len(d.queues[d.cursor]) == 0) {
			fail("real active empty queue")
		}
		if r.active && (len(r.queues) == 0 || len(r.queues[r.cursor]) == 0) {
			fail("reference active empty queue")
		}
		for q := range r.queues {
			if len(d.queues[q]) == 0 && d.deficits[q].Sign() != 0 {
				fail("real empty queue %d has credit", q)
			}
			if len(r.queues[q]) == 0 && r.deficit[q] != 0 {
				fail("reference empty queue %d has credit", q)
			}
		}
	}
	check()
	for i := 1; i+3 < len(data); i += 4 {
		op, a, b, c := data[i]%9, data[i+1], data[i+2], data[i+3]
		if len(r.queues) == 0 && op >= 3 && op <= 6 {
			trace = append(trace, "AddQueue() [indexed operation]")
			d.AddQueue()
			r.add()
		}
		q := 0
		if len(r.queues) > 0 {
			q = int(a) % len(r.queues)
		}
		switch op {
		case 0, 1, 2:
			n := int(b % 6)
			trace = append(trace, fmt.Sprintf("Take(%d)", n))
			if got, want := d.Take(n), r.take(n); !reflect.DeepEqual(got, want) {
				fail("Take got=%v want=%v", got, want)
			}
		case 3:
			costs := []uint64{1 + uint64(b%32)}
			if c&1 != 0 {
				costs = append(costs, 1+uint64(c%32))
			}
			trace = append(trace, fmt.Sprintf("Enqueue(%d,%v)", q, costs))
			items := make([]Item[uint64], len(costs))
			for j, cost := range costs {
				items[j] = Item[uint64]{cost, cost}
			}
			if err := d.Enqueue(uint(q), items); err != nil {
				fail("Enqueue: %v", err)
			}
			r.queues[q] = append(r.queues[q], costs...)
		case 4:
			trace = append(trace, fmt.Sprintf("RemoveHead(%d)", q))
			got, ok := d.RemoveHead(uint(q))
			want, wok := r.remove(q)
			if got != want || ok != wok {
				fail("RemoveHead got=(%v,%t) want=(%v,%t)", got, ok, want, wok)
			}
		case 5:
			trace = append(trace, fmt.Sprintf("DrainQueue(%d)", q))
			if got, want := d.DrainQueue(uint(q)), r.drain(q); !reflect.DeepEqual(got, want) {
				fail("DrainQueue got=%v want=%v", got, want)
			}
		case 6:
			// Until SetQuantum preserves credit, only reweight empty queues.
			if len(r.queues[q]) > 0 || r.deficit[q] != 0 {
				break
			}
			x := uint64(b % 9)
			trace = append(trace, fmt.Sprintf("SetQuantum(%d,%d)", q, x))
			if got, want := d.SetQuantum(uint(q), x), r.setQuantum(q, x); got != want {
				fail("SetQuantum got=%v want=%v", got, want)
			}
		case 7:
			if len(r.queues) == 6 {
				break
			}
			trace = append(trace, "AddQueue()")
			if got := d.AddQueue(); got != uint(len(r.queues)) {
				fail("AddQueue index=%d", got)
			}
			r.add()
		case 8:
			trace = append(trace, "RemoveQueue()")
			d.RemoveQueue()
			r.removeQueue()
		}
		check()
	}
}

func TestDifferentialRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(0xD122))
	for trial := 0; trial < 3000; trial++ {
		data := make([]byte, 1+4*100)
		_, _ = rng.Read(data)
		t.Run(fmt.Sprint(trial), func(t *testing.T) { runDifferential(t, data) })
	}
}

func FuzzDifferential(f *testing.F) {
	// Each operation is {opcode, queue, argument, tail}; initial byte is n-1.
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 1, 0, 0, 1, 0})                                     // paused visit
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 1, 0, 0, 1, 0, 3, 0, 0, 0, 0, 0, 3, 0})             // append during visit
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 7, 3, 0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 1, 0}) // cheaper exposed head
	f.Add([]byte{1, 3, 1, 0, 1, 0, 0, 1, 0, 8, 0, 0, 0})                                     // remove active last queue
	f.Add([]byte{1, 3, 0, 31, 0, 3, 1, 23, 0, 0, 0, 2, 0})                                   // every head exceeds quantum
	f.Fuzz(func(t *testing.T, data []byte) { runDifferential(t, data) })
}

const boundaryMax = uint64(math.MaxUint64)

func TestBoundaryMaximumCostAndQuantum(t *testing.T) {
	d := NewDWRR[int](1)
	mustQuantum(t, d, 0, boundaryMax)
	mustEnqueue(t, d, 0, []Item[int]{{1, boundaryMax}, {2, boundaryMax}})
	if got, want := d.Take(2), ([]Scheduled[int]{{0, 1, boundaryMax}, {0, 2, boundaryMax}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
func TestBoundaryHugeSkipServiceOrder(t *testing.T) {
	d := NewDWRR[int](3)
	for q := uint(0); q < 3; q++ {
		mustEnqueue(t, d, q, []Item[int]{{int(q), boundaryMax - uint64(q)}})
	}
	want := []Scheduled[int]{{2, 2, boundaryMax - 2}, {1, 1, boundaryMax - 1}, {0, 0, boundaryMax}}
	if got := d.Take(3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
func TestBoundaryPausedCreditAndRemoval(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, boundaryMax)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, boundaryMax}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) {
		t.Fatal(got)
	}
	if got, ok := d.RemoveHead(0); !ok || got.Value != 2 {
		t.Fatal(got, ok)
	}
	if got := d.Take(2); !reflect.DeepEqual(got, []Scheduled[int]{{0, 3, 1}, {1, 9, 1}}) {
		t.Fatal(got)
	}
}
func TestBoundaryLaterMinimumGap(t *testing.T) {
	d := NewDWRR[int](3)
	for q, cost := range []uint64{31, 23, 9} {
		mustQuantum(t, d, uint(q), 2)
		mustEnqueue(t, d, uint(q), []Item[int]{{q, cost}})
	}
	want := []Scheduled[int]{{2, 2, 9}, {1, 1, 23}, {0, 0, 31}}
	if got := d.Take(3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
