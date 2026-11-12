package dwrr

import (
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

func TestReferenceGeneratedMutationScenarios(t *testing.T) {
	const seed int64 = 0x5eedc0de
	const scenarios, operationsPerScenario = 2000, 60
	rng := rand.New(rand.NewSource(seed))
	for scenario := 0; scenario < scenarios; scenario++ {
		n := 1 + rng.Intn(4)
		d := NewDWRR[int](uint(n))
		r := &reference{queues: make([][]Item[int], n), quantum: make([]uint64, n), deficit: make([]uint64, n)}
		for q := range r.quantum {
			r.quantum[q] = 1
		}
		trace := make([]string, 0, operationsPerScenario)
		ensureQueue := func() {
			if len(r.queues) == 0 {
				if index := d.AddQueue(); index != uint(len(r.queues)) {
					t.Fatalf("AddQueue returned index %d, want %d", index, len(r.queues))
				}
				r.queues = append(r.queues, nil)
				r.quantum = append(r.quantum, 1)
				r.deficit = append(r.deficit, 0)
				trace = append(trace, "AddQueue() [required for indexed operation]")
			}
		}
		checkState := func(step int) {
			t.Helper()
			if !reflect.DeepEqual(d.queues, r.queues) || !reflect.DeepEqual(d.quantums, r.quantum) || d.cursor != r.cursor || d.active != r.active {
				t.Fatalf("seed=%d scenario=%d step=%d state mismatch\ntrace=%v\ngot queues=%v quantum=%v deficits=%v cursor=%d active=%t\nwant queues=%v quantum=%v deficits=%v cursor=%d active=%t", seed, scenario, step, trace, d.queues, d.quantums, d.deficits, d.cursor, d.active, r.queues, r.quantum, r.deficit, r.cursor, r.active)
			}
			for q := range r.deficit {
				if d.deficits[q].Cmp(new(big.Int).SetUint64(r.deficit[q])) != 0 {
					t.Fatalf("seed=%d scenario=%d step=%d deficit q=%d got=%s want=%d\ntrace=%v", seed, scenario, step, q, d.deficits[q], r.deficit[q], trace)
				}
			}
		}
		for step := 0; step < operationsPerScenario; step++ {
			switch rng.Intn(9) {
			case 0, 1, 2: // Include Take(0), Take(1), and broader bounded limits.
				limit := 0
				if step%3 == 1 {
					limit = 1
				} else if step%3 == 2 {
					limit = 2 + rng.Intn(5)
				}
				trace = append(trace, fmt.Sprintf("Take(%d)", limit))
				got, want := d.Take(limit), r.take(limit)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed=%d scenario=%d step=%d Take(%d) got=%v want=%v\ntrace=%v", seed, scenario, step, limit, got, want, trace)
				}
			case 3:
				ensureQueue()
				q := rng.Intn(len(r.queues))
				items := make([]Item[int], rng.Intn(3))
				for i := range items {
					items[i] = Item[int]{Value: scenario*10000 + step*10 + i, Cost: uint64(1 + rng.Intn(8))}
				}
				trace = append(trace, fmt.Sprintf("Enqueue(%d,%v)", q, items))
				mustEnqueue(t, d, uint(q), items)
				r.queues[q] = append(r.queues[q], items...)
			case 4:
				ensureQueue()
				q := rng.Intn(len(r.queues))
				trace = append(trace, fmt.Sprintf("RemoveHead(%d)", q))
				want := Item[int]{}
				wantOK := len(r.queues[q]) > 0
				if wantOK {
					want = r.queues[q][0]
					r.queues[q] = r.queues[q][1:]
					if len(r.queues[q]) == 0 {
						r.deficit[q] = 0
						if r.active && r.cursor == q {
							r.active = false
							r.cursor = (q + 1) % len(r.queues)
						}
					}
				}
				got, gotOK := d.RemoveHead(uint(q))
				if gotOK != wantOK || (gotOK && !reflect.DeepEqual(got, want)) {
					t.Fatalf("seed=%d scenario=%d step=%d RemoveHead got=%v want=%v\ntrace=%v", seed, scenario, step, got, want, trace)
				}
			case 5:
				ensureQueue()
				q := rng.Intn(len(r.queues))
				trace = append(trace, fmt.Sprintf("DrainQueue(%d)", q))
				want := append([]Item[int](nil), r.queues[q]...)
				got := d.DrainQueue(uint(q))
				r.queues[q], r.deficit[q] = nil, 0
				if r.active && r.cursor == q {
					r.active = false
					r.cursor = (q + 1) % len(r.queues)
				}
				if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
					t.Fatalf("seed=%d scenario=%d step=%d DrainQueue got=%v want=%v\ntrace=%v", seed, scenario, step, got, want, trace)
				}
			case 6:
				ensureQueue()
				q, quantum := rng.Intn(len(r.queues)), uint64(1+rng.Intn(8))
				trace = append(trace, fmt.Sprintf("SetQuantum(%d,%d)", q, quantum))
				mustQuantum(t, d, uint(q), quantum)
				r.quantum[q] = quantum // Reweighting preserves credit and the active visit.
			case 7:
				trace = append(trace, "AddQueue()")
				if index := d.AddQueue(); index != uint(len(r.queues)) {
					t.Fatalf("seed=%d scenario=%d step=%d AddQueue returned index %d, want %d", seed, scenario, step, index, len(r.queues))
				}
				r.queues = append(r.queues, nil)
				r.quantum = append(r.quantum, 1)
				r.deficit = append(r.deficit, 0)
			case 8:
				trace = append(trace, "RemoveQueue()")
				d.RemoveQueue()
				if len(r.queues) > 0 {
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
			}
			checkState(step)
		}
	}
}
