package dwrr

import (
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

// Naive DRR with arbitrary-precision credit, used only to check the uint64
// implementation at the top of its range.
type bigRef struct {
	q      [][]uint64
	qt     []uint64
	df     []*big.Int
	cur    int
	active bool
}

func (r *bigRef) close() { r.active = false; r.cur = (r.cur + 1) % len(r.q) }
func (r *bigRef) take(n int) []Scheduled[uint64] {
	out := []Scheduled[uint64]{}
	for len(out) < n {
		any := false
		for _, x := range r.q {
			if len(x) > 0 {
				any = true
			}
		}
		if !any {
			break
		}
		c := r.cur
		if !r.active {
			if len(r.q[c]) == 0 {
				r.df[c].SetUint64(0)
				r.cur = (c + 1) % len(r.q)
				continue
			}
			r.df[c].Add(r.df[c], new(big.Int).SetUint64(r.qt[c]))
			r.active = true
		}
		h := new(big.Int).SetUint64(r.q[c][0])
		if r.df[c].Cmp(h) < 0 {
			r.close()
			continue
		}
		r.df[c].Sub(r.df[c], h)
		out = append(out, Scheduled[uint64]{uint(c), r.q[c][0], r.q[c][0]})
		r.q[c] = r.q[c][1:]
		if len(r.q[c]) == 0 {
			r.df[c].SetUint64(0)
			r.close()
		}
	}
	return out
}

func TestExtremeDifferentialBigReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	cost := func() uint64 {
		switch rng.Intn(3) {
		case 0:
			return 1 + uint64(rng.Intn(4))
		case 1:
			return MaxCost - uint64(rng.Intn(4))
		default:
			return 1 + uint64(rng.Int63n(int64(MaxCost)))
		}
	}
	quantum := func() uint64 { return MaxQuantum - uint64(rng.Int63n(1<<62)) } // >= 2^62: at most two grants per head
	for trial := 0; trial < 20000; trial++ {
		n := 1 + rng.Intn(4)
		d := NewDWRR[uint64](uint(n))
		r := &bigRef{q: make([][]uint64, n), qt: make([]uint64, n), df: make([]*big.Int, n)}
		for i := 0; i < n; i++ {
			x := quantum()
			if err := d.SetQuantum(uint(i), x); err != nil {
				t.Fatal(err)
			}
			r.qt[i], r.df[i] = x, new(big.Int)
		}
		for step := 0; step < 40; step++ {
			i := rng.Intn(n)
			switch rng.Intn(5) {
			case 0, 1:
				c := cost()
				if err := d.Enqueue(uint(i), []Item[uint64]{{c, c}}); err != nil {
					t.Fatal(err)
				}
				r.q[i] = append(r.q[i], c)
			case 2:
				x := quantum()
				_ = d.SetQuantum(uint(i), x)
				r.qt[i] = x
			case 3:
				got, ok := d.RemoveHead(uint(i))
				if ok != (len(r.q[i]) > 0) || (ok && got.Cost != r.q[i][0]) {
					t.Fatalf("trial %d: RemoveHead mismatch", trial)
				}
				if ok {
					r.q[i] = r.q[i][1:]
					if len(r.q[i]) == 0 {
						r.df[i].SetUint64(0)
						if r.active && r.cur == i {
							r.close()
						}
					}
				}
			default:
				k := rng.Intn(4)
				if a, b := d.Take(k), r.take(k); !reflect.DeepEqual(a, b) {
					t.Fatalf("trial %d step %d: got %v want %v", trial, step, a, b)
				}
			}
			for j := range r.df {
				if !r.df[j].IsUint64() || r.df[j].Uint64() != d.deficits[j] {
					t.Fatalf("trial %d step %d: deficit %d got %d want %s", trial, step, j, d.deficits[j], r.df[j])
				}
			}
		}
	}
}

// Leftover MaxCost-1, reweight to MaxQuantum (now allowed to keep credit),
// expose a cheap head: the grant reaches the true peak, 2^64-3.
func TestBoundaryPeakCreditAfterReweight(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, MaxCost-1)
	mustEnqueue(t, d, 0, []Item[int]{{1, MaxCost}, {2, 1}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	// Queue 0 is granted MaxCost-1, blocks; queue 1 serves and empties.
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) || d.deficits[0] != MaxCost-1 || d.active {
		t.Fatalf("setup: %v %d", got, d.deficits[0])
	}
	mustQuantum(t, d, 0, MaxQuantum) // credit is kept
	d.RemoveHead(0)                  // exposes a cost-1 head
	got := d.Take(1)                 // grant reaches MaxCost-1+MaxQuantum = 2^64-3
	if !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 1}}) || d.deficits[0] != ^uint64(0)-3 || !d.active {
		t.Fatalf("peak: %v deficit=%d", got, d.deficits[0])
	}
}
