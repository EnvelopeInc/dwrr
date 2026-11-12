package dwrr

import (
	"reflect"
	"testing"
)

func TestNextBusyWrapsAcrossWords(t *testing.T) {
	d := NewDWRR[int](130)
	if got := d.nextBusy(0); got != -1 {
		t.Fatalf("empty bitmap: %d", got)
	}
	for _, q := range []uint{1, 63, 65, 129} {
		mustEnqueue(t, d, q, []Item[int]{{int(q), 1}})
	}
	for _, tc := range []struct{ from, want int }{{0, 1}, {2, 63}, {63, 63}, {64, 65}, {66, 129}, {128, 129}, {129, 129}} {
		if got := d.nextBusy(tc.from); got != tc.want {
			t.Fatalf("nextBusy(%d)=%d want %d", tc.from, got, tc.want)
		}
	}
	d.DrainQueue(129)
	if got := d.nextBusy(129); got != 1 {
		t.Fatalf("cross-word wrap: %d", got)
	}
	d.DrainQueue(1)
	d.DrainQueue(63)
	if got := d.nextBusy(66); got != 65 {
		t.Fatalf("same-word wrap: %d", got)
	}
	if d.busyCount != 1 || d.total != 1 {
		t.Fatalf("draining corrupted counts: %d/%d", d.busyCount, d.total)
	}
}

func TestBusyBitmapLastQueueRemovalAndReuse(t *testing.T) {
	d := NewDWRR[int](130)
	mustEnqueue(t, d, 129, []Item[int]{{1, 1}, {2, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{129, 1, 1}}) || !d.active {
		t.Fatalf("prepare active last queue: %v", got)
	}
	d.RemoveQueue()
	if d.busyCount != 0 || d.total != 0 || d.busy[2] != 0 || d.cursor != 0 || d.active {
		t.Fatal("removal left stale busy state")
	}
	d.RemoveQueue() // Cross from 129 queues to 128, removing the third word.
	if len(d.busy) != 2 {
		t.Fatalf("bitmap words after shrink: %d", len(d.busy))
	}
	if got := d.AddQueue(); got != 128 || len(d.busy) != 3 || d.busy[2] != 0 {
		t.Fatalf("reused word contains stale state: index=%d busy=%v", got, d.busy)
	}
	mustEnqueue(t, d, 128, []Item[int]{{3, 1}})
	if item, ok := d.RemoveHead(128); !ok || item.Value != 3 || d.busyCount != 0 || d.total != 0 || d.busy[2] != 0 {
		t.Fatal("head removal left stale busy state")
	}
	mustEnqueue(t, d, 128, []Item[int]{{4, 1}})
	if got := d.Take(1); !reflect.DeepEqual(got, []Scheduled[int]{{128, 4, 1}}) || d.busyCount != 0 || d.total != 0 || d.busy[2] != 0 {
		t.Fatalf("Take left stale busy state: %v", got)
	}
}

func TestBusyBitmapSparseServiceOrder(t *testing.T) {
	d, r := NewDWRR[uint64](193), newNaive(193)
	for _, q := range []int{0, 63, 64, 127, 128, 192} {
		x := uint64(q%8 + 1)
		if err := d.SetQuantum(uint(q), x); err != nil {
			t.Fatal(err)
		}
		r.setQuantum(q, x)
		costs := []uint64{uint64(q%13 + 9), 3}
		for _, cost := range costs {
			if err := d.Enqueue(uint(q), []Item[uint64]{{cost, cost}}); err != nil {
				t.Fatal(err)
			}
		}
		r.queues[q] = costs
	}
	for i := 0; i < 14; i++ {
		if got, want := d.Take(1), r.take(1); !reflect.DeepEqual(got, want) {
			t.Fatalf("item %d got=%v want=%v", i, got, want)
		}
		if d.cursor != r.cursor || d.active != r.active {
			t.Fatalf("item %d cursor/visit mismatch", i)
		}
		for q := range r.deficit {
			if d.deficits[q] != r.deficit[q] {
				t.Fatalf("item %d queue %d credit mismatch", i, q)
			}
		}
	}
	if d.total != 0 || d.busyCount != 0 {
		t.Fatal("drained scheduler has busy queues")
	}
	for _, word := range d.busy {
		if word != 0 {
			t.Fatal("drained scheduler has busy bits")
		}
	}
}
