package dwrr

import "testing"

func TestEmptyingQueueReusesStorage(t *testing.T) {
	t.Run("Take", func(t *testing.T) {
		d := NewDWRR[int](1)
		items := []Item[int]{{Value: 7, Cost: 1}}
		mustEnqueue(t, d, 0, items)
		d.Take(1) // Warm the queue storage before measuring.
		allocations := testing.AllocsPerRun(100, func() {
			mustEnqueue(t, d, 0, items)
			if got := d.Take(1); len(got) != 1 || got[0] != (Scheduled[int]{Queue: 0, Value: 7, Cost: 1}) {
				t.Fatalf("unexpected service: %v", got)
			}
		})
		if allocations > 1 {
			t.Fatalf("Enqueue+Take allocated %g times, want at most one result allocation", allocations)
		}
	})
	t.Run("RemoveHead", func(t *testing.T) {
		d := NewDWRR[int](1)
		items := []Item[int]{{Value: 7, Cost: 1}}
		mustEnqueue(t, d, 0, items)
		d.RemoveHead(0) // Warm the queue storage before measuring.
		allocations := testing.AllocsPerRun(100, func() {
			mustEnqueue(t, d, 0, items)
			if got, ok := d.RemoveHead(0); !ok || got != items[0] {
				t.Fatalf("unexpected removal: %v, %t", got, ok)
			}
		})
		if allocations != 0 {
			t.Fatalf("Enqueue+RemoveHead allocated %g times, want zero", allocations)
		}
	})
}

func TestEmptyingQueueClearsRetainedSlots(t *testing.T) {
	for _, method := range []string{"Take", "RemoveHead"} {
		t.Run(method, func(t *testing.T) {
			d := NewDWRR[*int](1)
			d.queues[0] = make([]Item[*int], 0, 8)
			first, second, third := 1, 2, 3
			items := []Item[*int]{{&first, 1}, {&second, 1}, {&third, 1}}
			mustEnqueue(t, d, 0, items)
			backing := d.queues[0][:cap(d.queues[0])]
			for _, want := range items {
				if method == "Take" {
					if got := d.Take(1); len(got) != 1 || got[0].Value != want.Value || got[0].Cost != want.Cost {
						t.Fatalf("unexpected service: %v", got)
					}
				} else if got, ok := d.RemoveHead(0); !ok || got != want {
					t.Fatalf("unexpected removal: %v, %t", got, ok)
				}
			}
			q := d.queues[0]
			if len(q) != 0 || cap(q) == 0 {
				t.Fatalf("expected empty retained storage, len/cap=%d/%d", len(q), cap(q))
			}
			for i, item := range q[:cap(q)] {
				if item != (Item[*int]{}) {
					t.Fatalf("retained slot %d is not clear: %v", i, item)
				}
			}
			for i, item := range backing {
				if item != (Item[*int]{}) {
					t.Fatalf("original slot %d is not clear: %v", i, item)
				}
			}
		})
	}
}

func TestDrainQueueReleasesStorage(t *testing.T) {
	for _, state := range []string{"nonempty", "Take", "RemoveHead"} {
		t.Run(state, func(t *testing.T) {
			d := NewDWRR[int](1)
			mustEnqueue(t, d, 0, []Item[int]{{Value: 1, Cost: 1}})
			switch state {
			case "Take":
				d.Take(1)
			case "RemoveHead":
				d.RemoveHead(0)
			}
			if cap(d.queues[0]) == 0 {
				t.Fatal("missing storage before drain")
			}
			d.DrainQueue(0)
			if d.queues[0] != nil {
				t.Fatal("DrainQueue retained queue storage")
			}
		})
	}
}
