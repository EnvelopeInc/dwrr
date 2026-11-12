package dwrr

import "testing"

func BenchmarkTakeSparse10k(b *testing.B) {
	d := NewDWRR[int](10_000)
	items := []Item[int]{{Value: 1, Cost: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(9_999, items); err != nil {
			b.Fatal(err)
		}
		d.Take(1)
	}
}

func BenchmarkTakeRotating16(b *testing.B) {
	d := NewDWRR[int](16)
	for q := uint(0); q < 16; q++ {
		if err := d.SetQuantum(q, uint64(q+1)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(uint(i%16), []Item[int]{{Value: i, Cost: uint64(i%7 + 1)}}); err != nil {
			b.Fatal(err)
		}
		d.Take(1)
	}
}

func BenchmarkTakeBacklogged16(b *testing.B) {
	d := NewDWRR[int](16)
	for q := uint(0); q < 16; q++ {
		if err := d.SetQuantum(q, uint64(q+1)); err != nil {
			b.Fatal(err)
		}
		items := make([]Item[int], 64)
		for i := range items {
			items[i] = Item[int]{Value: int(q)*64 + i, Cost: uint64(i%7 + 1)}
		}
		if err := d.Enqueue(q, items); err != nil {
			b.Fatal(err)
		}
	}
	items := make([]Item[int], 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := d.Take(1)[0]
		items[0] = Item[int]{Value: item.Value, Cost: item.Cost}
		if err := d.Enqueue(item.Queue, items); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTakeBatch64(b *testing.B) {
	d := NewDWRR[int](64)
	items := []Item[int]{{Value: 1, Cost: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for q := uint(0); q < 64; q++ {
			if err := d.Enqueue(q, items); err != nil {
				b.Fatal(err)
			}
		}
		d.Take(64)
	}
}

func BenchmarkLargeCostSmallQuantum(b *testing.B) {
	d := NewDWRR[int](8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for q := uint(0); q < 8; q++ {
			if err := d.Enqueue(q, []Item[int]{{Value: int(q), Cost: 1_000_000_000_000 + uint64(q)}}); err != nil {
				b.Fatal(err)
			}
		}
		d.Take(8)
	}
}
