# dwrr

`github.com/EnvelopeInc/dwrr` is a generic Deficit Weighted Round Robin scheduler for FIFO queues. It assigns uniform cost to each item and schedules by item count; it is not byte-aware or configurable for application-specific item costs.

Requires Go 1.22.2 or newer.

## Install

```sh
go get github.com/EnvelopeInc/dwrr@v0.9.0
```

## Example

```go
package main

import (
	"fmt"

	"github.com/EnvelopeInc/dwrr"
)

func main() {
	s := dwrr.NewDWRR[string](2, 2)
	s.SetQuantum(1, 2)
	s.Enqueue(0, []string{"first"})
	s.Enqueue(1, []string{"second", "third"})
	fmt.Println(s.Do()) // one output slice per queue, in index order
}
```

## Scheduling and queue operations

`NewDWRR(count, maxTake)` creates `count` queues, each with quantum 1. `Do()` makes one round, visiting queues once in index order. For each non-empty queue, it adds the configured quantum to its deficit, saturating at the current backlog, then emits up to `min(deficit, maxTake)` items. It returns a `[][]T` with one entry per queue in index order; queues with no output have a nil entry. Unused deficit carries over while items remain and resets when the queue empties. A zero quantum provides no service until changed. `maxTake` is a per-queue, per-round item cap; zero means no items are emitted. A cap can limit or delay service implied by a quantum. Quantum and deficit count items only, regardless of size or processing cost.

- `SetQuantum(index, quantum)` changes the count-based quantum and clears that queue's deficit.
- `Enqueue(index, items)` appends items in FIFO order without changing quantum or deficit.
- `Dequeue(index)` removes the FIFO head and returns a pointer to a copy of the item, or `nil` if empty. Removing the last item clears deficit.
- `DequeueAll(index)` returns all items and clears the queue and its deficit.
- `AddQueue()` appends an empty queue with quantum 1.
- `RemoveQueue()` removes the last queue and discards its contents; with no queues it does nothing. Other queue indices do not change.

All indexed operations panic on an invalid index, consistent with slice indexing. Coordinate queue removal with operations using that index: a concurrently removed index may become invalid. Scheduler methods synchronize internal state, but do not make mutable values stored in `T` safe for concurrent access. Do not copy a `DWRR` after first use.

## Limitations

This scheduler does not account for bytes, size, runtime, or any cost other than one item. `maxTake` limits a queue's output even when its accumulated deficit is larger. Queue management only appends or removes the last queue; there is no arbitrary insertion, removal, or reordering.

## Contributing

Format Go changes with `gofmt`. Before opening a pull request, run:

```sh
go test -race ./...
go vet ./...
```

Pull requests should explain the user-visible change, include focused tests for behavior changes, and report relevant validation. Keep docs and tests consistent with the count-based scheduling semantics. CI runs the race tests and vet on Go 1.22.x and the current stable Go version.
