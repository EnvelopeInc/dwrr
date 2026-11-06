# DWRR (Deficit Weighted Round Robin) Scheduler

This Go package schedules generic items across dynamically managed FIFO queues. Each item has uniform cost 1: quantum values and deficits count items, not bytes, sizes, or application-defined work.

## Scheduling

`NewDWRR(count, maxTake)` creates `count` queues, each with a default quantum of 1. Use `SetQuantum(queue, quantum)` to configure a queue's item-count service quantum; a 1:2 quantum ratio grants queue two twice the count-based service of queue one over repeated rounds when both have backlog and `maxTake` does not limit either queue. Enqueueing items does not change the configured quantum. A zero quantum provides no service while configured as zero.

One `Do()` call is one round: the scheduler visits each queue once in index order. For each non-empty queue, its quantum is added to its deficit, saturated at its current backlog. It emits up to the deficit and `maxTake`; unused deficit carries forward while backlog remains, and resets when the queue becomes empty. `maxTake` is a per-queue per-round item limit; `maxTake == 0` means `Do()` emits no items. Thus `maxTake` can constrain or delay the service implied by a quantum. Queue operations and scheduling are protected by a mutex.

`Do()` returns one output slice per queue in the same order as the queue indices:

```go
package main

import (
	"fmt"

	"github.com/EnvelopeInc/dwrr"
)

func main() {
	scheduler := dwrr.NewDWRR[string](2, 2)
	scheduler.SetQuantum(1, 2)
	scheduler.Enqueue(0, []string{"first"})
	scheduler.Enqueue(1, []string{"second", "third"})
	fmt.Println(scheduler.Do())
}
```

`AddQueue` appends an empty queue with quantum 1. `RemoveQueue` removes the last queue and does nothing if there are no queues. `Enqueue` appends items to one queue without changing its quantum. `Dequeue` removes and returns the first item (or `nil` if empty), resetting deficit if it empties the queue; `DequeueAll` returns all queued items and resets deficit. All indexed queue operations panic for an invalid index, consistent with Go slice indexing. Scheduler operations are mutex-protected; a `DWRR` must not be copied after first use.

This is a count-based scheduler, not a byte- or cost-aware scheduler: every item consumes one unit regardless of its size or processing cost. Deficit is retained only while a queue remains non-empty; emptying it through `Do`, `Dequeue`, or `DequeueAll` clears that credit. Setting a quantum, including setting it to zero, also clears the queue's accumulated deficit. Changing queue count only appends or removes the last queue; it does not reorder existing queues.

## Development and releases

Run the CI checks locally with `go test -race ./...` and `go vet ./...`. GitHub Actions runs these checks for pull requests and pushes to `main`, as well as version tags. See [RELEASING.md](RELEASING.md) for versioning and release steps.
