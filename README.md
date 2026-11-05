# DWRR (Deficit Weighted Round Robin) Scheduler

This Go package schedules generic items across dynamically managed FIFO queues. Each item has uniform cost 1: quantum values and deficits count items, not bytes, sizes, or application-defined work.

## Scheduling

`NewDWRR(count, maxTake)` creates `count` queues, each with a default quantum of 1. Use `SetQuantum(queue, quantum)` to configure a queue's item-count service quantum; a 1:2 quantum ratio grants queue two twice the count-based service of queue one over repeated rounds when both have backlog. Enqueueing items does not change the configured quantum. A zero quantum provides no service while configured as zero.

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

`AddQueue` appends a queue with quantum 1. `RemoveQueue` removes the last queue. Invalid indices for indexed queue operations, including `SetQuantum`, retain Go's index-out-of-range panic behavior.
