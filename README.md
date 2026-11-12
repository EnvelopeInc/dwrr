# dwrr

`github.com/EnvelopeInc/dwrr` is a generic cost-aware Deficit Weighted Round Robin scheduler for FIFO queues. Requires Go 1.22.2 or newer.

## Install

```sh
go get github.com/EnvelopeInc/dwrr@v0.10.0
```

## Example

```go
package main

import (
	"fmt"

	"github.com/EnvelopeInc/dwrr"
)

func main() {
	s := dwrr.NewDWRR[string](2)
	if err := s.SetQuantum(0, 2); err != nil {
		panic(err)
	}
	if err := s.SetQuantum(1, 3); err != nil {
		panic(err)
	}
	if err := s.Enqueue(0, []dwrr.Item[string]{{Value: "a", Cost: 2}, {Value: "b", Cost: 2}}); err != nil {
		panic(err)
	}
	if err := s.Enqueue(1, []dwrr.Item[string]{{Value: "x", Cost: 4}, {Value: "y", Cost: 1}}); err != nil {
		panic(err)
	}

	fmt.Println(s.Take(1)) // [{0 a 2}]; queue 0's visit remains active
	fmt.Println(s.Take(1)) // [{0 b 2}]; queue 1 gets its first quantum and blocks at cost 4
	fmt.Println(s.Take(1)) // [{1 x 4}]; queue 1 accumulated enough deficit
	fmt.Println(s.Take(1)) // [{1 y 1}]
}
```

(The result formatting above is illustrative; the `Scheduled` fields are `Queue`, `Value`, and `Cost`.)

## Scheduling

`NewDWRR[T](count)` creates `count` queues, each with quantum 1. Queue indices start at 0, stay stable while a queue exists, and are fixed in count except for `AddQueue` and `RemoveQueue` (which removes only the last queue). A removed-last index may be reused by a later `AddQueue`; indices are not permanent IDs and are best suited to fixed or slowly changing lanes. `AddQueue()` appends an empty queue at quantum 1 and returns its index. The zero value is unsupported; use the constructor. `Item[T]` has a `Value` and a positive `Cost`; `Scheduled[T]` reports the source queue, value, and cost. `SetQuantum(queue, quantum)` configures a positive quantum and returns `ErrZeroQuantum` for zero. Setting the current quantum is idempotent and changes no scheduler state. `Enqueue` returns `ErrZeroCost` if any item has zero cost and validates the full input before changing the queue. It copies each item record, so an accepted cost cannot change if the caller later modifies its input slice; values of type `T` are shallow-copied. Invalid queue indices panic as slice indexing does.

The scheduler visits queues cyclically. At the start of a visit it adds that queue's quantum to its deficit, then serves affordable FIFO heads consecutively, subtracting each cost. An unaffordable head blocks later items and ends that visit. Unused deficit carries forward while the queue remains nonempty: this is what lets costs larger than one quantum eventually become eligible. Empty queues reset their deficit. A `Take` limit is only a retrieval limit, not a fairness cap: if it interrupts a visit, that visit and its remaining deficit persist, and the next call continues without awarding another quantum. Output is item-by-item in scheduler service order; queue identity remains in each result.

`Take(n)` returns up to n scheduled items in service order; it returns fewer only after the queued work is exhausted. It returns a nonnil empty slice for n=0, no queues, or no available work; negative n panics. `Take(0)` does not change scheduling state. The result is not preallocated from n, and its slice storage does not alias internal queues. Internal visits may process several items and cross multiple queue rounds in one call. A retrieval limit never ends a visit or caps a queue's fairness share.

Costs and quanta are limited to `MaxCost` and `MaxQuantum`, both `uint64(1<<63 - 1)`. `Enqueue` returns `ErrCostTooLarge` for a larger cost and validates the whole batch before changing the queue. `SetQuantum` checks the queue index first, then returns `ErrZeroQuantum` for zero or `ErrQuantumTooLarge` above the limit, without changing state. Deficits use exact `uint64` arithmetic: credit is below 2⁶³ outside an active visit, a grant adds at most `MaxQuantum`, and fast-forward keeps credit below the head cost. Pausing never grants credit. Once a complete cycle produces no item, the scheduler uses exact division to skip only whole unproductive cycles, crediting every nonempty queue equally before resuming visits in order.

## Queue operations and mutation semantics

All methods synchronize scheduler state; do not copy a `DWRR` after first use. `Take` selects and removes work; the caller owns downstream execution and its concurrency.

- `Enqueue` appends FIFO items. Appending to the active queue joins that same visit at its tail without another quantum.
- `SetQuantum` keeps leftover credit and any active visit. A reweight takes effect at the next visit's quantum grant, not when a paused visit resumes; an unchanged quantum is a true no-op. Credit carried from a completed visit is less than the head cost at that time, so this transition effect is bounded and brief. A paused visit keeps the credit it already received.
- `Take` selects and removes items in service order. `RemoveHead` directly removes the FIFO head without scheduling it, returning an item and `true` or the zero item and `false` when empty. If items remain, it preserves deficit and the active visit; emptying the queue resets deficit and cancels its active visit. `DrainQueue` directly removes all items, resets deficit, and cancels its active visit. Emptying via `Take` or `RemoveHead` retains the queue's remaining storage for reuse; `DrainQueue` and removing the queue release that storage.
- `AddQueue` appends an empty queue at quantum 1 and returns its index. Existing indices and an active visit remain unchanged.
- `RemoveQueue` drops the last queue and its contents; removing the active queue closes its visit. Other indices remain unchanged and the cursor stays valid. Removing from no queues is a no-op.

Cost fairness is fairness over application-declared costs, not execution time, bytes, or wall-clock resource use. Synchronization protects scheduler state, not concurrent mutation of values stored in `T`.
