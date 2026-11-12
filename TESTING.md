# Testing dwrr

How the test suite shows that dwrr schedules correctly, how to run it, and how to check for yourself that the tests can catch a bug.

## Running the tests

```sh
go test -race ./...                                        # full suite, with the race detector
go test -run='^$' -fuzz=FuzzDifferential -fuzztime=60s     # fuzz against the reference
go test -run='^$' -bench=. -benchmem                       # benchmarks, with allocations
go test -cover ./...                                       # statement coverage
```

The only test dependency is `github.com/stretchr/testify`, used by the external API tests.

CI runs on every push to `main`, on every pull request, and on version tags:
- `go test -race ./...` and `go vet ./...` on Go 1.22.x and on the current stable release;
- on stable only, a 30-second fuzz run of `FuzzDifferential`.

## The idea: trust the reference, not the code

The scheduler has optimizations that make it fast and harder to read:
- it skips empty queues using a bitmap;
- it fast-forwards through rounds in which no queue can afford its next item;
- it keeps exact credit in fixed-width integers.

Rather than trying to prove each of these correct by inspection, the tests compare the scheduler against a **reference model**: a second scheduler written to be obviously right instead of fast. It visits one queue at a time, never skips anything, and is short enough to check by reading.

The tests feed both schedulers the same long sequences of random operations, covering every public method including changes made partway through a visit. They require identical results at every step. If an optimization changed behavior in any way, the two would disagree.

The method has a limit. A reference checks that the code follows the rules; it can't show that the rules are right. Before v0.10.0, `SetQuantum` reset a queue's credit whenever its quantum changed. The reference model at the time followed the same rule, so it couldn't catch that a queue whose weight changes every round might never be served. A separate check that asked that question directly found the problem, and it's now a regression test, `TestSetQuantumDoesNotStarve`.

## What each test file checks

| File | What it checks |
|---|---|
| `dwrr_reference_test.go` | The naive reference model and the main differential tests. `TestDifferentialRandom` runs thousands of seeded random operation sequences against both schedulers. `FuzzDifferential` does the same with fuzz-generated input. Both check internal invariants after every operation: empty queues hold no credit; the scheduler's position stays in range; no visit sits on an empty queue; the busy-queue bitmap, busy count and item total match the queues. The `TestBoundary*` tests cover the largest allowed costs and quanta, rejected inputs leaving state unchanged, large fast-forward skips, and fast-forward making no allocations. |
| `dwrr_extreme_test.go` | `TestExtremeDifferentialBigReference` runs a second reference that keeps credit with `math/big`, using quanta of at least 2⁶² and costs up to `MaxCost`, and compares credit as well as results at every step. `TestBoundaryPeakCreditAfterReweight` drives a queue's credit to its maximum, `2⁶⁴ − 3`. |
| `dwrr_test.go` | Scenario and unit tests, including `TestReferenceGeneratedScenarios`, which runs the original reference-style model from before v0.10.0. The tests cover service order, credit carry-over, `Take` limits and pauses, validation and panics, mutations during a visit, sustained fairness under backlog, and concurrent use under the race detector. |
| `dwrr_mutation_test.go` | `TestReferenceGeneratedMutationScenarios`: 2,000 seeded scenarios mixing every operation, checked against the original reference model. |
| `dwrr_bitmap_test.go` | The busy-queue bitmap: searches that wrap across 64-bit words, removing and reusing the last queue, and service order with sparse busy queues. |
| `dwrr_storage_test.go` | Queue storage: an emptied queue reuses its storage without allocating, retained storage holds no references to removed values, and `DrainQueue` releases storage. |
| `dwrr_external_test.go` | The public API exercised from outside the package, as a user would call it. |
| `dwrr_bench_test.go` | Benchmarks: many idle queues with one busy (`TakeSparse10k`), one item at a time rotating across 16 queues (`TakeRotating16`), 16 continuously backlogged queues (`TakeBacklogged16`), batch retrieval (`TakeBatch64`) and large costs with small quanta (`LargeCostSmallQuantum`), which exercises fast-forward. All report allocations. |

## Why the arithmetic is exact

Credit is stored in a `uint64`. Costs and quanta are each limited to `MaxCost = MaxQuantum = 2⁶³ − 1`. The bound:

1. **Outside a visit, a queue's credit is at most `MaxCost − 1`.**
   - A visit ends only when the queue's credit is less than its next item's cost, and that cost is at most `MaxCost`.
   - Emptying a queue resets its credit to zero.
   - `RemoveHead` can expose a cheaper item, but it never increases credit.
2. **A visit begins by adding one quantum,** at most `MaxQuantum`. So credit never exceeds `MaxCost − 1 + MaxQuantum = 2⁶⁴ − 3`, which fits in a `uint64`.
3. **Fast-forward never overshoots.** It skips the largest number of whole rounds that leaves every waiting queue still unable to afford its next item, so it adds at most `cost − 1 − credit` to any queue, and neither its multiplication nor its addition can overflow.

`TestBoundaryPeakCreditAfterReweight` reaches the maximum:
- a queue blocks with `MaxCost − 1` credit;
- its quantum is changed to `MaxQuantum`, keeping that credit;
- `RemoveHead` exposes a cheap item;
- the next grant brings credit to `2⁶⁴ − 3`.

`TestExtremeDifferentialBigReference` then checks that credit near the top of the range matches an arbitrary-precision reference exactly.

## Check it yourself: plant a bug

Good tests fail when the code is wrong. To see it:

1. In `dwrr.go`, find the check in `Take` that decides whether a queue can afford its next item:
   ```go
   if d.deficits[q] < head.Cost {
   ```
2. Change `<` to `<=`, so a queue with exactly enough credit is treated as unable to afford the item.
3. Run:
   ```sh
   go test -run TestDifferentialRandom
   ```
   It fails within a second. Each failure shows what the scheduler returned (`got`), what the reference returned (`want`), and the sequence of operations that led there. You can replay that sequence by hand to see exactly where the two diverge.
4. Restore the file with `git checkout dwrr.go`.

Other bugs worth planting:
- remove the `- 1` in `fastForward`'s `(gap - 1) / d.quantums[i]`, so it skips one round too many;
- delete the line in `Take` that resets an emptied queue's credit (`d.deficits[q] = 0`); `popHead` has the same reset for `RemoveHead`.

The differential test catches each of these.

## Adding tests

- **Behavior changes** go in the reference model first. Then the implementation must match it. If the change is intentional, explain why the reference changed in the commit message.
- **Failing inputs** found by the fuzzer are saved under `testdata/fuzz/FuzzDifferential/`. After fixing the bug, commit them, so every future `go test` run replays them.
- **Numeric limits.** Any change that affects the bound above needs an updated argument here and a boundary test that reaches the new extreme.
