# Changelog

## v0.10.1

- Module path moved to `github.com/version2beta/dwrr`. No code changes.
- v0.9.0 and v0.10.0 remain available at `github.com/EnvelopeInc/dwrr`.

## v0.10.0

- Replace the unit-cost `Do` API with cost-aware `Take`, returning items in scheduler service order. Retrieval limits preserve a paused visit and its credit.
- Keep deficit and any active visit when `SetQuantum` changes a queue's weight. The new quantum applies at the next grant, preventing repeated reweighting from starving large-cost heads.
- Limit costs and quanta to `MaxCost` and `MaxQuantum`, both `uint64(1<<63 - 1)`. Oversized inputs return `ErrCostTooLarge` or `ErrQuantumTooLarge` without changing state. Exact fixed-width deficits replace `math/big`.
- Skip empty queues with a busy-queue bitmap and bound result allocation by the queued item count, preserving service order while improving sparse-queue performance.
- Reuse remaining queue storage when `Take` or `RemoveHead` empties a queue; `DrainQueue` and removing a queue release its storage.
- Add an independent naive reference model, deterministic mutation comparisons, boundary tests, differential fuzz testing, and allocation-reporting benchmarks. Arbitrary-precision top-of-range differential tests and a peak-credit reweighting test verify exact deficit arithmetic. Stable-Go CI runs the fuzz target.
