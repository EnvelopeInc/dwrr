# Changelog

## v0.10.0

- Replace the unit-cost `Do` API with cost-aware `Take`, returning items in scheduler service order. Retrieval limits preserve a paused visit and its credit.
- Keep deficit and any active visit when `SetQuantum` changes a queue's weight. The new quantum applies at the next grant, preventing repeated reweighting from starving large-cost heads.
- Limit costs and quanta to `MaxCost` and `MaxQuantum`, both `uint64(1<<63 - 1)`. Oversized inputs return `ErrCostTooLarge` or `ErrQuantumTooLarge` without changing state. Exact fixed-width deficits replace `math/big`.
- Skip empty queues with a busy-queue bitmap and bound result allocation by the queued item count, preserving service order while improving sparse-queue performance.
- Add an independent naive reference model, deterministic mutation comparisons, boundary tests, differential fuzz testing, and allocation-reporting benchmarks. Stable-Go CI runs the fuzz target.
