# V2.1 — Reversal and Fundamental Alpha

V2.1 expands the Universal Alpha Factory beyond time-series momentum while preserving the same paper-only, forward-evidence promotion boundary.

## New active research family: mean reversion

`mean_reversion_v1` searches point-in-time spot/perpetual history for statistically unusual price displacement using a robust median/MAD center and scale. It forecasts only a conservatively shrunk fraction of convergence back toward the robust center.

Discovery does not authorize allocation. A reversal candidate must independently pass the common Alpha Factory gates:

- non-overlapping forward sample minimum;
- confidence-adjusted realized net-return hurdle;
- Wilson hit-rate lower bound;
- multiple-testing penalty across registered alpha strategies;
- multi-regime robustness;
- fresh public L2 depth/slippage and venue fee economics;
- current conservative net-return hurdle.

## New provider-ready family: on-chain/fundamental composite

`onchain_fundamental_composite_v1` accepts normalized directional factor observations through an append-only point-in-time evidence contract.

A factor observation must carry:

- provider identity;
- asset and explicit as-of timestamp;
- named normalized factor scores in `[-1, 1]`;
- point-in-time lineage;
- `authoritative=true`;
- `commercial_use_permitted=true`.

The strategy fails closed when evidence is stale, incomplete, non-authoritative, or not licensed for the intended use. No public research feed is silently upgraded into allocation authority.

Even a valid factor observation produces only a research candidate. It must accumulate its own independent forward outcomes and pass the same statistical and L2 execution gates as every other predictive alpha family before it can enter the unified paper allocator.

## Independent forward evidence

V2.1 closes a statistical weakness in V2.0: overlapping forecasts can no longer inflate effective sample size.

- the worker will not create another open signal for the same strategy/asset/direction while a prior signal remains unresolved;
- qualification also de-overlaps previously persisted outcomes by their observation/due intervals;
- only non-overlapping outcomes count toward the minimum forward sample threshold and confidence calculations.

This protects the promotion gate even if older deployments accumulated overlapping V2.0 signals.

## Portfolio authority

The unified allocator remains the only common paper-capital comparison layer. Momentum, reversal, fundamental alpha, structural CEX opportunities and qualified CEX↔DEX opportunities compete under the same capital, venue, asset and instrument-conflict constraints.

Predictive alpha remains:

- `paper_only=true`;
- `executable_eligible=false`;
- `live_execution_eligible=false`.

No private keys, balances, custody, transaction signing, order submission or live-money authorization are added by V2.1.
