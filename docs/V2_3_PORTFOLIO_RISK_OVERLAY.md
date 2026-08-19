# V2.3 Cross-Strategy Portfolio Risk Overlay

V2.3 closes a portfolio-level gap created by broadening the engine beyond market-neutral inefficiencies.

A collection of independently qualified opportunities is not automatically a good portfolio. Multiple directional alpha strategies can all be statistically valid while loading on the same underlying crypto beta. Existing venue, asset and instrument-conflict caps do not fully address that risk.

## Exposure classification

Every unified allocation candidate now carries an explicit exposure kind:

- `market_neutral`
- `directional_long`
- `directional_short`

Core funding/basis/dislocation and qualified CEX↔DEX opportunities remain market-neutral at the allocator boundary. Predictive alpha inherits its long/short direction from the promoted alpha candidate.

## Portfolio budgets

The overlay is deliberately subtractive. It can reject an otherwise qualified candidate but cannot create eligibility.

Initial conservative budgets are:

- predictive alpha: at most 40% of total paper capital;
- all directional exposure: at most 35%;
- one directional side: at most 25%;
- one predictive-alpha strategy: at most 20%.

These controls sit alongside the existing total-capital, venue, asset, allocation-count and shared-instrument/route constraints.

## Why this matters

Without the overlay, momentum in BTC, momentum in ETH, a fundamental long in SOL and another directional strategy could all pass independently and leave the portfolio dominated by one broad market move. V2.3 constrains that portfolio-level concentration even when each signal looks attractive in isolation.

Market-neutral opportunities do not consume the directional budget, so the engine can still deploy capital to independent arbitrage/carry opportunities while keeping directional beta controlled.

## Observability

Every `UnifiedPaperAllocationPlan` now includes a `portfolio_risk_budget` snapshot containing:

- predictive-alpha capital;
- market-neutral capital;
- gross directional capital;
- long directional capital;
- short directional capital;
- net directional capital;
- capital by predictive strategy.

Skipped candidates record the specific portfolio-risk budget that blocked them.

## Authority boundary

The overlay is paper-only and has no order-entry authority. It runs after opportunity/alpha qualification and can only reduce the set of allocations. It cannot override evidence requirements, create synthetic opportunity economics, or authorize live money.
