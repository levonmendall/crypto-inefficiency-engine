# Roadmap

## Milestone 1 — Discovery core — complete
- Canonical quote/funding models
- Public Hyperliquid and Coinbase adapters
- Funding-dispersion and spot/perp-basis detectors
- Conservative screening hurdle
- Paper-only execution boundary
- Read-only API

## Milestone 2 — Point-in-time evidence — foundation complete
- Append-only SQLite/PostgreSQL observation store
- Provider health and source timestamps
- Opportunity/order-book/executability snapshots
- Lineage hashes and exact analysis configuration
- Deterministic replay harness

Next: retention/export policy, historical funding ingestion, partition/archival policy, and scan-level quality metrics.

## Milestone 3 — Executability and economic realism — core complete
- L2 parsers/adapters
- Depth-aware VWAP and slippage
- Exact-base-quantity paired-leg sizing
- Capital tiers and continuous capacity frontier
- Freshness/skew gates
- Venue-specific taker fees
- Capital requirement and collateral opportunity cost
- Conservative latency and hedge-recovery buffers
- Extra hedge-liquidity reserve
- Fail-closed short-spot borrow economics

## Milestone 4 — Broader alpha graph
- Direct exchange adapters
- CEX/CEX spot dislocations
- CEX/DEX and DEX/DEX routing
- Cross-chain liquidity and stablecoin dislocations
- Futures term structure and options relative value

This remains secondary until the existing alpha is empirically shown to survive market contact.

## Milestone 5 — Shadow evidence runtime — v0.7 complete
- Durable always-on worker and managed PostgreSQL topology
- Stable economic opportunity signatures
- Re-test every initially qualified capital-tier cohort
- Default 1s/5s/15s/30s/60s verification horizons
- Per-leg adverse-selection attribution
- Spread/depth/slippage deterioration
- Funding/basis edge decay and cost expansion
- Capacity deterioration
- Failure causes: signal disappearance, insufficient depth, slippage expansion, fee/cost hurdle, stale/provider failure, hedge-leg divergence
- Survival by strategy, asset, venue pair, capital size, time of day, and initial expected return
- Median lifetime lower bound, survival probabilities, edge decay, deployable-capital estimate, false-positive rate, and capture-probability proxy

## v0.8 — Empirical fill/latency modeling — next
- Replace conservative hard-coded latency buffers with measured latency distributions
- Estimate likely fill probability from observed book evolution
- Reconstruct partial-fill and hedge-delay states
- Model queue/arrival uncertainty where public data supports it
- Calibrate capture probability from measured execution timing rather than horizon survival alone

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg atomicity/hedge recovery, venue concentration limits, dead-man switches, and kill switch. This remains blocked until shadow and fill/latency evidence are statistically convincing.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing
