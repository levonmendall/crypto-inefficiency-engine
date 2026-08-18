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
- Conservative latency and hedge-recovery fallback buffers
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
- Adverse selection, spread/depth/slippage, edge decay, cost expansion, and capacity deterioration
- Explicit failure causes and segmented survival statistics

## v0.8 — Empirical fill/latency modeling — in progress
### Implemented
- Persist original target base quantity into shadow attribution
- Measure initial/verification scan duration as observation-path latency
- Reconstruct per-leg visible base-depth multiples
- Reconstruct pair fillability and hedge-reserve fillability
- Flag asymmetric visible-depth states that would require hedge recovery
- Build latency p50/p90/p95 distributions from unique verification scans
- Map measured latency quantile to a conservative shadow horizon
- Estimate pair-fill, reserve-fill, capture, and hedge-recovery probabilities
- Derive p50/p90/p95 pair adverse-selection distributions
- Gate empirical latency-risk use behind minimum evidence thresholds
- Automatically retain the fixed expected-latency model until the evidence gate passes
- Hierarchical empirical cohorts: strategy + venue pair + asset + capital, with controlled fallback through broader scopes
- Resolve a separate empirical model for each evaluated capital size
- Persist model scope and fallback provenance in executable capital-tier output
- Support scoped inspection through `GET /v1/latency/model`

### Next v0.8 refinements
- Interval-censored interpolation between the 1/5/15/30/60s horizons
- Separate network/data latency from hypothetical order-submission/acknowledgement latency
- Queue-position and maker-fill modeling where public venue data makes it defensible
- More explicit partial-fill sequencing and hedge-recovery cost distributions
- Statistical confidence intervals / minimum effective sample size by cohort

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg atomicity/hedge recovery, venue concentration limits, dead-man switches, and kill switch. This remains blocked until shadow and fill/latency evidence are statistically convincing.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing
