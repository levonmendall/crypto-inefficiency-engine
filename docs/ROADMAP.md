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

Future operational work: retention/export policy, historical funding ingestion, partition/archival policy, and scan-level quality metrics.

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

## Milestone 4 — Broader alpha graph — deferred
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

## v0.8 — Empirical fill/latency modeling — complete
- Persist original target quantity into shadow attribution
- Measure public L2 request round-trip latency on Coinbase and Hyperliquid
- Preserve whole-scan latency only as an explicitly labeled historical fallback
- Separate measured collector/data timing from hypothetical order-ack and second-leg timing
- Expose that execution latency is not empirical while no live order path exists
- Reconstruct per-leg visible base-depth multiples
- Reconstruct pair/reserve fillability and visible taker fill fractions
- Quantify asymmetric partial fills and unmatched/unhedged exposure
- Estimate partial-fill probability and p50/p90/p95 unhedged-fraction distributions
- Estimate p50/p90/p95 hedge-recovery-loss proxies
- Ensure empirical recovery risk can increase, but never reduce, the fixed recovery floor
- Build p50/p90/p95 adverse-selection distributions
- Hierarchical empirical cohorts: strategy + venue pair + asset + capital, with controlled fallback through broader scopes
- Resolve a separate empirical model for every evaluated capital size
- Interval-censored interpolation between adjacent 1/5/15/30/60s horizons
- Enforce monotone-conservative quality/risk interpolation
- Cluster capital tiers from the same market event so correlated rows do not inflate sample size
- Wilson confidence intervals for fill/reserve/capture/recovery probabilities
- Gate calibration on raw count, effective independent-event count, tail-risk samples, and maximum CI width at every endpoint
- Persist model scope, effective sample size, confidence intervals, and fallback provenance in executable output
- Scoped inspection through `GET /v1/latency/model`
- Explicitly abstain from maker queue-position/fill probability because current public L2 cannot establish queue position
- Automatically retain the fixed conservative model whenever any empirical gate fails

v0.8 is complete at the paper/shadow evidence boundary. It does not claim exchange-confirmed fills or empirical order acknowledgement latency.

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg atomicity/hedge recovery, venue concentration limits, dead-man switches, and kill switch. This remains blocked until accumulated v0.8 evidence is statistically convincing and a separate live-execution decision is explicitly authorized.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing
