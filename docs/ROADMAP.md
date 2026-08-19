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

## Milestone 4 — Universal alpha graph — v0.9 foundation complete
### v0.9.0 foundation
- Stable canonical crypto asset IDs independent of provider symbol
- Stable current spot/perpetual venue-instrument IDs with extensible contract keys
- Provider symbols retained as aliases rather than primary identity
- Venue, instrument, asset, listing, representation, and economic-equivalence graph relationships
- Common detector context and detector registry
- Existing funding-dispersion and spot/perp-basis strategies migrated through the registry without changing their economics
- Graph lineage attached to discovered opportunities
- Live graph inspection through `GET /v1/graph/live`
- Detector manifest through `GET /v1/detectors`
- Common ranking of already-qualified opportunities by capital-adjusted net annualized return
- Capacity kept explicit beside ranking score
- Ranking explicitly has no allocator or execution authority

### Next breadth slices
1. Additional direct CEX/perpetual venue adapters so funding/basis are genuinely multi-venue
2. Dated futures identity + futures term-structure/basis detector
3. CEX/CEX spot dislocation detector
4. Stablecoin dislocation graph and detector
5. DEX pool/route identity and DEX/DEX routing
6. CEX/DEX comparison and transfer/settlement cost edges
7. Cross-chain/bridge liquidity edges
8. Liquidation/backstop and solver opportunity modules
9. Options/volatility relative-value graph extensions
10. Portfolio/capital allocator only after several independent strategies have statistically credible shadow evidence

## Milestone 5 — Shadow evidence runtime — v0.7 complete
- Durable always-on worker and managed PostgreSQL topology
- Stable economic opportunity signatures
- Re-test every initially qualified capital-tier cohort
- Default 1s/5s/15s/30s/60s verification horizons
- Adverse selection, spread/depth/slippage, edge decay, cost expansion, and capacity deterioration
- Explicit failure causes and segmented survival statistics

## v0.8 — Empirical fill/latency modeling — complete
- Measure public L2 request round-trip latency on Coinbase and Hyperliquid
- Preserve whole-scan latency only as an explicitly labeled historical fallback
- Separate measured collector/data timing from hypothetical order-ack and second-leg timing
- Reconstruct pair/reserve fillability and visible taker fill fractions
- Quantify asymmetric partial fills and unmatched/unhedged exposure
- Estimate partial-fill probability and p50/p90/p95 unhedged-fraction/recovery-loss distributions
- Hierarchical per-capital empirical cohorts with conservative fallback
- Interval-censored interpolation between adjacent shadow horizons
- Cluster correlated capital-tier rows by independent market event
- Wilson confidence intervals and effective-sample-size gates
- Automatically retain the fixed conservative model whenever any empirical gate fails
- Explicitly abstain from maker queue-position/fill probability when public data cannot prove it

v0.8 is complete at the paper/shadow evidence boundary. It does not claim exchange-confirmed fills or empirical order acknowledgement latency.

## Milestone 6 — Tiny-capital controlled execution — blocked
Separate service, credentials, explicit authorization, hard caps, paired-leg atomicity/hedge recovery, venue concentration limits, dead-man switches, and kill switch. This remains blocked until accumulated evidence is statistically convincing and a separate live-execution decision is explicitly authorized.

## Milestone 7 — Machine-paid intelligence API
- API keys and usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional machine-payment gateway
- Never expose private positions or proprietary execution timing
