# Roadmap

## Milestone 1 — Discovery core — complete
- Canonical quote/funding models
- Hyperliquid predicted-funding parser
- Coinbase public spot adapter
- Hyperliquid perp context adapter
- Funding-dispersion detector
- Basis detector
- Conservative screening hurdle
- Paper executor
- Read-only FastAPI service
- Unit tests

## Milestone 2 — Point-in-time evidence — foundation complete
- Append-only SQLite observation store
- PostgreSQL-backed production evidence store through the same persistence interface
- Append-only opportunity snapshots
- Provider health/degradation records
- Source timestamps and lineage hashes
- Exact analysis configuration captured per scan
- Deterministic replay harness

Next: retention/export policy, historical funding ingestion, partition/archival policy, and scan-level quality metrics.

## Milestone 3 — Executability and economic realism — core complete
- Canonical L2 order-book model
- Hyperliquid perpetual L2 parser/adapter
- Coinbase spot Level-2 parser/adapter
- Depth-aware VWAP and slippage
- Exact-base-quantity paired-leg sizing
- Configurable capital tiers
- Continuous capacity-frontier search
- Book freshness and cross-book timestamp-skew gates
- Conservative venue-specific taker fees
- Capital requirement across both legs
- Collateral opportunity-cost model
- Book-age + expected hedge-latency risk charge
- Hedge-recovery buffer
- Extra visible hedge-liquidity reserve
- Short-spot borrow cost fails closed when unavailable
- Execution evidence persisted and replayable

Next: authenticated account-specific fee tiers, measured latency distributions, dynamic borrow feeds, collateral/liquidation stress, partial-fill state machine, and direct L2 adapters for additional perp venues.

## Milestone 4 — Broader alpha graph
- Direct exchange adapters
- CEX/CEX spot dislocations
- CEX/DEX routing
- DEX/DEX arbitrage
- Cross-chain liquidity
- Stablecoin dislocations
- Futures term structure
- Options relative value

## Milestone 5 — Live shadow trading — foundation complete
- Re-scan live opportunities after a configurable delay
- Match opportunities by stable economic signature rather than observation ID
- Re-test the original target notional against fresh market data and L2
- Classify signal disappearance vs execution failure vs survival
- Persist append-only shadow-cycle evidence
- Aggregate empirical survival rate
- Long-running `cie shadow-loop`
- Resilient `cie worker` with durable heartbeat/error telemetry
- Render worker + managed Postgres Blueprint

Next: deploy the persistent worker, accumulate a statistically useful evidence set, then add multiple verification horizons, price-path/adverse-selection attribution, empirical latency distributions, likely-fill reconstruction, and survival estimates by strategy/venue/asset/size.

## Milestone 6 — Tiny-capital controlled execution
Separate service, separate credentials, explicit authorization, hard capital caps, dead-man switches, paired-leg atomicity/hedge recovery, venue concentration limits, kill switch. This milestone remains blocked until shadow evidence is statistically convincing.

## Milestone 7 — Machine-paid intelligence API
- API keys / usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional x402-compatible payment gateway
- Never expose private positions or proprietary execution timing
