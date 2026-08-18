# Roadmap

## Milestone 1 — Discovery core — complete
- Canonical quote/funding models
- Hyperliquid predicted-funding parser
- Coinbase public spot adapter
- Hyperliquid perp context adapter
- Funding-dispersion detector
- Basis detector
- Conservative cost hurdle
- Paper executor
- Read-only FastAPI service
- Unit tests

## Milestone 2 — Point-in-time evidence — foundation complete
- Append-only SQLite observation store
- Append-only opportunity snapshots
- Provider health/degradation records
- Source timestamps and lineage hashes
- Exact analysis configuration captured per scan
- Deterministic replay harness

Next: long-running collection process, retention/export policy, Postgres option, historical funding ingestion, and scan-level quality metrics.

## Milestone 3 — Executability — core complete
- Canonical L2 order-book model
- Hyperliquid perpetual L2 parser/adapter
- Coinbase spot Level-2 parser/adapter
- Depth-aware VWAP and slippage
- Exact-base-quantity paired-leg sizing
- $1K/$10K/$25K/$50K/$100K qualification tiers
- Dynamic net-return recomputation after observed entry slippage and conservative exit slippage
- Book freshness and cross-book timestamp-skew gates
- Missing venue depth and insufficient depth fail closed
- Order books and execution qualification persisted and replayable

Next: partial-fill/hedge recovery simulation, latency haircuts, venue-specific fee schedules, borrow/collateral costs, and direct L2 adapters for additional perp venues.

## Milestone 4 — Broader alpha graph
- Direct exchange adapters
- CEX/CEX spot dislocations
- CEX/DEX routing
- DEX/DEX arbitrage
- Cross-chain liquidity
- Stablecoin dislocations
- Futures term structure
- Options relative value

## Milestone 5 — Live shadow trading
- Observe live quotes/books
- Generate intended orders without sending them
- Reconstruct likely fills
- Track simulated vs observed execution quality
- Require statistically meaningful evidence before any live-capital consideration

## Milestone 6 — Tiny-capital controlled execution
Separate service, separate credentials, explicit authorization, hard capital caps, dead-man switches, paired-leg atomicity/hedge recovery, venue concentration limits, kill switch.

## Milestone 7 — Machine-paid intelligence API
- API keys / usage metering
- Per-query pricing
- Bot/agent endpoints
- Optional x402-compatible payment gateway
- Never expose private positions or proprietary execution timing
