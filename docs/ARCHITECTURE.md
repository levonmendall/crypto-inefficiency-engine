# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable in visible public depth, persistent, and plausibly capturable.

## Pipeline

`Public market data -> normalization -> canonical crypto opportunity graph -> registered strategy detectors -> L2 books + request timing -> economic cost model -> paired executability -> capacity frontier -> strategy-neutral opportunity ranking -> multi-horizon shadow attribution -> statistically gated empirical execution-risk model -> durable evidence database -> read-only API`

## Universal opportunity graph — v0.9 foundation

v0.9 introduces the architectural layer required to move beyond two hard-coded strategies.

### Canonical identity

Assets receive stable canonical IDs independent of provider symbol. Venue instruments receive IDs based on venue, canonical asset, market kind, and contract identity; provider-specific symbols are stored only as aliases. For the currently implemented spot and perpetual markets, the canonical contract keys are `spot` and `continuous` respectively. The identity shape is intentionally extensible to dated futures and other contract specifications later.

The graph contains canonical asset nodes, venue nodes, instrument nodes, `lists` edges, `represents` edges, and `economic_equivalence` edges connecting instruments that reference the same economic asset across fragmented markets.

### Detector registry

Discovery modules consume a shared detector context containing normalized funding observations, normalized market observations, and the canonical market graph. Existing funding-dispersion and spot/perp-basis detectors are compatibility-wrapped through the registry so their economics do not change during the architectural migration.

Every registry-produced opportunity carries detector provenance, graph version, canonical asset ID, and canonical instrument IDs. New strategy modules can therefore be graph-native without changing the downstream Opportunity/risk/execution contract.

### Common opportunity ranking

Only opportunities that have already passed economic and L2 executability qualification are eligible for the v0.9 ranking surface. Ranking currently uses the existing capital-adjusted net annualized return while reporting capacity separately. This is deliberately **not** an allocator: it does not reserve capital, construct a portfolio, or authorize execution. It is the comparable opportunity surface a future allocator will consume once several independent strategy families exist.

## Core boundaries

### Data adapters
Adapters convert venue-specific responses into canonical models. No strategy logic belongs in an adapter. Current public adapters cover Coinbase spot ticker/L2 and Hyperliquid perpetual context/predicted funding/L2. L2 requests record their own client-observed request round-trip time; this is **data-path timing**, not exchange order latency.

### Point-in-time evidence
Each persisted scan has an immutable scan ID, provider-health records, canonical quote payloads, opportunity payloads, order books, execution decisions, lineage hashes, and the exact analysis configuration used for that scan. Shadow cycles are append-only JSON payloads.

### Detectors
Detectors generate candidates; they do **not** certify economics. Final qualification happens only after explicit fee/risk/depth modeling. v0.9 routes detectors through a common registry rather than having the service know each strategy implementation directly.

### Economic cost model
Executable qualification uses explicit taker-fee assumptions, entry and exit costs, financing when applicable, modeled capital consumption, collateral opportunity cost, current book-age risk, execution-timing risk, and a hedge-recovery buffer. Unknown venue fee models and required-but-unavailable short-spot borrow costs fail closed.

The fixed model remains the fallback. When the v0.8 empirical model passes every evidence/confidence gate, observed p95 adverse selection replaces only the fixed execution-timing risk component. Current book-age risk is retained. The charged hedge-recovery buffer is the maximum of the configured floor and the empirical p95 recovery-loss proxy.

### Executability and capacity
Both legs must fill the same base quantity from point-in-time L2 while preserving additional visible hedge liquidity. The capacity frontier estimates the largest visible notional that still clears the net-return hurdle. These are taker-style visible-depth estimates; they are not exchange fill confirmations.

### Multi-horizon shadow attribution
Every initially qualified opportunity/capital-tier cohort is re-observed at the configured horizons (default 1/5/15/30/60 seconds) using shared fresh scans. The same economic signature and original target size are followed across time.

Each observation records signal survival, per-leg adverse selection, spread/depth/slippage changes, edge/cost/capacity deterioration, scan timing, L2 request timing, visible taker fill fraction, paired fill fraction, unmatched exposure, partial-fill state, hedge-recovery loss proxy, and a primary failure cause.

Provider degradation is fail-closed: a verification scan with provider failure cannot count as a surviving opportunity.

### v0.8 empirical execution-risk model
The resolver separates measured public-data latency from assumed order-acknowledgement and second-leg/hedge latency. The measured latency quantile plus those explicit execution assumptions determines the effective decision-to-hedge exposure time. Adjacent shadow horizons are interpolated conservatively when needed.

Models are resolved hierarchically for each evaluated capital size:

`strategy + venue pair + asset + capital -> strategy + venue pair + asset -> strategy + venue pair -> strategy -> global`

A scope can affect qualification only if every required horizon meets raw observation, independent-event/effective-sample, adverse-tail, recovery-tail, and confidence-width requirements. Wilson intervals are reported for fill/reserve/capture/recovery probabilities.

### Partial-fill and hedge-recovery reconstruction
Public L2 supports a conservative taker reconstruction of how much of the original target was visible on each leg. v0.8 derives paired fill fraction, maximum leg fill fraction, unmatched/unhedged fraction, partial-fill probability, and recovery-loss distributions.

This does **not** establish maker queue position. The model declares `queue_position_supported=false` and does not produce a maker-fill probability.

### API and provenance
`GET /v1/graph/live` exposes the canonical graph. `GET /v1/detectors` exposes the installed detector registry. `GET /v1/opportunities/ranked/live` exposes the strategy-neutral ranking of already-qualified opportunities. Existing v0.8 latency, shadow, executability, replay, worker and evidence endpoints remain intact.

### Execution boundary
There is no live executor. Shadow mode never sends orders. The opportunity ranking layer has no allocation or execution authority. Real-money execution remains a separate future service requiring independent authorization and risk controls.

### Durable runtime
SQLite remains the local backend. Production can provide `DATABASE_URL` or `CIE_DATABASE_URL`; the same append-only evidence ledger then uses PostgreSQL. The background worker records durable heartbeats, backs off after transient failures, and performs the multi-horizon study continuously.
