# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable in visible public depth, persistent, and plausibly capturable.

## Pipeline

`Public market data -> normalization -> strategy detectors -> L2 books + request timing -> economic cost model -> paired executability -> capacity frontier -> multi-horizon shadow attribution -> statistically gated empirical execution-risk model -> durable evidence database -> read-only API`

## Core boundaries

### Data adapters
Adapters convert venue-specific responses into canonical models. No strategy logic belongs in an adapter. Current public adapters cover Coinbase spot ticker/L2 and Hyperliquid perpetual context/predicted funding/L2. L2 requests record their own client-observed request round-trip time; this is **data-path timing**, not exchange order latency.

### Point-in-time evidence
Each persisted scan has an immutable scan ID, provider-health records, canonical quote payloads, opportunity payloads, order books, execution decisions, lineage hashes, and the exact analysis configuration used for that scan. Shadow cycles are append-only JSON payloads, so v0.8 attribution fields remain point-in-time without a destructive schema migration.

### Detectors
Detectors use a cheap screening cost floor. They generate candidates; they do **not** certify economics. Final qualification happens only after explicit fee/risk/depth modeling.

### Economic cost model
Executable qualification uses explicit taker-fee assumptions, entry and exit costs, financing when applicable, modeled capital consumption, collateral opportunity cost, current book-age risk, execution-timing risk, and a hedge-recovery buffer. Unknown venue fee models and required-but-unavailable short-spot borrow costs fail closed.

The fixed model remains the fallback. When the v0.8 empirical model passes every evidence/confidence gate, observed p95 adverse selection replaces only the fixed execution-timing risk component. Current book-age risk is retained. The charged hedge-recovery buffer is the maximum of the configured floor and the empirical p95 recovery-loss proxy, so empirical evidence cannot make recovery protection less conservative.

### Executability and capacity
Both legs must fill the same base quantity from point-in-time L2 while preserving additional visible hedge liquidity. The capacity frontier estimates the largest visible notional that still clears the net-return hurdle. These are taker-style visible-depth estimates; they are not exchange fill confirmations.

### Multi-horizon shadow attribution
Every initially qualified opportunity/capital-tier cohort is re-observed at the configured horizons (default 1/5/15/30/60 seconds) using shared fresh scans. The same economic signature and original target size are followed across time.

Each observation records signal survival, per-leg adverse selection, spread/depth/slippage changes, edge/cost/capacity deterioration, scan timing, L2 request timing, visible taker fill fraction, paired fill fraction, unmatched exposure, partial-fill state, hedge-recovery loss proxy, and a primary failure cause.

Provider degradation is fail-closed: a verification scan with provider failure cannot count as a surviving opportunity.

### v0.8 empirical execution-risk model
The resolver separates three timing concepts:

1. **Measured public-data latency** — client-observed L2 request round-trip time. Historical evidence without this field can use whole-scan duration only as an explicitly labeled fallback.
2. **Assumed order acknowledgement latency** — hypothetical while the product sends no orders.
3. **Assumed second-leg/hedge latency** — hypothetical while the product sends no paired orders.

The measured latency quantile plus the two explicit execution assumptions determines the effective decision-to-hedge exposure time. Adjacent shadow horizons are interpolated conservatively when needed.

Models are resolved hierarchically for each evaluated capital size:

`strategy + venue pair + asset + capital -> strategy + venue pair + asset -> strategy + venue pair -> strategy -> global`

A scope can affect qualification only if every required horizon meets raw observation, independent-event/effective-sample, adverse-tail, recovery-tail, and confidence-width requirements. Capital tiers sharing one initial scan and economic opportunity signature are clustered as one event when broader cohorts pool notionals.

Wilson intervals are reported for fill/reserve/capture/recovery probabilities. Probability quality cannot improve with elapsed time due to noisy samples; adverse-selection, unmatched-exposure, and recovery risk cannot decrease due to noisy later samples.

### Partial-fill and hedge-recovery reconstruction
Public L2 supports a conservative taker reconstruction of how much of the original target was visible on each leg. v0.8 derives paired fill fraction, maximum leg fill fraction, unmatched/unhedged fraction, partial-fill probability, and recovery-loss distributions from adverse price movement and incremental slippage applied to the unmatched fraction.

This does **not** establish maker queue position. The model therefore declares `queue_position_supported=false` and does not produce a maker-fill probability. That abstention is part of the evidence contract, not a missing implicit assumption.

### API and provenance
`GET /v1/latency/model` exposes model scope, fallback path, data-latency source, measured and assumed timing components, interpolation endpoints, raw/effective sample counts, confidence intervals, fill/partial-fill/recovery distributions, queue-position capability, and whether the empirical model is permitted to influence qualification. Capital-tier qualification persists the relevant model provenance.

### Execution boundary
There is no live executor. Shadow mode never sends orders. `execution_latency_empirical=false` remains explicit. Real-money execution is a separate future service requiring independent authorization and risk controls.

### Durable runtime
SQLite remains the local backend. Production can provide `DATABASE_URL` or `CIE_DATABASE_URL`; the same append-only evidence ledger then uses PostgreSQL. The background worker records durable heartbeats, backs off after transient failures, and performs the multi-horizon study continuously.
