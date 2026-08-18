# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable, persistent, and plausibly capturable.

## Pipeline

`Public market data -> normalization -> strategy detectors -> L2 books -> economic cost model -> paired executability -> capacity frontier -> multi-horizon shadow attribution -> empirical capture metrics -> durable evidence database -> read-only API`

## Core boundaries

### Data adapters
Adapters convert venue-specific responses into canonical models. No strategy logic belongs in an adapter. Current public adapters cover Coinbase spot ticker/L2 and Hyperliquid perpetual context/predicted funding/L2.

### Point-in-time evidence
Each persisted scan has an immutable scan ID, provider-health records, canonical quote payloads, opportunity payloads, order books, execution decisions, lineage hashes, and the exact analysis configuration used for that scan.

### Detectors
Detectors use a cheap screening cost floor. They generate candidates; they do **not** certify economics. Final qualification happens only after explicit fee/risk/depth modeling.

### Economic cost model
Executable qualification uses explicit taker-fee assumptions, entry and exit fees, financing when applicable, modeled capital consumption, collateral opportunity cost, book-age/hedge-latency risk, and a hedge-recovery buffer. Unknown venue fee models and required-but-unavailable short-spot borrow costs fail closed.

### Executability and capacity
Both legs must fill the same base quantity from point-in-time L2 while preserving additional visible hedge liquidity. The capacity frontier estimates the largest visible notional that still clears the net-return hurdle.

### Multi-horizon shadow attribution
v0.7 starts with every initially qualified opportunity/capital-tier cohort. A shared fresh market scan is taken at each configured horizon (default 1/5/15/30/60 seconds), the same economic signature is matched, and the original notional is re-qualified.

Each observation records:

- signal persistence and return-hurdle survival;
- per-leg adverse selection from executable best prices;
- spread, visible-depth, and slippage change;
- gross funding/basis edge decay;
- modeled-cost expansion;
- executable-capacity deterioration;
- hedge-leg divergence;
- one primary failure cause while retaining the continuous diagnostics.

Provider degradation is fail-closed: a verification scan with provider failure cannot count as a surviving opportunity.

### Empirical metrics
The read-only shadow summary computes survival by horizon and by strategy, asset, venue pair, capital size, UTC hour, and initial expected return. It also reports a shortest-horizon capture-probability proxy, false-positive rate, edge decay, a right-censored lifetime lower bound, and conservative deployable-capital quantiles.

These are observational statistics, not fill probabilities. v0.8 is reserved for empirical fill/latency modeling and queue/partial-fill reconstruction.

### Execution boundary
There is no live executor. Shadow mode never sends orders. Real-money execution remains a separate future service with independent authorization and risk controls.

### Durable runtime
SQLite remains the local backend. Production can provide `DATABASE_URL` or `CIE_DATABASE_URL`; the same append-only evidence ledger then uses PostgreSQL. The background worker records durable heartbeats, backs off after transient failures, and performs the multi-horizon study continuously.
