# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable, and persistent.

## Pipeline

`Public market data -> normalization -> strategy detectors -> L2 books -> economic cost model -> paired executability -> capacity frontier -> shadow persistence test -> durable evidence database -> read-only API`

## Core boundaries

### Data adapters
Adapters convert venue-specific responses into canonical models. No strategy logic belongs in an adapter. Current public adapters cover Coinbase spot ticker/L2 and Hyperliquid perpetual context/predicted funding/L2.

### Point-in-time evidence
Each persisted scan has an immutable scan ID, provider-health records, canonical quote payloads, opportunity payloads, order books, execution decisions, lineage hashes, and the exact analysis configuration used for that scan.

### Detectors
Detectors intentionally use a cheap screening cost floor. They generate candidates; they do **not** certify economics. Final qualification happens only after explicit fee/risk/depth modeling.

### Economic cost model
Executable qualification uses explicit taker-fee assumptions for supported venues, applies entry and exit fees, financing when applicable, modeled capital consumption, collateral opportunity cost, latency/book-age risk, and a hedge-recovery buffer. The generic detector cost is used as a floor rather than double-counted with explicit venue fees.

Unknown venue fee models and required-but-unavailable short-spot borrow costs fail closed.

### Capital denominator
Return is measured on modeled capital required across both hedge legs. Defaults assume fully funded spot and fully collateralized perps (`1.0` collateral fraction each) until a separately justified margin/liquidation model supports a less conservative denominator.

### Executability
Both legs must fill the same base quantity from point-in-time L2. The engine also requires extra visible depth according to `hedge_liquidity_reserve_ratio`. Capacity is therefore bounded below raw displayed depth.

### Latency and hedge risk
The current pre-trade model charges a deterministic risk buffer based on worst observed book age plus configured expected hedge latency, and a separate recovery buffer. These are intentionally conservative placeholders until shadow evidence supplies empirical distributions.

### Shadow observation
A shadow cycle performs a full executable scan, waits a configurable interval, performs a fresh scan, matches the same economic structure, and re-tests the original notional. Outcomes are `survived`, `signal_disappeared`, or `executability_failed`. Results are append-only and summarized as an empirical survival rate.

### Execution boundary
There is still no live executor. Shadow mode never sends orders. Real-money execution must remain a separate, explicitly authorized service with independent risk controls.

### API boundary
The API exposes derived intelligence and shadow evidence without exposing private positions or future proprietary execution timing. This surface can later support metering and machine payments.


### Durable evidence runtime
SQLite remains the zero-configuration local backend. Production can provide `DATABASE_URL` or `CIE_DATABASE_URL`; the same `EvidenceStore` then uses PostgreSQL through SQLAlchemy while preserving the existing append-only schema and lineage hashes. Database credentials are resolved outside `Settings`, so they are never copied into persisted analysis-configuration snapshots.

### Worker liveness and failure isolation
The production worker writes heartbeats before and after every shadow cycle. Transient market/provider failures are persisted as `error` heartbeats and trigger bounded backoff rather than process termination. A stale-heartbeat health check distinguishes a live-but-idle API from a dead evidence collector. SIGTERM/SIGINT set a stop event so a deployment can finish the active cycle and record a final state before exit.

### Deployment topology
The repository includes a Render Blueprint with one always-on background worker, one lightweight read-only web API, and one managed PostgreSQL database. The database blocks public IP access and is injected into both services over Render's internal connection string. The worker is the only component required for evidence collection; the web service is observational.
