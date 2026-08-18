# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected net return after costs and risk haircuts, while preserving enough point-in-time evidence to prove whether an apparent edge was actually observable and executable.

## Pipeline

`Public market data -> normalization -> append-only evidence -> freshness validation -> strategy detectors -> cost model -> risk filters -> ranker -> depth/slippage checks -> paper ledger -> read-only API`

## Core boundaries

### Data adapters
Adapters convert venue-specific responses into canonical models. No strategy logic belongs in an adapter. Current public adapters cover Coinbase spot tickers, Hyperliquid perpetual contexts/predicted funding, and Hyperliquid L2 book snapshots.

### Point-in-time evidence
Each persisted scan has an immutable scan ID, provider-health records, canonical quote payloads, opportunity payloads, lineage hashes, and the exact analysis configuration used for that scan. Evidence is append-only; a repeated observation creates a new record rather than rewriting history.

### Replay
A stored scan can be recomputed with its original configuration. The replay result reports whether the opportunity IDs match the original scan. This is the first guard against backtest/research drift.

### Detectors
Detectors consume canonical observations and emit candidate opportunities. V1 implements:
- cross-venue funding dispersion;
- spot/perpetual basis.

### Executability
L2 snapshots are represented canonically. The execution estimator walks observed levels and computes visible VWAP, maximum notional, and slippage. If the requested paper order cannot be fully filled from observed depth it fails closed instead of extrapolating liquidity.

This is not yet a latency-aware fill model: visible L2 is necessary evidence, not proof that the liquidity would remain available by the time an order arrived.

### Cost model
Every opportunity must carry its modeled round-trip transaction cost. Gross opportunities that fail the cost hurdle are not emitted. Venue-specific fees, borrow/collateral costs, and latency haircuts remain later work.

### Provider degradation
Public-data collection is isolated by provider. One failed source is recorded as failed telemetry rather than erasing evidence from other sources. Missing evidence naturally prevents strategies that depend on it from qualifying.

### Risk gate
Rejects stale, incomplete, non-finite, negative-net, or below-threshold opportunities. Future versions add venue concentration, collateral, liquidation, transfer, settlement, and chain-risk gates.

### Execution boundary
V1 has no live executor. `PaperExecutor` records hypothetical paired fills only. A future live executor must be a separate component with explicit enablement and independent risk controls.

### API boundary
Read-only endpoints expose derived intelligence and provider status. This is intentionally shaped so a paid per-query API (including machine-payment protocols) can be layered on later without exposing proprietary internals.
