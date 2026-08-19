# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. v0.8 does not place live orders and does not require exchange trading keys.

## v0.8 — Empirical Fill / Latency Modeling — complete

v0.8 turns the shadow runtime into a statistically gated execution-realism model while preserving conservative fixed fallbacks whenever evidence is insufficient.

### What is measured

Each qualified opportunity/capital cohort is followed at 1s, 5s, 15s, 30s, and 60s. Shadow evidence records the original target quantity, provider/data-path timing, per-leg visible depth, spread/slippage change, adverse selection, edge decay, capacity deterioration, reconstructed fill fractions, unhedged exposure, and hedge-recovery loss proxies.

Coinbase and Hyperliquid L2 requests now measure their own public-data round-trip latency. Historical v0.8 observations without that field may use whole-scan duration only as an explicitly labeled fallback.

### What remains assumed

Because v0.8 sends **no orders**, exchange order acknowledgement and second-leg hedge timing cannot be measured empirically. They remain explicit configuration assumptions and are added to measured collector/data latency when selecting the relevant shadow interval. The model exposes `execution_latency_empirical=false` so this distinction cannot be hidden.

Public L2 can support taker visible-depth reconstruction. It cannot prove maker queue position. Accordingly:

- `fill_model_kind=visible_l2_taker_reconstruction`;
- `queue_position_supported=false`;
- `maker_fill_probability=null`.

### Statistical calibration

For each evaluated capital size, the resolver tries the narrowest cohort first:

**strategy + venue pair + asset + capital → strategy + venue pair + asset → strategy + venue pair → strategy → global**

A cohort can affect qualification only when every required interpolation endpoint passes all configured gates:

- minimum raw observations;
- minimum independent/effective market events;
- enough adverse-selection and hedge-recovery-loss tail observations;
- Wilson confidence intervals within the configured maximum width.

Capital tiers from the same detected market event are clustered together and do not inflate effective sample size.

When effective decision-to-hedge timing lies between two shadow horizons, v0.8 interpolates conservatively: fill/capture quality cannot improve with elapsed time and adverse/recovery risk cannot decrease because of noisy later samples.

### Partial fills and hedge recovery

The engine reconstructs visible taker fill fraction for each leg, paired fill fraction, asymmetric unmatched exposure, partial-fill probability, and p50/p90/p95 unhedged-fraction and hedge-recovery-loss distributions. Empirical recovery evidence may **increase** the charged recovery buffer but never reduce the configured fixed floor.

Likewise, empirical p95 adverse selection can replace the fixed execution-latency risk charge only after the statistical gate passes. Current book-age risk remains separate and always applies.

## Evidence pipeline

**Detect → qualify economics → prove L2 executability → observe 1/5/15/30/60s → measure public-data latency → reconstruct taker fill/partial-fill states → cluster independent events → select the narrowest statistically valid cohort → interpolate at measured+assumed decision-to-hedge timing → estimate confidence-bounded capture/adverse/recovery risk → qualify or fall back conservatively.**

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, trading secrets, custody, deposits, withdrawals, or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Reconstructed fills are visible-L2 taker reconstructions, not exchange-confirmed fills.
- Maker queue probability is not estimated from data that cannot identify queue position.
- Empirical calibration cannot influence qualification until all configured evidence/confidence gates pass.
- Tiny-capital live execution remains outside v0.8 and blocked pending convincing shadow evidence plus separate authorization and controls.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
export CIE_EVIDENCE_DB_PATH=data/cie-evidence.sqlite3
uvicorn inefficiency_engine.api:app --reload
```

Useful commands:

```bash
cie demo
cie live
cie executability
cie shadow-once
cie shadow-loop
cie worker
cie worker-health
```

Endpoints:

- `GET /health`
- `GET /v1/opportunities/demo`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/evidence/{scan_id}/replay`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`
- `GET /v1/latency/model`
- `GET /v1/worker/health`
- `GET /v1/evidence/counts`

`GET /v1/latency/model` supports scoped inspection by strategy, venue pair, asset, and notional and exposes latency provenance, effective sample size, confidence intervals, interpolation endpoints, fill/partial-fill/recovery distributions, and whether the model is allowed to influence qualification.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
