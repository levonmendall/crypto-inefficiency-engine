# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. v0.8 does not place live orders and does not require exchange trading keys.

## v0.8 — Empirical Fill / Latency Modeling

v0.8 begins replacing assumed execution timing with measured evidence while keeping the existing fixed model as the automatic fallback until the sample set is large enough.

Each multi-horizon shadow observation now records:

- the original target base quantity;
- measured end-to-end scan duration for the initial and verification scans;
- per-leg visible base depth and depth multiple versus the original target;
- whether both legs remained visibly fillable at the original size;
- whether both legs still preserved the configured hedge-liquidity reserve;
- whether asymmetric depth would have created a hedge-recovery state;
- per-leg adverse selection and the existing spread/slippage/edge/capacity attribution.

From those observations the engine builds an `EmpiricalLatencyModel`:

1. Deduplicate measured verification-scan latencies.
2. Take the configured latency quantile (default p95).
3. Map that measured latency to the first shadow horizon at or beyond it.
4. At that conservative horizon, estimate pair-fill probability, reserve-fill probability, capture probability, hedge-recovery probability, and the distribution of adverse price movement across the hedge pair.
5. Use p95 observed pair adverse selection as the empirical hedge-latency risk charge **only after** minimum scan and cohort sample thresholds are met.
6. Otherwise retain the prior fixed expected-hedge-latency charge automatically.

Book-age risk is not removed by the empirical model; stale current books remain explicitly charged and freshness gates remain fail-closed.

New endpoint:

- `GET /v1/latency/model` — current empirical model, evidence counts, selected latency horizon, fill/capture probabilities, adverse-selection quantiles, and whether the model is permitted to affect qualification.

`GET /v1/executability/live` and `GET /v1/shadow/summary` also expose the active latency model. Individual capital-tier qualifications record whether they used `fixed` or `empirical_shadow` latency risk.

## Current evidence pipeline

**Detect → qualify economics → prove L2 executability → observe 1/5/15/30/60s → reconstruct pair fillability → measure scan latency → measure adverse selection → estimate fill/capture probability → calibrate latency risk.**

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, trading secrets, custody, deposits, withdrawals, or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Reconstructed fills mean only that sufficient visible public L2 depth existed at the sampled instant; they do **not** claim exchange queue priority or a confirmed fill.
- Empirical latency cannot influence qualification until configured minimum sample thresholds are satisfied.

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

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
