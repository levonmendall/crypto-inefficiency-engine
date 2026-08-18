# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. v0.8 does not place live orders and does not require exchange trading keys.

## v0.8 — Empirical Fill / Latency Modeling

v0.8 replaces assumed execution timing only when measured evidence is sufficiently strong; otherwise the conservative fixed model remains active automatically.

Each multi-horizon shadow observation records original target size, scan duration, per-leg visible depth, reconstructed pair/reserve fillability, asymmetric hedge-recovery states, adverse selection, and the existing spread/slippage/edge/capacity attribution.

### v0.8.1 — hierarchical calibration

Empirical fill and adverse-selection distributions are selected per evaluated capital size using the narrowest statistically valid cohort:

**strategy + venue pair + asset + capital → strategy + venue pair + asset → strategy + venue pair → strategy → global**

Sparse scopes fall back automatically and the fallback path is preserved in model provenance.

### v0.8.2 — interval-censored latency interpolation

Measured observation-path latency no longer snaps blindly to the next 1/5/15/30/60-second checkpoint. When the selected latency quantile falls between two shadow horizons, both endpoints must independently satisfy the cohort evidence threshold before interpolation is allowed.

Interpolation is deliberately one-way conservative:

- pair-fill, reserve-fill, and capture probabilities may stay flat or deteriorate with elapsed time, but cannot improve because of noisy later samples;
- adverse-selection and hedge-recovery risk may stay flat or increase, but cannot decrease merely because the later checkpoint happened to look better;
- if either endpoint lacks enough evidence, the resolver falls back to a broader hierarchical cohort; if no scope qualifies, the fixed latency model remains active.

`GET /v1/latency/model` exposes the selected lower/upper horizons, interpolation weight, per-scope horizon counts, cohort fallback provenance, fill/capture probabilities, and adverse-selection quantiles.

## Current evidence pipeline

**Detect → qualify economics → prove L2 executability → observe 1/5/15/30/60s → reconstruct pair fillability → measure scan latency → select the narrowest valid cohort → interpolate conservatively across the measured latency interval → estimate fill/capture probability and adverse-selection risk.**

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
