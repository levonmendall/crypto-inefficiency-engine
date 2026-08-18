# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. v0.8 does not place live orders and does not require exchange trading keys.

## v0.8 — Empirical Fill / Latency Modeling

v0.8 replaces assumed execution timing only when measured evidence is sufficiently strong; otherwise the conservative fixed model remains active automatically.

Each multi-horizon shadow observation records original target size, scan duration, per-leg visible depth, reconstructed pair/reserve fillability, asymmetric hedge-recovery states, adverse selection, and the existing spread/slippage/edge/capacity attribution.

### v0.8.1 — hierarchical calibration

Empirical fill and adverse-selection distributions are no longer global-only. For each evaluated capital size the resolver tries the narrowest cohort first:

**strategy + venue pair + asset + capital → strategy + venue pair + asset → strategy + venue pair → strategy → global**

A scope is used only when it meets the configured minimum sample threshold for both reconstructed fills and adverse-selection observations. Sparse scopes fall back automatically, and the fallback path is persisted in each capital-tier qualification so broader evidence cannot masquerade as a precise cohort.

Observation-path latency remains measured globally from unique verification scans because it reflects collector/runtime performance; that measured latency is mapped to the first conservative 1/5/15/30/60-second shadow horizon. Fill, reserve-fill, capture, hedge-recovery, and adverse-selection distributions are then selected from the hierarchical cohort at that horizon.

`GET /v1/latency/model` accepts optional `strategy`, `venue_pair`, `asset`, and `notional_usd_per_leg` query parameters for inspecting the selected cohort and fallback provenance. Individual executable capital tiers expose their active latency-model scope and empirical probabilities.

## Current evidence pipeline

**Detect → qualify economics → prove L2 executability → observe 1/5/15/30/60s → reconstruct pair fillability → measure scan latency → select the narrowest valid empirical cohort → measure adverse selection → estimate fill/capture probability → calibrate latency risk.**

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
