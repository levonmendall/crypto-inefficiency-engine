# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives real fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. v0.7 does not place live orders and does not require exchange trading keys.

## v0.7 — Multi-Horizon Shadow Attribution

v0.7 changes the shadow layer from a single delayed re-check into an empirical capture study.

For every initially qualified opportunity and every qualified configured capital tier, the worker re-checks the same economic structure at **1s, 5s, 15s, 30s, and 60s**. Each horizon records whether the signal still exists, whether both legs remain executable, whether the net-return hurdle still clears, and how the market changed after detection.

The persisted attribution includes:

- per-leg adverse selection;
- spread, visible depth, and slippage deterioration;
- funding/basis gross-edge decay;
- modeled-cost expansion;
- executable-capacity deterioration;
- hedge-leg divergence;
- explicit primary failure causes: signal disappeared, insufficient depth, slippage expansion, fee/cost hurdle failure, stale data/provider failure, or hedge-leg divergence.

`GET /v1/shadow/summary` now derives empirical metrics from the append-only evidence ledger, including:

- median observed opportunity lifetime (reported as a lower bound because final-horizon survivors are right-censored);
- survival probability at 5s, 15s, and 30s;
- post-detection edge decay by horizon;
- conservative realistically deployable capital from surviving capacity observations;
- shortest-horizon capture-probability proxy and false-positive rate;
- survival segmented by strategy, asset, venue pair, capital size, UTC hour, and initial expected-return bucket.

The pipeline is now:

**Detect → qualify economics → prove L2 executability → observe over time → measure edge decay → estimate capture probability.**

## Existing foundations

The engine also provides public Coinbase spot and Hyperliquid perpetual adapters, funding-dispersion and spot/perp-basis detection, point-in-time append-only evidence, replay, explicit taker-fee/capital/latency/hedge-risk economics, continuous capacity-frontier estimation, SQLite/PostgreSQL persistence, an always-on worker with heartbeats/backoff, and a read-only FastAPI surface.

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, trading secrets, custody, deposits, withdrawals, or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Visible L2 depth and shadow survival are evidence, not guarantees of queue position or fills.
- v0.7 capture probability is an empirical **proxy** based on opportunity survival; v0.8 is intended to add measured fill/latency distributions.

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
- `GET /v1/worker/health`
- `GET /v1/evidence/counts`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
