# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. The engine does not place live orders and does not require exchange trading keys.

## v0.9 — Universal Opportunity Graph foundation

v0.9 begins the transition from a funding/basis engine into a strategy-agnostic crypto inefficiency platform without weakening the v0.8 evidence boundary.

### Canonical market graph

Normalized observations are converted into a graph of:

**canonical assets ↔ venue instruments ↔ venues**

Provider symbols are aliases, not identity. Current spot and perpetual instruments receive stable canonical IDs derived from venue, canonical asset, market kind, and contract identity. Instruments representing the same economic asset are linked by explicit economic-equivalence edges so future detectors can search across fragmented markets rather than hard-code venue pairs.

`GET /v1/graph/live` exposes the current graph, provider status, and graph summary.

### Detector registry

Funding dispersion and spot/perp basis now run through a common detector registry. Their existing economics are preserved, but every discovered opportunity is enriched with detector provenance, graph version, canonical asset ID, and canonical instrument IDs.

`GET /v1/detectors` exposes the installed discovery modules and their required normalized inputs. Future futures, CEX/CEX, stablecoin, DEX, cross-chain, solver, liquidation, and options modules can plug into the same downstream Opportunity contract.

### Strategy-neutral opportunity ranking

`GET /v1/opportunities/ranked/live` ranks only opportunities that already pass the existing L2/economic qualification pipeline. The current comparator is **capital-adjusted net annualized return**, with capacity kept explicit.

This is not a capital allocator and has no execution authority. It is the common comparison surface the future allocator will consume once multiple independent strategy families exist.

## v0.8 — Empirical Fill / Latency Modeling — complete

v0.8 turns the shadow runtime into a statistically gated execution-realism model while preserving conservative fixed fallbacks whenever evidence is insufficient.

Each qualified opportunity/capital cohort is followed at 1s, 5s, 15s, 30s, and 60s. Shadow evidence records provider/data-path timing, visible depth, spread/slippage change, adverse selection, edge decay, capacity deterioration, reconstructed fill fractions, unhedged exposure, and hedge-recovery loss proxies.

Coinbase and Hyperliquid L2 requests measure their own public-data round-trip latency. Because the engine sends **no orders**, exchange order acknowledgement and second-leg hedge timing remain explicit assumptions and `execution_latency_empirical=false` remains visible.

Public L2 supports taker visible-depth reconstruction but cannot prove maker queue position. Accordingly `queue_position_supported=false` and maker-fill probability is not invented.

For each evaluated capital size, the empirical resolver tries:

**strategy + venue pair + asset + capital → strategy + venue pair + asset → strategy + venue pair → strategy → global**

A cohort can affect qualification only when every required interpolation endpoint passes raw observation, independent/effective market-event, tail-risk sample, and confidence-width gates. Capital tiers from the same market event do not inflate effective sample size.

## Evidence pipeline

**Normalize public data → build canonical market graph → run registered opportunity detectors → qualify economics → prove L2 executability → rank qualified opportunities on a common capital-adjusted basis → observe 1/5/15/30/60s → measure public-data latency → reconstruct taker fill/partial-fill states → statistically calibrate execution risk or fall back conservatively.**

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, trading secrets, custody, deposits, withdrawals, or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Reconstructed fills are visible-L2 taker reconstructions, not exchange-confirmed fills.
- Maker queue probability is not estimated from data that cannot identify queue position.
- Empirical calibration cannot influence qualification until all configured evidence/confidence gates pass.
- The v0.9 ranking layer has no allocation or execution authority.
- Tiny-capital live execution remains blocked pending convincing evidence plus separate authorization and controls.

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
- `GET /v1/detectors`
- `GET /v1/graph/live`
- `GET /v1/opportunities/demo`
- `GET /v1/opportunities/live`
- `GET /v1/opportunities/ranked/live`
- `GET /v1/executability/live`
- `GET /v1/evidence/{scan_id}/replay`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`
- `GET /v1/latency/model`
- `GET /v1/worker/health`
- `GET /v1/evidence/counts`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
