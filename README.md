# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies and testing whether apparent edge survives fees, visible L2 depth, slippage, capital usage, hedge risk, latency, and time. It does not place live orders or require exchange trading keys.

## v0.9 — Universal Opportunity Graph

v0.9 changes the discovery architecture from two hard-wired strategies into a common crypto market graph plus detector registry.

### v0.9.0 — graph foundation

- stable canonical asset, venue, and instrument identities;
- provider symbols retained as aliases rather than primary identity;
- asset/instrument/venue graph with economic-equivalence edges;
- common detector registry and graph lineage on every opportunity;
- strategy-neutral ranking of already-qualified opportunities;
- ranking remains non-authoritative: it does not reserve capital or authorize execution.

### v0.9.1 — market breadth

Public market coverage now includes:

- **Coinbase** USD spot;
- **Kraken** USD spot via public PreTrade depth;
- **Hyperliquid** perpetual/funding data;
- **Bybit** USDT spot, linear perpetuals, and nearest dated linear futures.

The detector registry now contains four strategy families:

1. funding dispersion;
2. spot/perpetual basis;
3. dated futures basis;
4. CEX↔CEX spot dislocation.

Dated futures receive contract-specific canonical IDs, so two expiries on the same venue cannot collide. Spot/perp, futures-basis, and CEX-spot comparisons require compatible quote currencies when both sides declare them; stablecoin/FX conversion risk is not silently assumed away.

CEX↔CEX spot dislocation is intentionally **fail-closed at executable qualification** when the expensive spot leg requires a short and no borrow-cost assumption is configured. Discovery can show the raw gap without pretending inventory or borrow is free.

Provider degradation is now opportunity-scoped in shadow attribution. A Bybit failure invalidates Bybit-dependent opportunities but does not automatically turn an unrelated Coinbase/Hyperliquid opportunity into a provider failure.

## v0.8 — Empirical Fill / Latency Modeling — complete

v0.8 turns the shadow runtime into a statistically gated execution-realism model while preserving conservative fixed fallbacks whenever evidence is insufficient.

Each qualified opportunity/capital cohort is followed at 1s, 5s, 15s, 30s, and 60s. Evidence records target quantity, public L2 request timing, visible depth, spread/slippage change, adverse selection, edge decay, capacity deterioration, reconstructed fill fractions, unhedged exposure, and hedge-recovery loss proxies.

The empirical resolver uses hierarchical cohorts and Wilson confidence intervals. Empirical risk can influence qualification only after raw-sample, independent-event, tail-sample, and confidence-width gates pass. Otherwise the fixed model remains active automatically.

Public L2 supports taker visible-depth reconstruction, not maker queue position. Accordingly `queue_position_supported=false` and no maker-fill probability is invented.

## Current pipeline

**Public venue data → canonical opportunity graph → detector registry → conservative screening → L2 executable economics → capital-adjusted ranking → multi-horizon shadow attribution → statistically gated execution-risk learning → durable evidence.**

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, custody, deposits, withdrawals, or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed during executable qualification.
- Short spot fails closed without an explicit borrow-cost assumption.
- Reconstructed fills are visible-L2 taker reconstructions, not exchange-confirmed fills.
- Ranking does not allocate or authorize capital.
- Tiny-capital live execution remains separately blocked pending convincing evidence and explicit authorization.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
export CIE_EVIDENCE_DB_PATH=data/cie-evidence.sqlite3
uvicorn inefficiency_engine.api:app --reload
```

Useful endpoints include:

- `GET /health`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/detectors`
- `GET /v1/graph/live`
- `GET /v1/opportunities/ranked/live`
- `GET /v1/latency/model`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
