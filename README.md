# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies, comparing them on conservative capital-adjusted economics, and learning which apparent edges survive market contact. It does not place live orders or require trading keys.

## v0.10 — Evidence maturation

### v0.10.0 — public-adapter registry and live diagnostics

v0.10 starts by turning the broadened v0.9 graph into one coherent evidence pipeline rather than adding another strategy family.

- one `PublicAdapterRegistry` now owns public quote/funding collection, venue-specific visible-L2 routing and provider→venue attribution;
- OKX spot/perpetual/funding data is promoted from universal research-only discovery into the same core discovery, L2 qualification, shadow and empirical-learning path as the existing CEX venues;
- OKX qualification uses explicit conservative fee configuration and still fails closed on missing books, stale data or unsupported economics;
- zero-item provider responses are treated as degraded evidence rather than successful empty scans;
- `cie diagnose-live` and `GET /v1/providers/diagnostic` test public market/funding surfaces plus representative L2 books and report observed item counts, sample symbols, errors and measured book-request latency;
- the registry contains no private-account or order-entry capability.

## v0.9 — Universal Opportunity Graph — complete

v0.9 turns the project from a two-strategy engine into a strategy-agnostic crypto opportunity graph with explicit evidence boundaries.

### v0.9.0 — graph foundation

- canonical asset, venue and instrument identities;
- provider symbols retained as aliases;
- economic-equivalence graph edges;
- common detector registry and graph lineage;
- strategy-neutral ranking of already-qualified opportunities.

### v0.9.1 — CEX breadth

Core public coverage includes Coinbase USD spot, Kraken USD spot, Hyperliquid perpetual/funding data, and Bybit USDT spot/perpetual/dated futures. Core detectors cover funding dispersion, spot/perpetual basis, dated futures basis and same-quote CEX↔CEX spot dislocation.

Dated futures use contract-specific IDs. Quote currency is explicit. Short spot fails closed without borrow economics. Provider failures are scoped to opportunities that depend on the failed venue.

### v0.9.2 — universal alpha graph

The universal research surface adds:

- **Stablecoin conversion risk:** USD, USDC and USDT remain distinct currencies. Conversion edges charge observed spread plus configurable parity/depeg risk haircuts.
- **Additional CEX discovery:** public OKX spot, perpetual and funding observations feed the universal graph and detector search surface.
- **DEX identity and routing research:** chain-specific tokens and DexScreener pools become graph nodes; CEX↔DEX and DEX↔DEX price dislocations are searchable.
- **Cross-chain capability:** typed bridge quotes model fees, fill time and settlement-risk haircuts; representational bridge edges remain blocked until a fresh authoritative quote is available.
- **Liquidation/backstop and solver interfaces:** external signals have typed expiry, cost, risk and capacity requirements and cannot become eligible without authoritative capacity evidence.
- **Options relative value:** public Deribit option summaries provide volatility-surface observations; anomalies remain research-only until option L2, fee and hedge economics exist.
- **Paper capital allocator:** deterministic allocation over already-qualified core CEX opportunities only, with total-capital, venue, asset, capacity and shared-instrument constraints.

## Capability is not authority

The universal graph intentionally contains relationships that are **searchable but not executable**. Each universal candidate exposes `executable_eligible` and, when blocked, an explicit reason.

Examples:

- DexScreener reported liquidity is a discovery proxy, not a route-specific executable swap curve.
- A bridge capability edge is not a bridge quote.
- An option IV anomaly is not a delta/vega-complete executable trade.
- A solver/liquidation signal without authoritative capacity cannot enter allocation.

The paper allocator cannot promote any blocked or unqualified candidate. Cash/no allocation is a valid result and `authorizes_execution=false`.

## v0.8 — empirical execution realism — complete

Qualified core opportunities are followed at 1s, 5s, 15s, 30s and 60s. The engine records visible depth, public-data latency, slippage, adverse selection, edge/cost/capacity deterioration, partial-fill states and hedge-recovery proxies. Hierarchical calibration is gated by independent samples, tail evidence and confidence intervals. When empirical evidence is insufficient, fixed conservative assumptions remain active automatically.

Public L2 supports taker visible-depth reconstruction, not maker queue position. No maker-fill probability is invented.

## Current architecture

**Executable-evidence core:**

`public CEX adapter registry → canonical CEX graph → detector registry → conservative screening → exact visible-L2 economics → capacity → ranking → paper allocation → shadow evidence → empirical learning`

**Universal research surface:**

`CEX + stablecoins + DEX pools + chain/bridge capabilities + options + external solver/liquidation signals → universal graph → research candidates → explicit evidence gates`

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, custody, deposits, withdrawals or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed in executable qualification.
- Short spot fails closed without explicit borrow economics.
- DEX discovery proxies do not masquerade as exact swap depth.
- Universal research candidates do not bypass core L2 qualification.
- Paper allocation has no execution authority.
- Tiny-capital live execution remains separately blocked pending convincing evidence and explicit authorization.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
export CIE_EVIDENCE_DB_PATH=data/cie-evidence.sqlite3
cie diagnose-live
uvicorn inefficiency_engine.api:app --reload
```

Useful read-only/paper endpoints:

- `GET /health`
- `GET /v1/providers/diagnostic`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/opportunities/ranked/live`
- `GET /v1/graph/live`
- `GET /v1/universal/graph/live`
- `GET /v1/universal/candidates/live`
- `GET /v1/universal/interfaces`
- `GET /v1/stablecoins/live`
- `GET /v1/allocation/live?capital_usd=100000`
- `GET /v1/latency/model`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
