# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies, comparing them on conservative capital-adjusted economics, and learning which apparent edges survive market contact. It does not place live orders or require trading keys.

## v0.10 — Evidence maturation

### v0.10.0 — public-adapter registry and live diagnostics

- one `PublicAdapterRegistry` owns public CEX quote/funding collection, visible-L2 routing and provider→venue attribution;
- OKX spot/perpetual/funding is promoted into the core discovery, L2 qualification, shadow and empirical-learning path;
- explicit OKX fees are required by executable economics;
- zero-item provider responses are degraded evidence, not successful empty scans;
- `cie diagnose-live` and `GET /v1/providers/diagnostic` test public surfaces and representative L2 without order authority.

### v0.10.1 — amount-specific DEX route evidence

- quote-only Velora Market API `/prices` v6.2 adapter for Ethereum BTC/ETH↔USDC;
- BTC/ETH buy and sell probes at a $1,000 evidence notional;
- exact source/destination amounts, block number, route exchanges, gas estimate when supplied and request latency are retained;
- RFQ liquidity is excluded;
- a successful quote is **not** treated as capacity evidence;
- route-quoted CEX↔DEX candidates remain blocked from allocation and execution.

### v0.10.2 — durable DEX route survival

v0.10.2 turns one-time DEX route observations into longitudinal evidence:

- re-quote the exact original source amount at 1/5/15/30/60-second horizons;
- record whether the quote survives, directionally adverse price movement, route-composition change, block advance, gas-cost change and request latency;
- persist every successful initial/verification route and each route-shadow cycle in the same append-only SQLite/PostgreSQL evidence ledger as the CEX study;
- expose `POST /v1/dex/route-shadow/cycle` and `GET /v1/dex/route-shadow/summary`;
- production worker runs CEX shadow and DEX route shadow concurrently;
- DEX provider failure is scoped to DEX evidence and cannot invalidate a successful core CEX shadow cycle;
- route-shadow evidence still claims no capacity and builds/submits no transaction.

The next promotion gate is statistical evidence: repeated quote survival and adverse-price behavior must be credible before a multi-notional capacity frontier or any allocation eligibility is considered.

## v0.9 — Universal Opportunity Graph — complete

v0.9 turns the project from a two-strategy engine into a strategy-agnostic crypto opportunity graph with explicit evidence boundaries.

- **v0.9.0:** canonical asset/venue/instrument graph, detector registry and common ranking.
- **v0.9.1:** Bybit/Kraken breadth, dated-futures basis and CEX↔CEX spot dislocation.
- **v0.9.2:** stablecoin conversion risk, DEX pool identity, bridge/solver/liquidation interfaces, Deribit option research and deterministic paper allocation over already-qualified core CEX opportunities.

## Capability is not authority

The universal graph intentionally contains relationships that are searchable but not executable. A DexScreener pool is discovery metadata; a Velora price route is amount-specific quote evidence; neither establishes inventory, settlement, recoverability or capacity. Bridge/options/solver/liquidation research likewise remains behind explicit evidence gates.

The paper allocator cannot promote a blocked or unqualified candidate. Cash/no allocation is valid and `authorizes_execution=false`.

## v0.8 — empirical execution realism — complete

Qualified core opportunities are followed at 1s, 5s, 15s, 30s and 60s. The engine records visible depth, public-data latency, slippage, adverse selection, edge/cost/capacity deterioration, partial-fill states and hedge-recovery proxies. Hierarchical calibration is gated by independent samples, tail evidence and confidence intervals. When empirical evidence is insufficient, fixed conservative assumptions remain active automatically.

## Current architecture

**Executable-evidence core:**

`public CEX adapter registry → canonical CEX graph → detector registry → conservative screening → exact visible-L2 economics → capacity → ranking → paper allocation → multi-horizon shadow evidence → empirical learning`

**Universal evidence surface:**

`stablecoin observations + DEX pool discovery + amount-specific DEX route quotes → route-survival evidence + chain/bridge capabilities + options + external solver/liquidation signals → universal graph → research candidates → explicit evidence gates`

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, custody, deposits, withdrawals or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed in executable qualification.
- Short spot fails closed without explicit borrow economics.
- DEX route collection calls price quoting only; it does not build, sign or submit transactions.
- Successful route probes do not imply capacity.
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

Useful read-only/paper endpoints include:

- `GET /health`
- `GET /v1/providers/diagnostic`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/opportunities/ranked/live`
- `GET /v1/graph/live`
- `GET /v1/universal/graph/live`
- `GET /v1/universal/candidates/live`
- `GET /v1/stablecoins/live`
- `GET /v1/dex/route-quotes/live`
- `POST /v1/dex/route-shadow/cycle`
- `GET /v1/dex/route-shadow/summary`
- `GET /v1/allocation/live?capital_usd=100000`
- `GET /v1/evidence/counts`
- `GET /v1/latency/model`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
