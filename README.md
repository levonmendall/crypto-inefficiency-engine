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

- add a quote-only Velora Market API adapter using `/prices` v6.2;
- probe BTC/ETH routes against USDC at a $1,000 evidence notional in both buy and sell directions;
- preserve route input/output amounts, block number, route exchanges, gas estimate when supplied and request latency;
- exclude RFQ liquidity from the route probe;
- expose raw route observations at `GET /v1/dex/route-quotes/live`;
- compare amount-specific DEX effective prices with contemporaneous CEX spot references;
- do **not** infer capacity from a successful quote;
- keep every route-quoted CEX↔DEX candidate blocked from allocation until cross-venue inventory/settlement, stablecoin conversion, quote survival and hedge recovery are qualified;
- remove the redundant universal-layer OKX fetch now that OKX is already supplied by the core adapter registry.

The quote adapter never calls transaction-building endpoints and contains no signer, wallet, allowance, approval or transaction-submission path.

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

### v0.9.2 — universal alpha graph
The universal research surface adds stablecoin conversion risk, DEX pool identity, CEX↔DEX/DEX↔DEX research, bridge capability models, liquidation/solver interfaces, Deribit option-surface research and deterministic paper allocation over already-qualified core CEX opportunities.

## Capability is not authority

The universal graph intentionally contains relationships that are searchable but not executable. DexScreener liquidity is only a discovery proxy; a Velora `/prices` response is amount-specific route evidence, not a capacity or settlement guarantee; bridge/options/solver/liquidation research likewise remains behind explicit evidence gates.

The paper allocator cannot promote a blocked or unqualified candidate. Cash/no allocation is valid and `authorizes_execution=false`.

## v0.8 — empirical execution realism — complete

Qualified core opportunities are followed at 1s, 5s, 15s, 30s and 60s. The engine records visible depth, public-data latency, slippage, adverse selection, edge/cost/capacity deterioration, partial-fill states and hedge-recovery proxies. Hierarchical calibration is gated by independent samples, tail evidence and confidence intervals. When empirical evidence is insufficient, fixed conservative assumptions remain active automatically.

## Current architecture

**Executable-evidence core:**

`public CEX adapter registry → canonical CEX graph → detector registry → conservative screening → exact visible-L2 economics → capacity → ranking → paper allocation → shadow evidence → empirical learning`

**Universal evidence surface:**

`stablecoin observations + DEX pool discovery + amount-specific DEX route quotes + chain/bridge capabilities + options + external solver/liquidation signals → universal graph → research candidates → explicit evidence gates`

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, custody, deposits, withdrawals or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees fail closed in executable qualification.
- Short spot fails closed without explicit borrow economics.
- DEX route quotes do not authorize transaction construction or execution.
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
- `GET /v1/universal/interfaces`
- `GET /v1/stablecoins/live`
- `GET /v1/dex/route-quotes/live`
- `GET /v1/allocation/live?capital_usd=100000`
- `GET /v1/latency/model`
- `POST /v1/shadow/cycle`
- `GET /v1/shadow/summary`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
