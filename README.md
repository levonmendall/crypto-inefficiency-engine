# Crypto Inefficiency Engine

A **paper-first, fail-closed** engine for discovering structural crypto-market inefficiencies, comparing them on conservative capital-adjusted economics, and learning which apparent edges survive market contact. It does not place live orders or require trading keys.

## v0.10 — Evidence maturation

### v0.10.0 — public-adapter registry and live diagnostics
- one public CEX adapter registry owns quote/funding collection, visible-L2 routing and provider→venue attribution;
- OKX is promoted into core discovery, qualification, shadow and empirical learning;
- zero-item public surfaces fail closed;
- `cie diagnose-live` and `GET /v1/providers/diagnostic` expose read-only diagnostics.

### v0.10.1 — amount-specific DEX route evidence
- quote-only Velora `/prices` v6.2 adapter for Ethereum BTC/ETH↔USDC;
- exact input/output, route composition, block, gas estimate and request latency;
- no transaction build/sign/submit path and no capacity claim.

### v0.10.2 — durable DEX route survival
- exact-source re-quotes at 1/5/15/30/60 seconds;
- durable route survival, adverse-price, route-change, gas and latency evidence;
- DEX failures cannot poison successful core CEX shadow cycles.

### v0.10.3 — multi-notional DEX route frontier
- sequential $1k/$5k/$10k/$25k quote probes by default;
- contiguous acceptable frontier with directional deterioration limits;
- larger successful quotes cannot rescue an intermediate failed/unacceptable tier;
- `capacity_claimed=false`, `executable_eligible=false`.

### v0.10.4 — explicit DEX/CEX quote-currency conversion
- USDC DEX routes can only be compared with USD/USDT CEX prices through fresh observed conversion paths;
- direct and two-hop stablecoin paths are supported;
- observed bid/ask is embedded once in the conversion rate and depeg/risk haircuts are charged separately;
- missing/stale conversion paths fail closed.

### v0.10.5 — amount-specific stablecoin conversion depth

v0.10.5 adds a stronger conversion-evidence layer using public Coinbase Exchange level-2 books for `USDC-USD` and `USDT-USD`:

- `USDC/USDT → USD` walks visible bids by the exact source-stablecoin quantity;
- `USD → USDC/USDT` walks visible asks by the exact USD input;
- `USDC ↔ USDT` executes a two-hop reconstruction through USD and feeds the **actual first-leg USD output** into the second leg;
- full visible-depth fill is mandatory; insufficient depth fails closed;
- book freshness and two-book timestamp skew are mandatory;
- effective conversion rate, best-rate reference, slippage, levels consumed, book timestamps and measured public-book latency are retained per leg;
- raw evidence is exposed at `GET /v1/stablecoins/depth-quote?source=USDC&target=USD&amount=1000`;
- depth quotes remain `visible_depth_only=true`, `capacity_claimed=false`, `executable_eligible=false` and have no allocation authority.

Top-of-book conversion edges continue to support broad universal discovery. Amount-specific depth quotes are intentionally separate until route-size evidence and conversion-depth evidence can be joined at the same notional with statistical confidence gates.

## Capability is not authority

A searchable market relationship is not executable authority. DEX route quotes, route-size frontiers and stablecoin depth reconstructions are evidence surfaces only. They do not establish atomic inventory, settlement, hedge recovery or deployable capacity, and they cannot enter the paper allocator unless explicitly promoted through later evidence gates.

## Current architecture

**Executable-evidence core:**

`public CEX adapter registry → canonical CEX graph → detector registry → visible-L2 qualification → capacity → ranking → paper allocation → multi-horizon shadow → empirical learning`

**Universal evidence surface:**

`stablecoin top-of-book graph + amount-specific stablecoin L2 depth + DEX pool discovery + amount-specific DEX routes → route survival + route-size frontiers + conversion-normalized CEX↔DEX research economics → explicit evidence gates`

## Safety boundary

- `paper_only=true` is enforced.
- No private keys, custody, deposits, withdrawals, transaction building, signing or live order placement.
- Stale/incomplete evidence fails closed.
- Unknown venue fees and required borrow fail closed.
- Cross-currency DEX comparisons fail closed without fresh observed conversion evidence.
- Stablecoin depth quotes fail closed unless the entire requested source amount fills in visible public depth.
- Successful route or conversion probes do not imply deployable capacity.
- Paper allocation has no live execution authority.

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
- `GET /v1/universal/candidates/live`
- `GET /v1/stablecoins/live`
- `GET /v1/stablecoins/depth-quote?source=USDC&target=USD&amount=1000`
- `GET /v1/dex/route-quotes/live`
- `POST /v1/dex/route-shadow/cycle`
- `GET /v1/dex/route-shadow/summary`
- `POST /v1/dex/route-frontier/probe`
- `GET /v1/dex/route-frontier/summary`
- `GET /v1/allocation/live?capital_usd=100000`
- `GET /v1/evidence/counts`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
