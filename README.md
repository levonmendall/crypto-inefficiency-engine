# Crypto Inefficiency Engine

A **strategy-agnostic, paper-first, fail-closed** engine for continuously discovering structural crypto-market inefficiencies, reconstructing conservative net economics, learning which apparent edges survive market contact, and allocating simulated capital only when the evidence supports it.

It does **not** place live orders, hold private keys, require trading credentials, or claim live-money authority.

## Paper V1 objective

> Continuously search the accessible crypto economy for mispriced risk, determine the true conservative return after observable costs and execution risks, learn which inefficiencies are genuinely capturable, and allocate paper capital only to the best independently qualified opportunities.

The engine has no loyalty to one strategy. Funding dispersion, basis, CEX dislocations and CEX↔DEX compete through common capital-allocation constraints. Research-only opportunity families remain searchable but cannot bypass their own missing evidence gates.

## V1 architecture

`public market adapters`

→ `canonical assets / venues / instruments`

→ `strategy-neutral opportunity graph + detector registry`

→ `explicit economic costs and risk haircuts`

→ `visible-L2 / amount-specific execution evidence`

→ `capacity and current-deployment economics`

→ `multi-horizon shadow evidence`

→ `statistical qualification`

→ `strategy-neutral paper allocator`

→ `append-only history + replay + read-only API`

Cash is a valid allocation outcome.

## Paper-allocatable opportunity families

### Funding dispersion
Compares economically equivalent perpetual positions across venues and models funding, fees, collateral, liquidity, latency and hedge recovery.

### Spot/perpetual basis
Pairs exact spot and perpetual instruments and requires matched visible depth, explicit fees and current qualification.

### Dated-futures basis
Uses expiry-specific canonical futures identity, quote-currency matching and paired visible-L2 qualification.

### CEX spot dislocation
Compares same-quote spot markets. Any trade requiring a short spot leg fails closed unless borrow economics are explicitly available.

### CEX↔DEX
This family has the strongest evidence stack in V1:

- exact amount-specific DEX routes;
- $1k/$5k/$10k/$25k route frontiers;
- exact-source multi-horizon re-quotes;
- stablecoin USD/USDC/USDT conversion normalization;
- amount-specific stablecoin L2 depth;
- same-notional CEX hedge economics;
- explicit CEX fees, DEX gas and stablecoin risk haircuts;
- persisted fully costed composite-edge survival;
- independent route/frontier statistical gates;
- independent conversion-depth statistical gates;
- direct net-edge survival statistics;
- pre-funded paper inventory requirements;
- no synchronous bridge/deposit/withdrawal assumption;
- independent hedge-recovery venue and recovery reserve;
- final statistically haircutted capture edge.

Only after all required layers pass can a CEX↔DEX candidate become **paper-allocation eligible**. It still cannot authorize execution.

## Strategy-neutral paper allocation

The unified allocator compares independently qualified families using:

> **conservative expected return on reserved capital for the current deployment**

For core carry opportunities, annualized return is converted back into the return expected over the modeled holding period. For CEX↔DEX, the allocator uses the statistically haircutted one-deployment capture edge. It deliberately does **not** pretend a fast arbitrage can be continuously annualized.

The allocator enforces total capital, venue concentration, asset concentration, shared instrument/route conflicts, explicit two-leg capital reservation, and a maximum allocation count.

## Research-only families

The universal graph also searches or represents the following families, but V1 keeps them fail-closed until the missing evidence is authoritative:

- **DEX↔DEX:** pool discovery exists; independent pool-specific executable route depth does not.
- **Stablecoin dislocations:** exact conversion depth/statistics exist; a market-neutral redemption/convergence path is not yet qualified.
- **Cross-chain liquidity:** typed bridge economics exist; an authoritative amount-specific bridge quote/fill/settlement source is not connected.
- **Solver opportunities:** typed external-signal contract exists; authoritative auction/capacity/settlement evidence is not connected.
- **Liquidation/backstop:** typed external-signal contract exists; authoritative capacity/expiry/recovery evidence is not connected.
- **Option relative value:** public Deribit surface discovery exists; option L2, fees, delta hedge, vega/gamma risk and paired capacity are not yet qualified.

A research candidate is never upgraded simply because it looks profitable.

## Evidence and learning

The durable worker continuously gathers point-in-time evidence where supported:

- provider health and source timestamps;
- market/funding quotes and order books;
- executability snapshots and capacity tiers;
- core multi-horizon shadow observations;
- DEX route survival and larger-tier route evidence;
- stablecoin conversion-depth survival;
- fully reconstructed CEX↔DEX composite-edge survival;
- worker heartbeats and lineage hashes.

Statistical qualification uses independent effective samples, confidence intervals and adverse-tail evidence. Conservative fixed assumptions remain in force when evidence is insufficient.

## Safety and authority boundary

- `paper_only=true` is enforced.
- No private keys or custody.
- No deposits or withdrawals.
- No transaction building or signing.
- No live order submission.
- No live-balance assumption.
- No capacity claim from a public quote alone.
- Stale or incomplete evidence fails closed.
- Unknown required costs fail closed.
- Paper allocation never authorizes execution.

Any future tiny-capital live executor must be a **separate explicitly authorized service** with separate credentials, hard limits, paired-leg recovery, concentration controls, dead-man/kill switches and convincing production evidence.

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

## Useful read-only / paper endpoints

Core and universal discovery:
- `GET /health`
- `GET /v1/providers/diagnostic`
- `GET /v1/opportunities/live`
- `GET /v1/executability/live`
- `GET /v1/opportunities/ranked/live`
- `GET /v1/universal/graph/live`
- `GET /v1/universal/candidates/live`

DEX and conversion evidence:
- `GET /v1/dex/route-quotes/live`
- `POST /v1/dex/route-shadow/cycle`
- `GET /v1/dex/route-shadow/summary`
- `POST /v1/dex/route-frontier/probe`
- `GET /v1/dex/route-frontier/summary`
- `GET /v1/stablecoins/depth-quote?source=USDC&target=USD&amount=1000`
- `POST /v1/stablecoins/depth-shadow/cycle`
- `GET /v1/stablecoins/depth-shadow/summary`
- `GET /v1/stablecoins/depth-statistical-model?source=USDC&target=USD&amount=1000`

CEX↔DEX promotion:
- `GET /v1/cex-dex/composite/live`
- `GET /v1/cex-dex/composite-shadow/summary`
- `GET /v1/cex-dex/composite-statistical/live`
- `GET /v1/cex-dex/operational/live`
- `GET /v1/cex-dex/paper-qualification/live`
- `GET /v1/cex-dex/allocation/live`

Unified capital allocation:
- `GET /v1/allocation/unified/candidates/live`
- `GET /v1/allocation/unified/live`

Evidence / runtime:
- `GET /v1/shadow/summary`
- `GET /v1/latency/model`
- `GET /v1/worker/health`
- `GET /v1/evidence/counts`
- `GET /v1/evidence/{scan_id}/replay`

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
