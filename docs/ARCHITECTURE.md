# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable, persistent and plausibly capturable.

## Two-layer v0.9 architecture

`Public data -> canonical CEX graph -> detector registry -> L2/economic qualification -> ranking -> paper allocation -> shadow evidence`

and, in parallel:

`CEX + stablecoin + DEX + chain/bridge + option + external solver/liquidation observations -> universal graph -> research candidates -> explicit capability/evidence gates`

The distinction is deliberate. A market relationship may be searchable before it is allowed to influence capital.

## Core executable graph

Stable canonical IDs exist for assets, venues and instruments. Provider symbols are aliases. Dated futures use expiry-specific contract keys. Quote currencies remain explicit. Current executable-capable strategy families are funding dispersion, spot/perpetual basis, dated futures basis, and CEX↔CEX spot dislocation, subject to the existing fee/borrow/L2 gates.

## Universal graph

v0.9.2 adds graph-native representations for:

- USD, USDC and USDT conversion edges with spread and depeg-risk haircuts;
- chain-specific token identities and DEX pool nodes;
- CEX↔DEX and DEX↔DEX research paths;
- cross-chain bridge capability edges and a typed bridge quote/settlement-risk model;
- liquidation/backstop and solver signal interfaces;
- Deribit option nodes and volatility-surface research candidates.

Universal candidates carry an explicit `executable_eligible` flag and blocked reason. Research data cannot silently become executable evidence.

## Public venue/data adapters

Core/public CEX coverage includes Coinbase, Kraken, Hyperliquid and Bybit. v0.9.2 also adds an OKX public spot/perpetual/funding adapter to the universal discovery surface.

Additional public research surfaces:

- Coinbase stablecoin USD tickers for conversion/depeg evidence;
- DexScreener token-pair discovery for pool identity, price and reported liquidity;
- Deribit public option summaries for option-surface observations.

DexScreener reported liquidity is not treated as an exact route-specific swap curve. Deribit option summaries are not treated as a delta/vega-complete executable trade.

## Stablecoin conversion risk

USD, USDC and USDT are separate canonical currencies. Conversion edges charge observed spread plus a configurable haircut that increases with parity deviation. Cross-quote relationships therefore require an explicit conversion path rather than a hidden 1:1 assumption.

## DEX and cross-chain boundary

DEX pool observations can produce research candidates, but exact execution remains blocked until a route-specific quote/depth model can prove slippage at the intended size. Bridge capability edges are representational until a fresh authoritative quote supplies fees, expected settlement time and settlement-risk evidence.

## Liquidation / solver boundary

Typed external signals require provider, expiry, modeled costs, risk haircut and capacity. They cannot become execution-eligible unless capacity is authoritative and the signal explicitly satisfies the execution-evidence contract.

## Options boundary

The public Deribit surface can identify relative implied-volatility anomalies. Options remain outside executable ranking until the engine has option L2, venue fee modeling, delta/vega/gamma hedge economics and paired capacity.

## Paper allocator

v0.9.2 adds a deterministic, non-authoritative paper allocator over **already qualified core CEX opportunities only**. It cannot promote an unqualified opportunity. It enforces:

- total capital;
- per-venue concentration;
- per-asset concentration;
- exact qualified capital tier/capacity;
- shared canonical-instrument conflicts;
- a maximum number of allocations.

Unused cash is a valid result. `authorizes_execution=false` is permanent for this layer.

## v0.8 empirical execution-risk model

The shadow runtime follows qualified capital tiers at 1/5/15/30/60 seconds and records signal survival, adverse selection, spread/depth/slippage changes, edge/cost/capacity deterioration, visible partial-fill states and hedge-recovery proxies. Empirical calibration remains hierarchical and confidence-gated. When evidence is insufficient, fixed conservative assumptions remain active.

## Execution boundary

There is no live executor. Shadow mode sends no orders. Real-money execution remains a separate future service requiring explicit authorization, credentials, caps, concentration controls, hedge-recovery controls and kill switches.

## Durable runtime

SQLite remains the local backend. Production can use PostgreSQL through `DATABASE_URL`/`CIE_DATABASE_URL`. The background worker records append-only evidence and durable heartbeats.
