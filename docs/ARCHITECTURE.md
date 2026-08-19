# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit costs and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable in visible public depth, persistent and plausibly capturable.

## Pipeline

`public adapter registry -> normalization -> canonical opportunity graph -> detector registry -> economic cost model -> paired visible-L2 executability -> capacity frontier -> common ranking -> paper allocation -> multi-horizon shadow attribution -> statistically gated empirical execution-risk model -> durable evidence -> read-only API`

## Public adapter registry

v0.10 centralizes public CEX collection in `PublicAdapterRegistry`. It owns:

- market/funding collection and provider status;
- venue-specific public L2 routing;
- provider-prefix to venue attribution used by shadow failure classification;
- live provider diagnostics.

`OpportunityService` no longer needs venue-specific collection/book `if/elif` branches. Adding a public venue therefore does not require a second, disconnected routing path.

Current core public surfaces are Coinbase USD spot, Kraken USD spot, Hyperliquid perpetual/funding, Bybit USDT spot/perpetual/dated futures, and OKX USDT spot/perpetual/funding. A zero-item surface is degraded evidence, not a healthy empty scan.

The diagnostic path requests representative visible L2 and reports item counts, symbols, errors and observed request latency. It has no private-account or order-entry capability.

## Canonical opportunity graph

Stable canonical IDs represent assets, venues and instruments. Provider symbols are aliases. Spot/perpetual contracts use stable contract keys; dated futures use expiry-specific keys. Economic-equivalence edges retain quote currency rather than assuming USD, USDC and USDT are risk-free equivalents.

## Detector and universal layers

Core CEX detector modules emit the common `Opportunity` contract and pass through shared risk, economic-cost, L2, capacity, shadow and ranking machinery. Universal v0.9 models stablecoin, DEX, bridge, option, solver and liquidation relationships behind explicit evidence gates.

Capability is not authority: research candidates cannot bypass core executable qualification or enter paper allocation unless their own evidence path is explicitly promoted.

## Economic cost model

Executable qualification applies explicit venue taker fees, financing/borrow where required, modeled capital consumption, collateral opportunity cost, book-age risk, execution-timing risk, slippage and hedge-recovery protection. Unknown fees and required-but-unavailable borrow fail closed. OKX now uses explicit configured spot and derivative fee assumptions before it can qualify.

## Executability and evidence

Each opportunity is qualified only against books matching exact venue, market kind and contract identity. Both legs must fill the same base quantity from visible public depth while preserving configured hedge liquidity. Public L2 is a taker visible-depth reconstruction, not proof of exchange-confirmed fills or maker queue position.

The shadow runtime follows qualified capital tiers at 1/5/15/30/60 seconds and records signal survival, adverse selection, depth/slippage change, edge/cost/capacity deterioration, partial-fill state and hedge-recovery proxies. Empirical calibration remains gated by independent samples, tail evidence and confidence intervals; fixed conservative assumptions remain when evidence is insufficient.

## Allocation and execution boundary

Paper allocation consumes already-qualified opportunities and enforces total-capital, venue, asset, capacity and shared-instrument conflicts. Cash is valid. Allocation has no live execution authority.

There is no live executor. Real-money execution remains a separate future service requiring explicit authorization, credentials, hard caps, hedge-recovery controls and kill switches.

## Durable runtime

SQLite remains the local backend. Production can use PostgreSQL through `DATABASE_URL`/`CIE_DATABASE_URL`. The worker records append-only evidence and durable heartbeats; the read-only API exposes diagnostics, evidence and shadow summaries.
