# Architecture

## Objective

Continuously identify structural crypto-market inefficiencies and rank them by conservative expected **capital-adjusted** net return after explicit fees and risk haircuts, while preserving point-in-time evidence sufficient to test whether apparent edge was observable, executable in visible public depth, persistent, and plausibly capturable.

## Pipeline

`Public venue data -> normalization -> canonical opportunity graph -> detector registry -> economic cost model -> paired L2 executability -> capacity frontier -> common ranking -> multi-horizon shadow attribution -> statistically gated empirical execution-risk model -> durable evidence -> read-only API`

## Canonical opportunity graph

v0.9 uses stable canonical IDs for assets, venues and instruments. Provider symbols are aliases. Spot and perpetual contracts use stable contract keys; dated futures use expiry-specific contract keys so multiple expiries on one venue cannot collide.

Economic-equivalence edges connect instruments representing the same underlying asset, but edges also retain quote-currency metadata. Detectors must explicitly decide whether two instruments are comparable; the graph does not imply that USD, USDC and USDT are risk-free equivalents.

## Detector registry

Every strategy module emits the same `Opportunity` contract and then passes through shared risk, economic-cost, L2, capacity, shadow and ranking machinery. Current modules are:

- funding dispersion;
- spot/perpetual basis;
- dated futures basis;
- CEX↔CEX spot dislocation.

The last two are graph-era modules. CEX spot dislocation may discover a raw gap, but executable qualification fails closed when a short-spot leg has no configured borrow cost.

## Public venue adapters

Current public-data coverage:

- Coinbase USD spot ticker/L2;
- Kraken USD spot PreTrade depth;
- Hyperliquid perpetual context/funding/L2;
- Bybit USDT spot, linear perpetual, nearest dated linear futures, funding and L2.

Adapters contain venue parsing only, not strategy authority. L2 request round-trip time is measured as data-path timing, not exchange order latency.

## Economic cost model

Executable qualification applies explicit venue taker fees, financing/borrow where required, modeled capital consumption, collateral opportunity cost, book-age risk, execution-timing risk, slippage and hedge-recovery protection. Unknown venue fees and required-but-unavailable short-spot borrow costs fail closed.

## Executability and contract selection

Each opportunity is qualified against only the books matching its exact venue, market kind and contract identity. This prevents multiple dated futures on the same venue/asset from overwriting each other inside the validated two-leg executor. Both legs must still fill the same base quantity from visible L2 and preserve configured hedge liquidity.

## Provider degradation

Provider failure attribution is opportunity-scoped. A failed Bybit request invalidates opportunities that depend on Bybit; it does not automatically invalidate an unrelated Coinbase/Hyperliquid opportunity. Unknown/unscoped provider failures remain conservative.

## v0.8 empirical execution-risk model

The shadow runtime follows initially qualified capital tiers at 1/5/15/30/60 seconds and records signal survival, adverse selection, spread/depth/slippage changes, edge/cost/capacity deterioration, visible partial-fill states and hedge-recovery proxies.

Empirical calibration is hierarchical by strategy/venue pair/asset/capital and gated by raw observations, independent-event counts, tail samples and Wilson confidence-width limits. When evidence is insufficient, the fixed conservative model remains active. Public L2 does not establish maker queue position, so no maker-fill probability is invented.

## Ranking and allocation boundary

The common ranking surface compares already-qualified opportunities using capital-adjusted net annualized return while retaining capacity separately. Ranking has no execution or capital-allocation authority. Portfolio allocation remains a later milestone after several independent opportunity families have accumulated convincing evidence.

## Execution boundary

There is no live executor. Shadow mode sends no orders. Real-money execution remains a separate future service requiring explicit authorization, credentials, caps, concentration controls, hedge-recovery controls and kill switches.

## Durable runtime

SQLite remains the local backend. Production can use PostgreSQL through `DATABASE_URL`/`CIE_DATABASE_URL`. The background worker records durable heartbeats and append-only evidence and continuously performs the multi-horizon study.
