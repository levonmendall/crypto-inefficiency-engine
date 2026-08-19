# Architecture

## Objective

Continuously search accessible crypto markets for structural inefficiencies, reconstruct conservative expected net economics after explicit costs and risk, learn which apparent edges survive market contact, and allocate **paper capital only** to independently qualified opportunities.

The engine is strategy-agnostic. A strategy family earns capital authority through evidence; it does not receive authority merely because a detector can describe an apparent spread.

## V1 pipeline

`public adapters`

→ `normalization / canonical identity`

→ `strategy-neutral opportunity graph + detector registry`

→ `current economic cost / risk reconstruction`

→ `exact or visible-depth execution evidence`

→ `capital requirement / capacity evidence`

→ `multi-horizon shadow evidence`

→ `statistical qualification`

→ `family-specific paper promotion gate`

→ `strategy-neutral paper allocation`

→ `durable history / replay / read-only API`

## Public adapter registry

`PublicAdapterRegistry` owns public CEX market/funding collection, venue-specific public L2 routing, provider-to-venue attribution and live diagnostics. Core public surfaces currently include Coinbase, Kraken, Hyperliquid, Bybit and OKX; universal surfaces add public DEX, stablecoin and option research data.

A zero-item or failed required public surface is degraded evidence rather than a successful empty scan.

No public adapter has private-account or order-entry authority.

## Canonical graph and detector layer

Stable internal identities represent assets, venues and contracts; provider symbols remain aliases. Dated futures have expiry-specific identity. Quote currencies remain explicit, so USD, USDC and USDT are not silently treated as risk-free equivalents.

Core detector modules currently cover funding dispersion, spot/perpetual basis, dated-futures basis and CEX spot dislocation. The universal graph also represents CEX↔DEX, DEX↔DEX, stablecoin, cross-chain, solver, liquidation/backstop and option-relative-value relationships.

A universal relationship is **capability**, not authority.

## Core CEX qualification

Core opportunities pass through shared:

- explicit venue fees;
- financing and required borrow economics;
- collateral opportunity cost;
- exact matching of venue/market/contract books;
- paired visible-L2 depth and same-base sizing;
- slippage and exit-cost modeling;
- latency risk;
- hedge liquidity and recovery protection;
- capital tiers and capacity frontiers.

Unknown required fees or borrow fail closed.

## Core shadow and empirical learning

Qualified capital tiers are shadowed at multiple horizons. The system records signal survival, paired fill state, adverse selection, depth/slippage changes, cost expansion, capacity deterioration, partial-fill state and hedge-recovery proxies.

Empirical calibration is gated by independent sample counts, tail evidence and confidence intervals. Conservative fixed assumptions remain in force when statistical evidence is insufficient.

## CEX↔DEX paper-promotion pipeline

CEX↔DEX is the first universal family promoted through its own full evidence path.

### 1. Amount-specific DEX routes
Quote-only routes preserve exact source/destination amounts, route composition, block, gas and public request latency. Quotes do not imply capacity.

### 2. Route survival and size frontiers
Exact original source amounts are re-quoted at multiple horizons. Independent $1k/$5k/$10k/$25k tiers collect their own survival and adverse-deterioration evidence. A larger successful quote cannot repair an intermediate failed tier.

### 3. Stablecoin conversion depth
When CEX and DEX quote currencies differ, the exact economic amount is reconstructed through public Coinbase USDC-USD / USDT-USD L2 books. Two-hop USDC↔USDT conversion carries the actual intermediate USD amount. Insufficient depth, stale books or excessive timestamp skew fail closed.

### 4. Fully costed composite economics
Each DEX route tier is joined to its same-notional CEX hedge and required conversion-depth output. CEX taker fees, DEX gas and stablecoin risk haircuts are charged explicitly without double-counting observable conversion spread/slippage.

### 5. Independent statistical evidence
Three separate evidence layers must mature:

- route/frontier survival;
- stablecoin conversion-depth survival where conversion is required;
- fully costed composite net-edge survival.

Qualification uses independent cycles, confidence intervals, minimum effective/tail samples, adverse-deterioration ceilings and low-tail retained-edge evidence.

### 6. Paper operational qualification
The system requires explicit simulated pre-funded inventory on the correct CEX/DEX side for the route direction. A qualifying paper opportunity cannot depend on a bridge, deposit or withdrawal occurring during the opportunity. A separate CEX recovery venue and recovery reserve are required.

These are paper assumptions only; they never claim live balances.

### 7. Final conservative capture edge
The allocator does not use the raw current spread. It applies the composite survival-confidence lower bound and low-tail retained-edge fraction, then caps the result at the hedge-recovery-adjusted current edge. The resulting conservative capture edge must still exceed the configured hurdle.

Only then may CEX↔DEX become `paper_allocation_eligible=true`.

## Strategy-neutral allocator

The unified allocator compares independently qualified core CEX and CEX↔DEX opportunities on **conservative expected return on reserved capital for the current deployment**.

For core opportunities, annualized return is converted back into the return expected over the model's holding period. For event-driven CEX↔DEX, the allocator uses the conservative one-deployment capture economics. This avoids falsely annualizing a short-lived edge as though it were continuously repeatable.

Portfolio constraints include:

- total paper capital;
- venue concentration;
- asset concentration;
- shared instrument/route conflicts;
- two-leg capital reservation;
- allocation-count limits.

Cash is valid.

## Research-only families

DEX↔DEX, stablecoin dislocation, cross-chain, solver, liquidation/backstop and option relative value remain graph-searchable but cannot allocate capital until their family-specific missing evidence is authoritative.

The system intentionally prefers a named blocker over a fabricated capacity or execution assumption.

## Durable runtime

SQLite is supported locally; PostgreSQL is supported in production through `DATABASE_URL` / `CIE_DATABASE_URL`. Durable evidence includes core scans, market/funding inputs, order books, executability, shadow cycles, DEX route evidence, size frontiers, stablecoin conversion-depth shadow evidence, composite CEX↔DEX edge history and worker heartbeats.

The API exposes diagnostics, evidence, statistical models, promotion status and paper allocation surfaces.

## Authority boundary

Paper V1 has **no live executor**.

It has no private keys, custody, deposits, withdrawals, transaction building, signing, order submission or live-money authorization. Public quotes and visible depth cannot create live capacity or balance authority.

Any future live executor must be a separate explicitly authorized service with separate credentials, hard capital limits, paired-leg recovery, concentration controls, dead-man/kill switches, reconciliation and production evidence that the promoted strategy is statistically convincing.
