# V2.5 Profit-Mechanism Coverage

V2.5 adds a canonical, machine-readable map of economically distinct ways the Crypto Opportunity Engine could make money.

The purpose is not to claim that every possible strategy has been enumerated. The purpose is to prevent a much more dangerous error: treating failure of the currently implemented strategies as evidence that the broader product concept has failed.

## Canonical mechanisms

The registry currently tracks 13 economic mechanisms:

1. price discrepancy / arbitrage;
2. carry / basis / funding;
3. yield / staking / lending;
4. liquidity provision / market making;
5. directional trend / momentum;
6. mean reversion / reversal;
7. on-chain / fundamental factor alpha;
8. cross-sectional / statistical relative value;
9. volatility / options risk premia;
10. event-driven alpha;
11. market microstructure / order-flow alpha;
12. liquidation / distress / solver opportunities;
13. capital-location / settlement optionality.

Every mechanism records implemented components, research-only components, missing components, blockers, priority, and the highest supported lifecycle stage.

## Coverage is deliberately multi-dimensional

A 100% taxonomy score means only that every canonical mechanism has an explicit registry entry. It does **not** mean that every mechanism has been evaluated.

The coverage layer separately reports:

- taxonomy coverage;
- decision-grade coverage;
- paper-allocation-capable coverage;
- allocator-level profitability-certifiable coverage;
- unresolved mechanism count;
- unresolved critical/high-priority mechanism count.

A mechanism is decision-grade only when authoritative point-in-time data, explicit executable economics, forward/out-of-sample evidence, and a statistical gate are all available.

Merely adding a detector, interface, research stub, or backtest never increases decision-grade coverage.

## Failure conclusion gate

The registry exposes `failure_conclusion_ready` and explicit blockers.

The intended interpretation is asymmetric:

- success can be demonstrated by one or more independent mechanisms producing durable positive forward-certified net economics after realistic costs, risk controls, and portfolio allocation;
- failure requires materially broader decision-grade coverage across the canonical mechanism taxonomy.

Therefore a poor result from funding, basis, CEX↔DEX, momentum, or reversal alone must not be interpreted as proof that the Crypto Opportunity Engine has no viable profit path while major mechanisms remain unevaluated.

## Current gap examples

The layer currently identifies gaps such as:

- generalized staking/lending/yield evidence;
- market-making fill, queue, adverse-selection, and inventory models;
- cross-sectional/statistical alpha;
- options volatility/skew/term-structure economics;
- event-driven evidence and forward cohorts;
- dedicated order-flow/lead-lag microstructure alpha;
- authoritative liquidation and solver evidence;
- dynamic capital-location optimization;
- allocator-level settlement for market-neutral carry/arbitrage families.

These gaps are explicit rather than hidden behind a generic roadmap.

## API

- `GET /v2/profit-mechanisms/coverage`
- `GET /v2/profit-mechanisms/gaps`

The endpoints are read-only, paper-only, and have no execution authority.

## Authority boundary

This layer measures research and evidence coverage. It cannot create an opportunity, qualify a strategy, override an evidence gate, allocate capital, access private balances, authorize live trading, sign a transaction, or submit an order.
