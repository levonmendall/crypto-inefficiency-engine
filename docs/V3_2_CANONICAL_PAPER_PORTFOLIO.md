# V3.2 Canonical Paper Portfolio

V3.2 adds one persistent, compounding paper account to the Crypto Opportunity Engine.

## Canonical account

- Portfolio id: `crypto-opportunity-engine-paper-portfolio`
- Initial capital: **$250,000**
- Paper only: yes
- Live execution authority: no
- Genesis is append-only and cannot be recreated or reset by deployment/restart.

The worker recovers the existing account from the durable evidence database. If the account has never existed, it writes exactly one genesis event for $250,000 and an initial NAV snapshot.

## Portfolio accounting

The account tracks:

- cash
- reserved capital
- open positions
- entry and current reference prices
- unrealized P&L
- realized P&L
- modeled trading costs
- NAV
- total return since $250,000 genesis
- peak NAV
- current and maximum drawdown
- closed trade count
- skipped unsupported allocation count
- P&L attribution by mechanism and strategy

Cash compounds. A supported position reserves paper cash when opened. At the strategy horizon, realized P&L and reserved capital return to cash. Future allocator cycles therefore operate against the resulting account value rather than a reset starting balance.

## Evidence integrity

Portfolio events are append-only and hash chained. The chain covers genesis, open, mark, close and skip events. Portfolio snapshots are also persisted append-only for historical NAV/performance reconstruction.

## Current settlement boundary

V3.2 deliberately opens only allocator decisions that already have sufficiently defensible forward settlement:

- predictive alpha
- directional long
- spot market
- one exact venue/symbol
- point-in-time entry reference
- known holding horizon
- precommitted round-trip cost model

The account does **not** manufacture P&L for unsupported mechanisms. Multi-leg arbitrage/carry, perpetual shorts, options, yield, maker strategies, liquidations/solvers and other mechanisms remain visible to the allocator/certification system but are recorded as portfolio skips until their realized settlement models are authoritative.

This is intentionally conservative: missing settlement fidelity is treated as missing evidence, not as a profitable paper trade.

## Worker operation

The production `cie worker` initializes/recover the canonical account on startup and advances it at the allocator-certification cadence. Each portfolio cycle:

1. collects fresh executable market evidence;
2. marks open supported positions;
3. closes positions whose predeclared holding horizon has matured;
4. returns realized P&L to cash;
5. asks the unified allocator to rank new opportunities using currently available paper cash;
6. opens only supported paper positions;
7. persists a new portfolio snapshot;
8. runs operating profitability certification against the resulting NAV.

## API visibility

- `GET /v3/portfolio/canonical` — latest canonical account
- `GET /v3/portfolio/performance` — headline performance summary
- `GET /v3/portfolio/positions` — current open positions
- `GET /v3/portfolio/trades` — closed paper trades
- `GET /v3/portfolio/history` — historical NAV/account snapshots
- `GET /v3/portfolio/attribution` — P&L by mechanism and strategy
- `POST /v3/portfolio/cycle` — manually advance one paper cycle

Existing operating-certification routes remain available under `/v3/operations/*`.

## Success interpretation

The canonical account provides the intuitive end-to-end metric that the earlier evidence layers lacked:

> If the engine had continuously managed $250,000 under its own qualified paper decisions, what would the account be worth now?

That NAV is not proof of future live profitability. It is a disciplined forward paper record whose execution assumptions remain fail-closed and progressively become more complete as settlement coverage expands.
