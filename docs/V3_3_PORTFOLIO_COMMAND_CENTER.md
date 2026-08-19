# V3.3 Portfolio Command Center

V3.3 adds a mobile-first visual command center for the canonical $250,000 compounding paper portfolio.

## Access

The dashboard is served by the existing FastAPI web service at both:

- `/`
- `/dashboard`

No separate frontend service, CDN, or JavaScript framework is required. `/docs` remains available for the raw API.

## What the dashboard shows

The command center auto-refreshes every 30 seconds and displays:

- current canonical NAV
- total return from the fixed $250,000 genesis
- cash and deployed/reserved capital
- realized and unrealized P&L
- maximum drawdown
- open-position and completed-trade counts
- NAV/equity curve from durable portfolio snapshots
- current open positions and their marks
- recent realized paper trades
- skipped/rejected allocator decisions and their fail-closed reasons
- P&L attribution by profit mechanism and strategy
- mechanism-level operating/certification state
- current action queue for unresolved provider, evidence, economics, execution, settlement, or certification blockers

## Execution status

The interface intentionally makes the authority boundary explicit:

- automatic paper execution: enabled through the production worker
- live-money execution: disabled
- private balances/custody: unavailable
- transaction/order signing: unavailable

The dashboard does not create allocation authority. It only visualizes the same canonical durable state used by the worker and APIs.

## Data sources

The browser reads same-origin endpoints from the existing service:

- `/v3/portfolio/canonical`
- `/v3/portfolio/performance`
- `/v3/portfolio/positions`
- `/v3/portfolio/trades`
- `/v3/portfolio/skips`
- `/v3/portfolio/history`
- `/v3/portfolio/attribution`
- `/v3/operations/mechanisms`
- `/v3/operations/action-queue`

The skipped-allocation endpoint uses a bounded database query rather than replaying the entire event ledger, so the dashboard remains efficient as portfolio history grows.

## Design principles

1. The $250,000 canonical paper ledger remains the source of truth.
2. A visual screen cannot alter or manufacture P&L.
3. Unsupported settlement paths continue to be recorded as skips rather than fictional trades.
4. The command center is optimized for phone-sized screens as well as desktop browsers.
5. The dashboard uses no external assets or third-party runtime dependencies.
