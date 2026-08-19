# V2 Universal Alpha Factory

V2 expands the Paper V1 Crypto Inefficiency Engine into a broader **Crypto Opportunity Engine** without weakening the paper-only authority boundary.

## Objective

Continuously search accessible crypto markets for any statistically defensible use of capital, including structural inefficiencies and predictive alpha, then compare independently qualified opportunities on conservative expected return on reserved capital.

## Design rule

**Aggressive discovery, conservative promotion.**

A research signal is not allocation authority. Predictive strategies may search broadly, but a candidate can enter the unified paper allocator only after forward evidence, current execution economics and portfolio constraints all pass.

## Universal alpha contract

Every predictive candidate exposes:

- strategy and family identity;
- asset, venue, instrument and direction;
- observation time, lookback and holding horizon;
- gross forecast, conservative costs and net forecast;
- required notional and reserved capital;
- confidence and regime classification;
- conflict keys shared with the unified allocator;
- explicit paper-only / non-execution authority flags.

This contract is strategy-neutral so future directional, reversal, on-chain factor, event, volatility, carry and market-making research can use the same promotion boundary.

## First predictive family

`time_series_momentum_v1` is the first real Alpha Factory strategy. It uses point-in-time persisted market history, volatility-aware regime classification and deliberately shrunk trailing-return forecasts.

It is intentionally simple. Its purpose is not to assert that momentum is profitable; its purpose is to create a genuine forward-testable predictive family whose authority must be earned empirically.

Positive signals prefer spot exposure. Negative signals require an observable perpetual instrument rather than assuming shortable spot inventory.

## Forward evidence

The production worker periodically runs an alpha forward-evidence cycle. The append-only alpha ledger records:

1. the original signal and its exact point-in-time candidate payload;
2. the predeclared maturity time;
3. the later observed market outcome;
4. gross directional return;
5. conservative net return after the signal-time research cost assumption;
6. whether the predicted direction was correct;
7. the regime in which the signal was generated.

No outcome rewrites or parameter updates are used to make an old signal look better.

## Statistical promotion

A candidate remains research-only unless its matching strategy / asset / direction cohort passes all configured gates:

- minimum independent forward sample threshold;
- positive confidence lower bound for mean realized net return;
- multiple-testing penalty added to the required return hurdle;
- Wilson lower confidence bound for hit rate;
- minimum regime coverage;
- positive mean performance in every required observed regime.

Even after those historical gates pass, the current candidate must still obtain fresh public L2 evidence. Current entry slippage, assumed exit slippage, taker fees and an execution-risk floor are reconstructed before promotion.

The promoted return is the lower of:

- the current forecast after fresh execution costs; and
- the forward realized-return confidence lower bound.

That conservative return must still exceed the current alpha hurdle.

## Unified allocator

Qualified alpha is a third allocation family beside:

- core CEX opportunities;
- CEX↔DEX opportunities.

All three compete on conservative expected return on reserved capital. Venue, asset, total-capital and allocation-count limits remain active. Shared venue/instrument conflict keys prevent the allocator from silently stacking incompatible uses of the same instrument.

Cash remains a valid outcome.

## API

Read-only / paper endpoints:

- `GET /v2/alpha/strategies`
- `GET /v2/alpha/evidence/summary`
- `POST /v2/alpha/evidence/cycle`
- `GET /v2/alpha/qualifications/live`
- `GET /v2/alpha/promoted/live`

The existing unified allocation endpoints automatically include only promoted alpha candidates.

## Safety boundary

V2 adds **no live executor**. It has no private keys, custody, deposit/withdrawal capability, transaction signing, live order submission or live-money authorization.

Machine learning or future automated hypothesis generation may propose research strategies, but those strategies must still pass the same forward evidence and promotion boundary before paper capital becomes eligible.

## Next alpha families

The architecture is designed to admit, without changing allocation authority:

1. reversal / mean reversion;
2. on-chain and fundamental factors;
3. option volatility and volatility-risk-premium signals;
4. event-driven signals;
5. generalized carry / yield opportunities;
6. liquidity provision / market making;
7. additional structural families as authoritative evidence becomes available.

Each family should be added as research first and promoted only after its own execution and forward-evidence requirements are explicit.
