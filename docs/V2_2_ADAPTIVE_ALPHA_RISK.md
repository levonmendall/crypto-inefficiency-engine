# V2.2 Adaptive Alpha Risk

V2.2 adds a deliberately subtractive control layer between predictive-alpha promotion and the unified paper allocator.

The V2 statistical promotion gate answers whether a strategy/asset/direction cohort has accumulated enough independent forward evidence to earn paper eligibility. That long-run evidence can become stale. V2.2 therefore adds a second question immediately before allocation:

> Does the edge still look healthy in its most recent independent outcomes, and how much paper capital should it receive now?

## Health evidence

For each candidate the controller evaluates only the same de-overlapped forward outcomes used by the promotion framework. It measures:

- recent mean realized net return;
- recent hit rate;
- median realized/predicted return capture ratio;
- recent return relative to the long-run forward mean;
- compounded forward-outcome drawdown;
- trailing consecutive losses;
- forecast calibration mean absolute error.

## Fail-closed degradation gates

A previously qualified alpha candidate receives no paper capital when recent evidence is insufficient or when any configured degradation gate fails. The initial conservative defaults block on:

- fewer than eight recent independent outcomes;
- non-qualifying recent mean net return;
- median forecast capture below 35%;
- recent performance below 35% of the long-run forward mean;
- more than 6% compounded forward-outcome drawdown;
- four or more trailing losses.

These gates never create authority. A candidate must first pass the existing forward statistical qualification and current L2 execution-cost gate.

## Confidence-scaled paper capital

Healthy candidates receive a capital multiplier derived from recent hit rate, forecast capture, recent-vs-long-run performance and drawdown headroom. The multiplier is bounded between a conservative floor and 1.0. Notional, capital reservation and expected paper profit are reduced consistently before the candidate reaches the unified allocator.

This avoids the previous binary behavior in which every statistically qualified alpha candidate entered allocation at the same research capital fraction regardless of how convincingly its edge was currently behaving.

## Observability

`GET /v2/alpha/health/live` returns the current candidate, long-run qualification and adaptive health state together. The endpoint is read-only and has no allocation or execution authority.

## Authority boundary

V2.2 remains paper-only. The adaptive health controller can only reduce or revoke paper eligibility. It cannot override statistical qualification, bypass live L2 economics, submit orders, access private balances, or authorize real money.
