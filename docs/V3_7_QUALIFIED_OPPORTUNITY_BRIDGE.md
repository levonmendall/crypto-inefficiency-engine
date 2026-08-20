# v3.7 Universal Qualified-Opportunity Bridge

## Objective

Make the canonical $250,000 paper portfolio capable of expressing every opportunity family for which the engine already has both current qualification authority and an honest settlement contract, without moving heavyweight research back into the portfolio accounting hot path.

## Architecture

The runtime now has three explicit boundaries:

1. **Research and certification** continue to search the broad crypto opportunity surface and accumulate forward evidence.
2. **Qualified-opportunity bridge** projects only fresh, paper-allocation-eligible candidates with a canonical settlement contract into an append-only expiring decision envelope.
3. **Canonical portfolio accounting** reads that envelope, reapplies portfolio risk, concentration and conflict constraints, and either opens a supported paper position or keeps cash.

The bridge publisher runs from the research worker immediately after the completed core shadow cycle. It reuses the latest persisted executable scan rather than launching another broad scan. Structural CEX candidates reuse the scan's existing executability. Alpha candidates must already clear forward statistical qualification, current cost/L2 qualification and adaptive health; any candidate-level L2 fallback remains bounded inside the research worker.

## Settlement capability

The canonical portfolio now recognizes the settlement contracts already implemented by allocation certification:

- spot directional-long alpha;
- perpetual directional-short alpha with observed funding accrual;
- two-leg market-neutral core CEX opportunities, including price-discrepancy and carry/basis/funding families, with visible-L2 close reconstruction and observed funding where required.

CEX↔DEX and later research families remain excluded from canonical portfolio authority until they have their own complete amount-specific settlement contract. Their research/certification coverage is unchanged.

## Freshness and fail-closed behavior

Every qualified-opportunity snapshot carries its source scan, observation time and expiration time. The canonical allocator never reruns research and refuses to use a missing or expired bridge snapshot.

A fresh empty bridge snapshot is different from a failed bridge:

- **fresh + zero candidates** means research completed and cash is the correct portfolio result under current qualification rules;
- **missing/stale/error** is recorded as a bridge failure and the portfolio remains in cash.

No return hurdle, statistical threshold, risk budget, cost assumption or live-execution boundary is weakened.

## Point-in-time portfolio integrity

Generalized short and multi-leg positions persist the exact settlement trial inside the append-only canonical portfolio event. Valuation uses the earliest actual quote timestamp across required legs. Mature positions settle only from evidence observed at or after the committed horizon.

Multi-leg close L2 is fetched only for positions that actually mature, and each request is bounded by the public adapter timeout. Missing funding, market, L2 or post-horizon evidence leaves the position unsettled and blocks new allocation rather than inventing P&L.

## Result

The expensive search path remains isolated from accounting, but the portfolio is no longer limited to long spot alpha. Every currently qualified and settlement-supported use of capital can compete under one portfolio risk budget. Cash remains valid only when the fresh bridge contains no deployable candidate or when required evidence fails closed.

The system remains paper-only and has no live order, custody, signing, withdrawal or live-money authority.
