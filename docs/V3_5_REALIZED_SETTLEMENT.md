# V3.5 — Realized Settlement Certification

V3.5 removes the generic allocator-level settlement blocker from structural CEX opportunity families without weakening profitability standards or adding live-money authority.

## Canonical realized multi-leg settlement

Qualified two-leg `core_cex` paper allocations now preserve exact point-in-time entry-leg metadata from the executable L2 qualification that created the allocation. At the predeclared holding horizon, the shared settlement engine requires current visible L2 for every close leg, requires a full paper fill, reconstructs the realized close price from displayed depth, and records leg-level price P&L, exit slippage, levels consumed, and realized funding where applicable.

Perpetual funding is recognized only from durable point-in-time observations carrying an authoritative funding-event timestamp. The settlement engine uses a durable market mark at the event time to reconstruct the funded notional. If the required funding schedule, event-time mark, order book, or full close depth is missing, settlement remains pending. It does not fabricate a zero cost, assume a fill, or substitute expected economics for realized economics.

Precommitted non-slippage costs are carried from qualification into settlement and subtracted from realized P&L. Entry slippage is already embedded in the recorded entry fill and exit slippage is observed from the realized L2 close, preventing those components from being double counted.

The same settlement contract covers CEX structural strategies such as spot dislocation, funding dispersion, spot/perpetual basis, and dated-futures basis when the required evidence is present. CEX↔DEX remains fail-closed until amount-specific realized route and hedge settlement can be reconstructed to the same standard.

## Profitability certification

Price-discrepancy and carry mechanisms are no longer hard-coded as `settlement_blocked`. Their operating state is derived from durable allocator trials and realized outcomes:

- no eligible realized trial yet → `collecting`
- realized settlement cohort accumulating → `certifying`
- non-positive realized economics → `poor_economics`
- insufficient conservative statistical evidence → `statistical_failure`
- positive statistically conservative allocator-level profitability → `certified`

The existing minimum settled-trial, confidence, profitable-rate, and positive aggregate P&L requirements remain unchanged.

## Forward-evidence heartbeat

Predictive mechanism status now exposes collector health separately from sample progress. Every status carries:

- forward-evidence worker health
- durable persistence health
- last eligible signal timestamp
- last realized forward outcome timestamp
- last evidence-cycle timestamp
- expected collection cadence
- estimated next evidence-cycle time

The current dashboard already renders the mechanism reason, so the heartbeat is appended there. A mechanism displaying `0/30` can therefore distinguish healthy accumulation from a stalled or degraded evidence path without adding another UI surface.

## Safety boundary

V3.5 remains paper-only. It adds no private-key access, custody, deposits, withdrawals, signing, live order submission, or live execution authority. Missing evidence fails closed and profitability thresholds are not lowered.
