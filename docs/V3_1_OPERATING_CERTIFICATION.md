# V3.1 Operating Profitability Certification

V3.1 changes the Crypto Opportunity Engine from a coverage-complete research architecture into a continuously interpreted operating evidence system.

## Objective

The worker must answer a different question for each canonical profit mechanism:

> Is progress blocked because authoritative evidence is missing, because observed economics are poor, because statistical evidence is insufficient, because current execution economics fail, because allocator settlement is incomplete, or because profitability has actually been forward certified?

Those states are deliberately distinct. A missing provider is not evidence that a mechanism is unprofitable. Equally, a provider being connected is not evidence that a mechanism works.

## Operating states

Each mechanism is classified into one of eight states:

- `provider_gap`: required authoritative point-in-time evidence is unavailable;
- `collecting`: evidence exists but the predeclared forward cohort is incomplete;
- `poor_economics`: enough real observations exist to show non-positive conservative economics;
- `statistical_failure`: returns may be positive on average but do not clear the confidence/statistical hurdle;
- `execution_blocked`: statistical evidence passes but fresh executable L2, costs, capacity, or adaptive health fail;
- `settlement_blocked`: paper allocation exists but exact allocator-level forward settlement is not supported yet;
- `certifying`: promotion works and allocator-level settlement evidence is accumulating;
- `certified`: statistically conservative allocator-level forward profitability has been demonstrated under the configured minimum cohort.

No state grants live execution authority.

## Profitability certification rule for predictive alpha

V3.1 does not reuse the V3 architecture-coverage flag as a profitability claim. A predictive family can be marked `certified` only when all of the following are true:

1. authoritative market/evidence inputs are available;
2. the independent forward sample minimum is met;
3. the forward mean-return confidence lower bound clears the configured return hurdle;
4. current candidates pass candidate-level statistical/regime qualification;
5. current candidates survive fresh L2/cost/capacity/adaptive-health promotion;
6. at least 20 supported allocator forward outcomes have settled by default;
7. allocator mean net-return confidence lower bound is positive;
8. allocator profitable-rate Wilson lower bound is at least 50% by default; and
9. aggregate realized paper profit across the certification cohort is positive.

The default allocator certification cohort is intentionally conservative and can be made stricter through future configuration. It cannot be bypassed by research code.

## Durable history

Every operating cycle writes an append-only row to `operating_certification_snapshots`. The snapshot includes:

- public market provider health;
- market/funding/L2 evidence counts;
- mechanism state and stage;
- authoritative observation counts;
- alpha signals and independent forward outcomes;
- forward mean return and confidence lower bound;
- forward hit rate and Wilson lower bound;
- current candidate, statistically qualified, and promoted counts;
- allocator settled outcome counts;
- allocator realized paper profit and confidence statistics;
- primary reason, blockers, and next required action.

This makes changes in status point-in-time auditable instead of recomputing history from current code.

## Production worker

The Render worker continues to start with `cie worker`. V3.1 routes that command through `operating_worker.run_forever`.

The existing structural shadow cycles, DEX evidence, alpha evidence, and allocation certification remain intact. At every allocation-certification interval the worker also executes an operating-certification cycle and persists the resulting snapshot. An operating-certification failure is allowed to propagate into worker health rather than being silently discarded.

## API

Read-only/paper endpoints:

- `GET /v3/operations/certification/latest`
- `GET /v3/operations/certification/history`
- `GET /v3/operations/certification/summary`
- `POST /v3/operations/certification/cycle`
- `GET /v3/operations/mechanisms`
- `GET /v3/operations/action-queue`

The action queue prioritizes provider gaps and observed poor economics before ordinary evidence accumulation. It is diagnostic only and cannot change strategy thresholds or allocate capital.

## Interpretation

The operating layer is intentionally asymmetric:

- a mechanism may fail quickly once it has enough authoritative evidence to demonstrate poor net economics;
- a mechanism may not be declared failed merely because its provider or settlement evidence is missing;
- success requires actual forward-certified allocator economics rather than architecture completeness;
- live execution remains a separate future authorization boundary.
