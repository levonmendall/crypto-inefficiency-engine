# Source State Model

The crypto inefficiency engine treats provider connectivity, source sufficiency, and downstream qualification as separate dimensions.

## Provider connectivity

Provider connectivity answers only whether authoritative provider evidence is currently usable.

- `healthy` — at least one fresh admitted authoritative source is usable and no observed source is currently failed/stale.
- `degraded` — a usable source exists alongside a failed/stale source, or the configured provider surface is currently failing.
- `stale` — the integration exists but the latest authoritative evidence is beyond its freshness contract.
- `missing` — no usable authoritative provider has been observed.

`provider_ready` is a connectivity fact. It is not source sufficiency and does not authorize forward trials or allocation.

## Source sufficiency

The Source Coverage Plane remains the fail-closed gate for opening forward trials. A lane is sufficient only when its required evidence classes and independent-authority redundancy target are satisfied under the existing freshness, authority, commercial-use, and point-in-time rules.

- `sufficient`
- `provider_gap` — literally no fresh admitted authoritative provider is usable.
- `evidence_class_gap` — provider evidence exists, but one or more required evidence classes are incomplete.
- `redundancy_gap` — provider evidence exists, but independent-authority redundancy is incomplete.
- `stale` — provider integration exists but authoritative evidence is stale.

The older Source Coverage Plane label `concentration_risk` is exposed as the clearer read-model term `redundancy_gap`; the underlying two-independent-authority requirement is unchanged.

## Qualification stage

Operating certification records source blockers as `waiting_for_source:<reason>`. Only a literal provider gap uses the headline operating state `provider_gap`. Evidence-class, redundancy, and stale-source blockers remain fail-closed but use the headline state `collecting` with their exact source stage preserved.

Once source sufficiency is satisfied, the existing profitability, forward-sample, statistical, execution, allocation, risk, and settlement gates operate unchanged.

## Governance invariants

This state-model repair does **not**:

- lower source-sufficiency requirements;
- reduce the independent-source redundancy target;
- fabricate candidates or forward outcomes;
- grant paper allocation authority from provider admission alone;
- add live execution capability;
- change profitability, statistical, risk, execution, or settlement thresholds.

The purpose is diagnostic precision: a working provider must not be reported as missing merely because a different source requirement is incomplete.
