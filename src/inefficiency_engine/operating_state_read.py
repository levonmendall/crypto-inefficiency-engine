from __future__ import annotations

from collections import defaultdict
from typing import Any


VALID_STATES = {
    "provider_gap",
    "collecting",
    "poor_economics",
    "statistical_failure",
    "execution_blocked",
    "settlement_blocked",
    "certifying",
    "certified",
}

# A lane remains economically alive when at least one underlying strategy is still
# progressing. This mirrors the existing strategy-attribution presentation rule and
# prevents one failed strategy from vetoing an otherwise viable mechanism family.
STATE_PRIORITY = {
    "certified": 0,
    "certifying": 1,
    "collecting": 2,
    "execution_blocked": 3,
    "settlement_blocked": 4,
    "statistical_failure": 5,
    "poor_economics": 6,
    "provider_gap": 7,
}
_SOURCE_WAIT_PREFIX = "waiting_for_source:"


def _number(value: object | None) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object | None) -> int:
    parsed = _number(value)
    return max(0, int(parsed or 0))


def _source_wait_state(row: dict[str, Any]) -> str | None:
    stage = str(row.get("stage") or "")
    if not stage.startswith(_SOURCE_WAIT_PREFIX):
        return None
    value = stage[len(_SOURCE_WAIT_PREFIX):].strip()
    return value or None


def _mixed_strategy_state(rows: list[dict[str, Any]]) -> str | None:
    states = {
        str(row.get("state") or "")
        for row in rows
        if str(row.get("state") or "") in VALID_STATES
    }
    if not states:
        return None
    return min(states, key=lambda state: STATE_PRIORITY.get(state, 99))


def _state_counts(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        state = str(row.get("state") or "collecting")
        if state in VALID_STATES:
            counts[state] += 1
    ordered = sorted(counts.items(), key=lambda item: STATE_PRIORITY.get(item[0], 99))
    return ", ".join(
        f"{count} {state.replace('_', ' ')}"
        for state, count in ordered
    )


def _strategy_reason(rows: list[dict[str, Any]], state: str) -> str:
    matching = [row for row in rows if str(row.get("state") or "") == state]
    if len(rows) == 1 and matching:
        reason = str(matching[0].get("primary_reason") or "")
        if reason:
            return reason
    return (
        f"live strategy evidence: {_state_counts(rows)}; lane state follows the most "
        "advanced viable strategy while failed cohorts remain strategy-specific"
    )


def _blocked_state_cleared(source_state: str, rows: list[dict[str, Any]]) -> bool:
    """Require durable downstream evidence before clearing a cached blocked state.

    Strategy evidence currently reconstructs forward statistics and realized allocator
    outcomes. It does not reconstruct the transient current-candidate promotion probe.
    Therefore an old execution/settlement block is retained until a realized allocator
    outcome proves that the blocked boundary was crossed, or until newer evidence moves
    the strategy to a terminal economics/statistical conclusion.
    """
    if source_state not in {"execution_blocked", "settlement_blocked"}:
        return True
    states = {str(row.get("state") or "") for row in rows}
    if states & {"poor_economics", "statistical_failure", "certified"}:
        return True
    return any(_int(row.get("settled_allocator_outcome_count")) > 0 for row in rows)


def _source_wait_reason(row: dict[str, Any], source_wait: str) -> str:
    existing = str(row.get("primary_reason") or "")
    if existing:
        return existing
    if source_wait == "provider_gap":
        return "no fresh admitted authoritative provider is currently usable"
    if source_wait == "stale":
        return "authoritative provider integration exists, but its source evidence is stale"
    if source_wait == "redundancy_gap":
        return "authoritative provider evidence is connected, but independent-source redundancy is incomplete"
    if source_wait == "evidence_class_gap":
        return "authoritative provider evidence is connected, but required evidence classes are incomplete"
    return "source sufficiency is incomplete under the existing fail-closed source contract"


def _derive_non_strategy_state(row: dict[str, Any], settings) -> tuple[str | None, str | None]:
    """Recompute states that are fully determined by fields already projected live."""
    source_wait = _source_wait_state(row)
    if source_wait is not None:
        if source_wait == "provider_gap":
            return "provider_gap", _source_wait_reason(row, source_wait)
        # A connected-but-insufficient source layer blocks downstream interpretation.
        # Keep the operating headline at COLLECTING while stage carries the exact
        # evidence-class/redundancy/freshness reason.
        return "collecting", _source_wait_reason(row, source_wait)

    # Legacy snapshots without the newer source stage retain the old fail-closed
    # behavior so backward compatibility does not accidentally loosen a gate.
    if row.get("provider_ready") is False:
        return "provider_gap", "required authoritative provider evidence is not currently fresh and admitted"

    if bool(row.get("profitability_certified")):
        return "certified", "latest durable evidence satisfies the profitability-certification gates"

    settled = _int(row.get("settled_allocator_outcome_count"))
    settled_required = max(
        5,
        int(getattr(settings, "operating_certification_min_settled_trials", 20)),
    )
    allocator_mean_lower = _number(row.get("allocator_mean_net_return_ci_lower"))
    allocator_hit_lower = _number(row.get("allocator_profitable_rate_ci_lower"))
    allocator_profit = _number(row.get("allocator_realized_profit_usd"))
    allocator_hit_required = float(
        getattr(settings, "operating_certification_min_profitable_rate_lower", 0.50)
    )

    if settled >= settled_required:
        if allocator_profit is not None and allocator_profit <= 0:
            return "poor_economics", "latest settled allocator cohort has non-positive aggregate realized paper profit"
        if (
            allocator_mean_lower is None
            or allocator_mean_lower <= 0
            or allocator_hit_lower is None
            or allocator_hit_lower < allocator_hit_required
        ):
            return "statistical_failure", "latest settled allocator cohort does not clear the conservative profitability confidence gates"
        return "certified", "latest settled allocator cohort clears the profitability-certification gates"
    if settled > 0:
        return "certifying", f"allocator settlement evidence is accumulating ({settled}/{settled_required})"

    forward = _int(row.get("independent_forward_outcome_count"))
    forward_required = max(1, int(getattr(settings, "alpha_min_forward_samples", 30)))
    mean = _number(row.get("mean_forward_net_return"))
    mean_lower = _number(row.get("mean_forward_net_return_ci_lower"))
    hit_lower = _number(row.get("forward_hit_rate_ci_lower"))
    mean_required = float(getattr(settings, "alpha_min_forward_mean_return", 0.0))
    hit_required = float(getattr(settings, "alpha_min_hit_rate_lower_bound", 0.50))

    if forward >= forward_required:
        if mean is not None and mean <= 0:
            return "poor_economics", "latest independent forward cohort has non-positive mean net return"
        if (
            mean_lower is None
            or mean_lower <= mean_required
            or hit_lower is None
            or hit_lower < hit_required
        ):
            return "statistical_failure", "latest independent forward cohort does not clear the predeclared confidence gates"
        if _int(row.get("current_promoted_count")) > 0:
            return "certifying", "current promoted opportunities exist and allocator settlement evidence is accumulating"
        if _int(row.get("current_statistically_qualified_count")) > 0:
            return "execution_blocked", "statistically qualified opportunities are not currently crossing the execution/promotion boundary"

    best_net = _number(row.get("best_net_economics"))
    if best_net is not None and best_net <= 0 and _int(row.get("authoritative_observation_count")) > 0:
        return "poor_economics", "latest authoritative observations show non-positive conservative net economics"

    # If a provider gap was just closed, or a previous terminal state no longer has
    # evidence supporting it, move back to evidence accumulation. We intentionally do
    # not manufacture CERTIFYING for research-only lanes that lack a defined forward
    # certification contract.
    source_state = str(row.get("state") or "")
    if source_state == "provider_gap" and row.get("provider_ready") is True:
        return "collecting", "authoritative provider evidence is available and downstream evidence is accumulating"
    return None, None


def reconcile_live_operating_states(
    mechanism_payload: dict[str, Any],
    settings,
) -> dict[str, Any]:
    """Reconcile every displayed lane state from the newest durable read-plane facts.

    Provider connectivity, source sufficiency, and downstream qualification remain
    distinct. This function is presentation-only: it never writes evidence, creates
    candidates, changes qualification thresholds, or grants allocation/execution
    authority.
    """
    result = dict(mechanism_payload or {})
    rows: list[dict[str, Any]] = []

    for source in list(result.get("mechanisms") or []):
        if not isinstance(source, dict):
            continue
        row = dict(source)
        source_state = str(row.get("state") or "collecting")
        source_wait = _source_wait_state(row)
        strategy_rows = [
            dict(item)
            for item in list(row.get("strategy_evidence") or [])
            if isinstance(item, dict)
        ]

        new_state: str | None = None
        reason: str | None = None

        # An explicit source-sufficiency blocker precedes downstream strategy
        # interpretation. A redundancy/evidence/freshness gap is not a provider gap.
        if source_wait is not None:
            new_state = "provider_gap" if source_wait == "provider_gap" else "collecting"
            reason = _source_wait_reason(row, source_wait)
        elif row.get("provider_ready") is False:
            # Backward-compatible handling for old snapshots that predate the
            # waiting_for_source taxonomy.
            new_state = "provider_gap"
            reason = "required authoritative provider evidence is not currently fresh and admitted"
        elif strategy_rows:
            strategy_state = _mixed_strategy_state(strategy_rows)
            if strategy_state is not None:
                if _blocked_state_cleared(source_state, strategy_rows):
                    new_state = strategy_state
                    reason = _strategy_reason(strategy_rows, strategy_state)
                else:
                    new_state = source_state
                    reason = str(row.get("primary_reason") or "") or (
                        "latest durable evidence has not yet proven that the blocked boundary was crossed"
                    )

            row["forward_signal_count"] = sum(
                _int(item.get("forward_signal_count")) for item in strategy_rows
            )
            row["independent_forward_outcome_count"] = sum(
                _int(item.get("independent_forward_outcome_count")) for item in strategy_rows
            )
            row["settled_allocator_outcome_count"] = sum(
                _int(item.get("settled_allocator_outcome_count")) for item in strategy_rows
            )
        else:
            new_state, reason = _derive_non_strategy_state(row, settings)

        if new_state in VALID_STATES:
            row["state"] = new_state
            if reason:
                row["primary_reason"] = reason
            # Preserve precise source remediation written by the worker. Generic
            # operating next-actions apply only once the source layer is not the gate.
            if source_wait is None:
                if new_state == "provider_gap":
                    row["next_action"] = "restore fresh authoritative provider evidence; downstream gates remain fail-closed"
                elif new_state == "collecting":
                    row["next_action"] = "continue append-only evidence collection under unchanged thresholds"
                elif new_state == "poor_economics":
                    row["next_action"] = "continue observing; do not allocate unless after-cost/risk economics recover"
                elif new_state == "statistical_failure":
                    row["next_action"] = "continue independent evidence accumulation; do not weaken confidence or regime gates"
                elif new_state == "execution_blocked":
                    row["next_action"] = "continue fresh execution/cost/capacity evidence and require the existing promotion gates"
                elif new_state == "settlement_blocked":
                    row["next_action"] = "restore supported forward settlement evidence before interpreting realized profitability"
                elif new_state == "certifying":
                    row["next_action"] = "continue independent forward and allocator settlement evidence until certification is complete"
                elif new_state == "certified":
                    row["next_action"] = "maintain forward monitoring and revoke automatically if later evidence degrades"

        row["live_operating_state_reconciled"] = True
        row["live_operating_state_source"] = "latest durable dashboard/provider/strategy evidence"
        row["live_operating_state_presentation_only"] = True
        rows.append(row)

    result["mechanisms"] = rows
    result["live_operating_state_reconciled"] = True
    result["live_operating_state_presentation_only"] = True
    return result


def rebuild_live_action_queue(mechanism_payload: dict[str, Any]) -> dict[str, Any]:
    """Rebuild the queue from reconciled states instead of patching a stale queue."""
    actions: list[dict[str, Any]] = []
    for row in list(mechanism_payload.get("mechanisms") or []):
        if not isinstance(row, dict) or row.get("state") == "certified":
            continue
        actions.append(
            {
                "mechanism_id": row.get("mechanism_id"),
                "name": row.get("name"),
                "state": row.get("state"),
                "stage": row.get("stage"),
                "provider_ready": row.get("provider_ready"),
                "primary_reason": row.get("primary_reason"),
                "next_action": row.get("next_action"),
                "blockers": list(row.get("blockers") or []),
                "worker_state": row.get("forward_evidence_worker_state"),
                "dominant_rejection_gate": row.get("dominant_rejection_gate"),
                "strategy_evidence": list(row.get("strategy_evidence") or []),
            }
        )
    actions.sort(
        key=lambda item: (
            STATE_PRIORITY.get(str(item.get("state") or ""), 99),
            str(item.get("mechanism_id") or ""),
        )
    )
    return {
        "paper_only": True,
        "count": len(actions),
        "actions": actions,
        "live_operating_state_reconciled": True,
    }
