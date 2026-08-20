from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy import inspect, text


# These are strategy identities already present in the production alpha registry.
# Explicitly listing the trend implementations ensures a newly added strategy is
# visible as COLLECTING even before its first signal/outcome has matured.
_CANONICAL_ALPHA_STRATEGIES: tuple[tuple[str, str, str, str], ...] = (
    ("trend_momentum", "directional_time_series", "time_series_momentum_v1", "Original time-series momentum"),
    ("trend_momentum", "directional_time_series", "cycle_aware_multi_horizon_trend_v1", "Cycle-aware multi-horizon trend"),
    ("mean_reversion", "directional_reversal", "mean_reversion_v1", "Robust mean reversion"),
    ("cross_sectional_relative_value", "cross_sectional_relative_value", "cross_sectional_relative_value_v1", "Cross-sectional relative value"),
    ("microstructure", "microstructure_orderflow", "microstructure_imbalance_v1", "Microstructure imbalance"),
)

_FAMILY_TO_MECHANISM = {
    "directional_time_series": "trend_momentum",
    "directional_reversal": "mean_reversion",
    "onchain_fundamental": "fundamental_onchain",
    "cross_sectional_relative_value": "cross_sectional_relative_value",
    "event_driven": "event_driven",
    "microstructure_orderflow": "microstructure",
}

_STRUCTURAL_STRATEGIES = {
    "price_discrepancy": (
        ("cex_spot_dislocation", "CEX spot dislocation"),
        ("cex_dex", "CEX↔DEX composite edge"),
    ),
    "carry": (
        ("funding_dispersion", "Funding dispersion"),
        ("spot_perp_basis", "Spot / perpetual basis"),
        ("futures_basis", "Dated-futures basis"),
    ),
}

_STATE_PRIORITY = {
    "certified": 0,
    "certifying": 1,
    "collecting": 2,
    "execution_blocked": 3,
    "settlement_blocked": 4,
    "statistical_failure": 5,
    "poor_economics": 6,
    "provider_gap": 7,
}

_CACHE_KEY: tuple[int, int, int] | None = None
_CACHE_VALUE: dict[str, list[dict[str, Any]]] | None = None


def _parse_time(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _mean_lower(values: list[float], z: float = 1.96) -> float | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean
    return mean - z * statistics.stdev(values) / math.sqrt(len(values))


def _wilson_lower(successes: int, total: int, z: float = 1.96) -> float | None:
    if total <= 0:
        return None
    p = successes / total
    denominator = 1.0 + z * z / total
    center = p + z * z / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return max(0.0, (center - margin) / denominator)


def _tail_id(db, table_name: str, available: set[str]) -> int:
    if table_name not in available:
        return 0
    value = db.execute(text(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1")).scalar_one_or_none()
    return int(value or 0)


def _independent_outcomes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mirror production de-overlap within strategy + asset + direction cohorts."""
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("strategy_id") or ""), str(row.get("asset") or ""), str(row.get("direction") or ""))].append(row)
    selected: list[dict[str, Any]] = []
    for cohort in grouped.values():
        next_independent_at: datetime | None = None
        ordered = sorted(
            cohort,
            key=lambda item: (
                _parse_time(item.get("observed_at")) or datetime.min,
                _parse_time(item.get("due_at")) or datetime.min,
                str(item.get("signal_id") or ""),
            ),
        )
        for row in ordered:
            observed = _parse_time(row.get("observed_at"))
            due = _parse_time(row.get("due_at"))
            if observed is None or due is None:
                continue
            if next_independent_at is not None and observed < next_independent_at:
                continue
            selected.append(row)
            next_independent_at = due
    return selected


def _outcome_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["realized_net_return"]) for row in rows if row.get("realized_net_return") is not None]
    positives = sum(value > 0 for value in values)
    return {
        "count": len(values),
        "mean": statistics.fmean(values) if values else None,
        "mean_lower": _mean_lower(values),
        "hit_rate": positives / len(values) if values else None,
        "hit_lower": _wilson_lower(positives, len(values)),
        "regime_count": len({str(row.get("regime")) for row in rows if row.get("regime")}),
    }


def _allocator_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["realized_net_return"]) for row in rows if row.get("realized_net_return") is not None]
    positives = sum(value > 0 for value in values)
    return {
        "count": len(values),
        "mean": statistics.fmean(values) if values else None,
        "mean_lower": _mean_lower(values),
        "hit_lower": _wilson_lower(positives, len(values)),
        "realized_profit": sum(float(row.get("realized_profit_usd") or 0.0) for row in rows),
    }


def diagnose_alpha_strategy(
    *,
    strategy_id: str,
    name: str,
    family: str,
    signal_count: int,
    outcomes: list[dict[str, Any]],
    allocator: dict[str, Any],
    settings,
    strategy_count: int,
) -> dict[str, Any]:
    stats = _outcome_stats(outcomes)
    count = int(stats["count"] or 0)
    mean = stats["mean"]
    mean_lower = stats["mean_lower"]
    hit_lower = stats["hit_lower"]
    regimes = int(stats["regime_count"] or 0)
    minimum = max(1, int(settings.alpha_min_forward_samples))
    hit_required = float(settings.alpha_min_hit_rate_lower_bound)
    regimes_required = max(1, int(settings.alpha_min_regimes))
    penalty = float(settings.alpha_multiple_testing_penalty_return) * math.sqrt(math.log(max(1, strategy_count) + 1.0))
    mean_required = float(settings.alpha_min_forward_mean_return) + penalty
    failed: list[str] = []
    if count < minimum:
        failed.append(f"independent outcomes {count} < required {minimum}")
    if count >= minimum and mean_lower is not None and float(mean_lower) <= mean_required:
        failed.append(f"mean-return CI lower {float(mean_lower):.6f} <= required {mean_required:.6f}")
    if count >= minimum and hit_lower is not None and float(hit_lower) < hit_required:
        failed.append(f"hit-rate CI lower {float(hit_lower):.3f} < required {hit_required:.3f}")
    if count >= minimum and regimes < regimes_required:
        failed.append(f"observed regimes {regimes} < required {regimes_required}")

    if signal_count <= 0 or count < minimum:
        state = "collecting"
        reason = f"independent forward evidence is accumulating ({count}/{minimum} outcomes)"
    elif isinstance(mean, (float, int)) and float(mean) <= 0:
        state = "poor_economics"
        reason = "completed independent forward evidence has non-positive mean net return"
    elif failed:
        state = "statistical_failure"
        reason = "forward evidence has not cleared every predeclared confidence/regime hurdle"
    else:
        settled = int(allocator.get("count") or 0)
        settled_required = max(5, int(getattr(settings, "operating_certification_min_settled_trials", 20)))
        allocator_hit_required = float(getattr(settings, "operating_certification_min_profitable_rate_lower", 0.50))
        certified = bool(
            settled >= settled_required
            and allocator.get("mean_lower") is not None
            and float(allocator["mean_lower"]) > 0
            and allocator.get("hit_lower") is not None
            and float(allocator["hit_lower"]) >= allocator_hit_required
            and float(allocator.get("realized_profit") or 0.0) > 0
        )
        state = "certified" if certified else "certifying"
        reason = (
            "allocator-level forward profitability is certified"
            if certified
            else f"statistical evidence is mature; allocator settlements are accumulating ({settled}/{settled_required})"
        )

    return {
        "strategy_id": strategy_id,
        "name": name,
        "family": family,
        "state": state,
        "qualification_scope": "strategy + asset + direction",
        "forward_signal_count": int(signal_count),
        "independent_forward_outcome_count": count,
        "mean_forward_net_return": mean,
        "mean_forward_net_return_ci_lower": mean_lower,
        "forward_hit_rate": stats["hit_rate"],
        "forward_hit_rate_ci_lower": hit_lower,
        "observed_regime_count": regimes,
        "required_forward_outcomes": minimum,
        "required_mean_return_ci_lower": mean_required,
        "required_hit_rate_ci_lower": hit_required,
        "required_regimes": regimes_required,
        "settled_allocator_outcome_count": int(allocator.get("count") or 0),
        "failed_gates": failed,
        "primary_reason": reason,
        "diagnostic_aggregate_only": True,
        "allocation_authority_unchanged": True,
        "paper_only": True,
    }


def _load_evidence(store, settings) -> dict[str, list[dict[str, Any]]]:
    global _CACHE_KEY, _CACHE_VALUE
    available = set(inspect(store.engine).get_table_names())
    with store.engine.connect() as db:
        key = (
            _tail_id(db, "alpha_forward_events", available),
            _tail_id(db, "allocation_forward_trials", available),
            _tail_id(db, "allocation_forward_outcomes", available),
        )
        if _CACHE_KEY == key and _CACHE_VALUE is not None:
            return _CACHE_VALUE

        signals: dict[tuple[str, str], int] = defaultdict(int)
        raw_outcomes: list[dict[str, Any]] = []
        observed_identity: dict[str, str] = {}
        if "alpha_forward_events" in available:
            for strategy_id, family, event_type, payload_json in db.execute(text(
                "SELECT strategy_id, family, event_type, payload_json FROM alpha_forward_events ORDER BY id"
            )):
                strategy_id = str(strategy_id)
                family = str(family)
                observed_identity[strategy_id] = family
                if str(event_type) == "signal":
                    signals[(strategy_id, family)] += 1
                    continue
                if str(event_type) != "outcome":
                    continue
                try:
                    payload = json.loads(str(payload_json))
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(payload, dict):
                    raw_outcomes.append(payload)

        allocator_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if "allocation_forward_outcomes" in available:
            for strategy_id, payload_json in db.execute(text(
                "SELECT strategy, payload_json FROM allocation_forward_outcomes ORDER BY id"
            )):
                try:
                    payload = json.loads(str(payload_json))
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(payload, dict):
                    allocator_by_strategy[str(strategy_id)].append(payload)

        supported_trials: dict[str, int] = defaultdict(int)
        if "allocation_forward_trials" in available:
            for strategy_id, supported in db.execute(text(
                "SELECT strategy, settlement_supported FROM allocation_forward_trials ORDER BY id"
            )):
                if bool(supported):
                    supported_trials[str(strategy_id)] += 1

    independent = _independent_outcomes(raw_outcomes)
    outcomes_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in independent:
        outcomes_by_strategy[str(row.get("strategy_id") or "")].append(row)

    canonical = {strategy_id: (mechanism_id, family, name) for mechanism_id, family, strategy_id, name in _CANONICAL_ALPHA_STRATEGIES}
    for strategy_id, family in observed_identity.items():
        mechanism_id = _FAMILY_TO_MECHANISM.get(family)
        if mechanism_id and strategy_id not in canonical:
            canonical[strategy_id] = (mechanism_id, family, strategy_id.replace("_", " ").strip().title())

    strategy_count = max(7, len(canonical))
    by_mechanism: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for strategy_id, (mechanism_id, family, name) in canonical.items():
        row = diagnose_alpha_strategy(
            strategy_id=strategy_id,
            name=name,
            family=family,
            signal_count=signals.get((strategy_id, family), 0),
            outcomes=outcomes_by_strategy.get(strategy_id, []),
            allocator=_allocator_stats(allocator_by_strategy.get(strategy_id, [])),
            settings=settings,
            strategy_count=strategy_count,
        )
        by_mechanism[mechanism_id].append(row)

    settled_required = max(5, int(getattr(settings, "operating_certification_min_settled_trials", 20)))
    hit_required = float(getattr(settings, "operating_certification_min_profitable_rate_lower", 0.50))
    for mechanism_id, specs in _STRUCTURAL_STRATEGIES.items():
        for strategy_id, name in specs:
            allocator = _allocator_stats(allocator_by_strategy.get(strategy_id, []))
            trial_count = int(supported_trials.get(strategy_id, 0))
            settled = int(allocator.get("count") or 0)
            if trial_count <= 0:
                state = "collecting"
                reason = "canonical settlement is enabled and awaiting an eligible allocator trial"
            elif settled < settled_required:
                state = "certifying"
                reason = f"allocator settlements are accumulating ({settled}/{settled_required})"
            elif isinstance(allocator.get("mean"), (float, int)) and float(allocator["mean"]) <= 0:
                state = "poor_economics"
                reason = "realized paper economics are non-positive after observed settlement and costs"
            elif allocator.get("mean_lower") is None or float(allocator["mean_lower"]) <= 0 or allocator.get("hit_lower") is None or float(allocator["hit_lower"]) < hit_required:
                state = "statistical_failure"
                reason = "realized paper returns have not cleared the conservative profitability confidence hurdle"
            elif float(allocator.get("realized_profit") or 0.0) > 0:
                state = "certified"
                reason = "allocator-level forward paper profitability is certified"
            else:
                state = "poor_economics"
                reason = "settled cohort has not produced positive aggregate realized paper profit"
            by_mechanism[mechanism_id].append({
                "strategy_id": strategy_id,
                "name": name,
                "family": "structural",
                "state": state,
                "qualification_scope": "strategy allocator settlement cohort",
                "forward_signal_count": trial_count,
                "independent_forward_outcome_count": 0,
                "mean_forward_net_return": None,
                "mean_forward_net_return_ci_lower": None,
                "forward_hit_rate": None,
                "forward_hit_rate_ci_lower": None,
                "observed_regime_count": 0,
                "required_forward_outcomes": 0,
                "required_mean_return_ci_lower": None,
                "required_hit_rate_ci_lower": None,
                "required_regimes": 0,
                "settled_allocator_outcome_count": settled,
                "required_allocator_outcomes": settled_required,
                "allocator_mean_net_return_ci_lower": allocator.get("mean_lower"),
                "allocator_profitable_rate_ci_lower": allocator.get("hit_lower"),
                "allocator_realized_profit_usd": allocator.get("realized_profit"),
                "failed_gates": [],
                "primary_reason": reason,
                "diagnostic_aggregate_only": False,
                "allocation_authority_unchanged": True,
                "paper_only": True,
            })

    result = {key: sorted(value, key=lambda row: str(row.get("strategy_id"))) for key, value in by_mechanism.items()}
    _CACHE_KEY = key
    _CACHE_VALUE = result
    return result


def _mixed_lane_state(rows: list[dict[str, Any]]) -> str:
    states = {str(row.get("state") or "collecting") for row in rows}
    for candidate in ("certified", "certifying", "collecting", "execution_blocked", "settlement_blocked", "statistical_failure", "poor_economics", "provider_gap"):
        if candidate in states:
            return candidate
    return "collecting"


def _state_counts(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("state") or "collecting")] += 1
    return ", ".join(f"{count} {state.replace('_', ' ')}" for state, count in sorted(counts.items(), key=lambda item: _STATE_PRIORITY.get(item[0], 99)))


def augment_mechanism_payload(store, settings, payload: dict[str, Any]) -> dict[str, Any]:
    """Add strategy attribution without changing trading/evidence authority."""
    evidence = _load_evidence(store, settings)
    result = dict(payload or {})
    rows: list[dict[str, Any]] = []
    for source in list(result.get("mechanisms") or []):
        if not isinstance(source, dict):
            continue
        row = dict(source)
        strategy_rows = [dict(item) for item in evidence.get(str(row.get("mechanism_id") or ""), [])]
        row["strategy_evidence"] = strategy_rows
        row["strategy_evidence_scope"] = "diagnostic attribution; allocation qualification remains strategy + asset + direction"
        row["negative_conclusions_strategy_specific"] = True
        if row.get("provider_ready") and len(strategy_rows) > 1:
            mixed_state = _mixed_lane_state(strategy_rows)
            state_set = {str(item.get("state") or "") for item in strategy_rows}
            if len(state_set) > 1:
                row["state"] = mixed_state
                row["primary_reason"] = (
                    f"mixed strategy evidence: {_state_counts(strategy_rows)}; negative conclusions remain attached "
                    "to the strategy that produced them while the economic lane continues under unchanged gates"
                )
                row["next_action"] = (
                    "continue independent strategy + asset + direction evidence; keep failed cohorts intact and do not lower thresholds"
                )
        rows.append(row)
    result["mechanisms"] = rows
    result["strategy_evidence_attribution"] = True
    result["strategy_evidence_cache_mode"] = "tail-keyed append-only ledger cache"
    return result


def reconcile_action_queue(queue: dict[str, Any], mechanism_payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(queue or {})
    by_id = {
        str(row.get("mechanism_id")): row
        for row in list(mechanism_payload.get("mechanisms") or [])
        if isinstance(row, dict)
    }
    actions: list[dict[str, Any]] = []
    for source in list(result.get("actions") or []):
        if not isinstance(source, dict):
            continue
        row = dict(source)
        mechanism = by_id.get(str(row.get("mechanism_id") or ""))
        if mechanism is not None:
            row["state"] = mechanism.get("state")
            row["primary_reason"] = mechanism.get("primary_reason")
            row["next_action"] = mechanism.get("next_action")
            row["strategy_evidence"] = mechanism.get("strategy_evidence") or []
        actions.append(row)
    actions.sort(key=lambda item: (_STATE_PRIORITY.get(str(item.get("state") or ""), 99), str(item.get("mechanism_id") or "")))
    result["actions"] = actions
    result["count"] = len(actions)
    return result
