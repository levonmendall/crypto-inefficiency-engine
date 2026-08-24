from __future__ import annotations

import json
import os
import threading
from collections import defaultdict
from typing import Any

from sqlalchemy import event, inspect, text


_PATCH_MARKER = "_cie_bounded_strategy_evidence_runtime"
_TIMEOUT_MARKER = "_cie_control_database_timeouts"
_CACHE_LOCK = threading.RLock()
_DEFAULT_BOOTSTRAP_BATCH_ROWS = 5000
_CACHE: dict[tuple[int, str], dict[str, Any]] = {}


def _bootstrap_batch_rows() -> int:
    raw = os.getenv(
        "CIE_CONTROL_STRATEGY_BOOTSTRAP_BATCH_ROWS",
        str(_DEFAULT_BOOTSTRAP_BATCH_ROWS),
    )
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_BOOTSTRAP_BATCH_ROWS
    return max(100, min(20000, value))


def _cache_key(store: Any) -> tuple[int, str]:
    return (
        id(store.engine),
        str(getattr(store, "safe_database_url", "")),
    )


def _new_cache_state() -> dict[str, Any]:
    return {
        "alpha_tail": 0,
        "trial_tail": 0,
        "allocation_tail": 0,
        "alpha_target_tail": 0,
        "trial_target_tail": 0,
        "allocation_target_tail": 0,
        "signals": defaultdict(int),
        "raw_outcomes": [],
        "observed_identity": {},
        "allocator_by_strategy": defaultdict(list),
        "supported_trials": defaultdict(int),
        "cache_complete": False,
        "last_alpha_batch_rows": 0,
        "last_trial_batch_rows": 0,
        "last_allocation_batch_rows": 0,
    }


def _tail_id(db: Any, table_name: str, available: set[str]) -> int:
    if table_name not in available:
        return 0
    value = db.execute(
        text(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1")
    ).scalar_one_or_none()
    return int(value or 0)


def _decode_payload(raw: object) -> dict[str, Any] | None:
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_alpha_batch(
    db: Any,
    state: dict[str, Any],
    *,
    after_id: int,
    target_id: int,
    batch_rows: int,
) -> tuple[int, int]:
    if target_id <= after_id:
        return target_id, 0
    rows = list(
        db.execute(
            text(
                "SELECT id, strategy_id, family, event_type, payload_json "
                "FROM alpha_forward_events "
                "WHERE id > :after_id AND id <= :target_id "
                "ORDER BY id LIMIT :batch_rows"
            ),
            {
                "after_id": int(after_id),
                "target_id": int(target_id),
                "batch_rows": int(batch_rows),
            },
        )
    )
    processed = int(after_id)
    for row_id, strategy_id, family, event_type, payload_json in rows:
        processed = int(row_id)
        strategy = str(strategy_id)
        family_name = str(family)
        state["observed_identity"][strategy] = family_name
        if str(event_type) == "signal":
            state["signals"][(strategy, family_name)] += 1
            continue
        if str(event_type) != "outcome":
            continue
        payload = _decode_payload(payload_json)
        if payload is not None:
            state["raw_outcomes"].append(payload)
    if len(rows) < batch_rows:
        processed = target_id
    return processed, len(rows)


def _load_allocation_batch(
    db: Any,
    state: dict[str, Any],
    *,
    after_id: int,
    target_id: int,
    batch_rows: int,
) -> tuple[int, int]:
    if target_id <= after_id:
        return target_id, 0
    rows = list(
        db.execute(
            text(
                "SELECT id, strategy, payload_json "
                "FROM allocation_forward_outcomes "
                "WHERE id > :after_id AND id <= :target_id "
                "ORDER BY id LIMIT :batch_rows"
            ),
            {
                "after_id": int(after_id),
                "target_id": int(target_id),
                "batch_rows": int(batch_rows),
            },
        )
    )
    processed = int(after_id)
    for row_id, strategy_id, payload_json in rows:
        processed = int(row_id)
        payload = _decode_payload(payload_json)
        if payload is not None:
            state["allocator_by_strategy"][str(strategy_id)].append(payload)
    if len(rows) < batch_rows:
        processed = target_id
    return processed, len(rows)


def _load_trial_batch(
    db: Any,
    state: dict[str, Any],
    *,
    after_id: int,
    target_id: int,
    batch_rows: int,
) -> tuple[int, int]:
    if target_id <= after_id:
        return target_id, 0
    rows = list(
        db.execute(
            text(
                "SELECT id, strategy, settlement_supported "
                "FROM allocation_forward_trials "
                "WHERE id > :after_id AND id <= :target_id "
                "ORDER BY id LIMIT :batch_rows"
            ),
            {
                "after_id": int(after_id),
                "target_id": int(target_id),
                "batch_rows": int(batch_rows),
            },
        )
    )
    processed = int(after_id)
    for row_id, strategy_id, supported in rows:
        processed = int(row_id)
        if bool(supported):
            state["supported_trials"][str(strategy_id)] += 1
    if len(rows) < batch_rows:
        processed = target_id
    return processed, len(rows)


def _refresh_cached_ledgers(store: Any) -> dict[str, Any]:
    """Advance exact strategy evidence by bounded primary-key batches.

    No partially loaded history is allowed to authorize qualification. The returned
    cache is marked complete only when all three append-only evidence ledgers have
    been consumed through the durable tails observed for this refresh.
    """

    available = set(inspect(store.engine).get_table_names())
    cache_key = _cache_key(store)
    batch_rows = _bootstrap_batch_rows()
    with _CACHE_LOCK:
        state = _CACHE.setdefault(cache_key, _new_cache_state())
        with store.engine.connect() as db:
            alpha_target = _tail_id(db, "alpha_forward_events", available)
            trial_target = _tail_id(db, "allocation_forward_trials", available)
            allocation_target = _tail_id(db, "allocation_forward_outcomes", available)

            reset_required = bool(
                alpha_target < int(state["alpha_tail"])
                or trial_target < int(state["trial_tail"])
                or allocation_target < int(state["allocation_tail"])
            )
            if reset_required:
                state = _new_cache_state()
                _CACHE[cache_key] = state

            alpha_processed, alpha_rows = (
                _load_alpha_batch(
                    db,
                    state,
                    after_id=int(state["alpha_tail"]),
                    target_id=alpha_target,
                    batch_rows=batch_rows,
                )
                if "alpha_forward_events" in available
                else (0, 0)
            )
            allocation_processed, allocation_rows = (
                _load_allocation_batch(
                    db,
                    state,
                    after_id=int(state["allocation_tail"]),
                    target_id=allocation_target,
                    batch_rows=batch_rows,
                )
                if "allocation_forward_outcomes" in available
                else (0, 0)
            )
            trial_processed, trial_rows = (
                _load_trial_batch(
                    db,
                    state,
                    after_id=int(state["trial_tail"]),
                    target_id=trial_target,
                    batch_rows=batch_rows,
                )
                if "allocation_forward_trials" in available
                else (0, 0)
            )

            state["alpha_tail"] = alpha_processed
            state["trial_tail"] = trial_processed
            state["allocation_tail"] = allocation_processed
            state["alpha_target_tail"] = alpha_target
            state["trial_target_tail"] = trial_target
            state["allocation_target_tail"] = allocation_target
            state["last_alpha_batch_rows"] = alpha_rows
            state["last_trial_batch_rows"] = trial_rows
            state["last_allocation_batch_rows"] = allocation_rows
            state["cache_complete"] = bool(
                alpha_processed >= alpha_target
                and trial_processed >= trial_target
                and allocation_processed >= allocation_target
            )

        return {
            "key": (alpha_target, trial_target, allocation_target),
            "cache_complete": bool(state["cache_complete"]),
            "signals": dict(state["signals"]),
            "raw_outcomes": list(state["raw_outcomes"]),
            "observed_identity": dict(state["observed_identity"]),
            "allocator_by_strategy": {
                key: list(value)
                for key, value in state["allocator_by_strategy"].items()
            },
            "supported_trials": dict(state["supported_trials"]),
            "diagnostics": {
                "alpha_processed_tail": int(state["alpha_tail"]),
                "alpha_target_tail": alpha_target,
                "trial_processed_tail": int(state["trial_tail"]),
                "trial_target_tail": trial_target,
                "allocation_processed_tail": int(state["allocation_tail"]),
                "allocation_target_tail": allocation_target,
                "last_alpha_batch_rows": alpha_rows,
                "last_trial_batch_rows": trial_rows,
                "last_allocation_batch_rows": allocation_rows,
            },
        }


def _rebuilding_evidence(settings: Any) -> dict[str, list[dict[str, Any]]]:
    """Return explicit fail-closed strategy state while exact history catches up."""

    from inefficiency_engine import strategy_evidence_read as legacy

    by_mechanism: dict[str, list[dict[str, Any]]] = defaultdict(list)
    required_forward = max(1, int(getattr(settings, "alpha_min_forward_samples", 30)))
    for mechanism_id, family, strategy_id, name in legacy._CANONICAL_ALPHA_STRATEGIES:
        by_mechanism[mechanism_id].append(
            {
                "strategy_id": strategy_id,
                "name": name,
                "family": family,
                "state": "collecting",
                "qualification_scope": "exact historical strategy evidence cache",
                "forward_signal_count": 0,
                "independent_forward_outcome_count": 0,
                "mean_forward_net_return": None,
                "mean_forward_net_return_ci_lower": None,
                "forward_hit_rate": None,
                "forward_hit_rate_ci_lower": None,
                "observed_regime_count": 0,
                "required_forward_outcomes": required_forward,
                "required_mean_return_ci_lower": None,
                "required_hit_rate_ci_lower": None,
                "required_regimes": max(1, int(getattr(settings, "alpha_min_regimes", 1))),
                "settled_allocator_outcome_count": 0,
                "required_allocator_outcomes": max(
                    5,
                    int(getattr(settings, "operating_certification_min_settled_trials", 20)),
                ),
                "allocator_mean_net_return_ci_lower": None,
                "allocator_profitable_rate_ci_lower": None,
                "allocator_realized_profit_usd": None,
                "failed_gates": ["historical_evidence_cache_rebuilding"],
                "primary_reason": (
                    "exact historical evidence is rebuilding in bounded batches; "
                    "qualification is withheld until the durable cache is complete"
                ),
                "diagnostic_aggregate_only": False,
                "evidence_cache_complete": False,
                "allocation_authority_unchanged": True,
                "paper_only": True,
            }
        )

    settled_required = max(
        5,
        int(getattr(settings, "operating_certification_min_settled_trials", 20)),
    )
    for mechanism_id, specs in legacy._STRUCTURAL_STRATEGIES.items():
        for strategy_id, name in specs:
            by_mechanism[mechanism_id].append(
                {
                    "strategy_id": strategy_id,
                    "name": name,
                    "family": "structural",
                    "state": "collecting",
                    "qualification_scope": "exact historical allocator evidence cache",
                    "forward_signal_count": 0,
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
                    "settled_allocator_outcome_count": 0,
                    "required_allocator_outcomes": settled_required,
                    "allocator_mean_net_return_ci_lower": None,
                    "allocator_profitable_rate_ci_lower": None,
                    "allocator_realized_profit_usd": None,
                    "failed_gates": ["historical_evidence_cache_rebuilding"],
                    "primary_reason": (
                        "exact historical allocator evidence is rebuilding in bounded batches; "
                        "qualification is withheld until the durable cache is complete"
                    ),
                    "diagnostic_aggregate_only": False,
                    "evidence_cache_complete": False,
                    "allocation_authority_unchanged": True,
                    "paper_only": True,
                }
            )
    return {
        key: sorted(value, key=lambda row: str(row.get("strategy_id")))
        for key, value in by_mechanism.items()
    }


def bounded_load_evidence(store: Any, settings: Any) -> dict[str, list[dict[str, Any]]]:
    """Load exact strategy evidence with a bounded, fail-closed cold start.

    Every call consumes at most one primary-key batch from each append-only strategy
    ledger. Partial history is never passed to statistical qualification. Once all
    durable tails are caught up, existing exact de-overlap/statistical calculations
    run over the complete accumulated history and later calls consume only new tails.
    """

    from inefficiency_engine import strategy_evidence_read as legacy

    cached = _refresh_cached_ledgers(store)
    if not bool(cached["cache_complete"]):
        return _rebuilding_evidence(settings)

    raw_outcomes = list(cached["raw_outcomes"])
    signals = dict(cached["signals"])
    observed_identity = dict(cached["observed_identity"])
    allocator_by_strategy = dict(cached["allocator_by_strategy"])
    supported_trials = dict(cached["supported_trials"])

    independent = legacy._independent_outcomes(raw_outcomes)
    outcomes_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in independent:
        outcomes_by_strategy[str(row.get("strategy_id") or "")].append(row)

    canonical = {
        strategy_id: (mechanism_id, family, name)
        for mechanism_id, family, strategy_id, name in legacy._CANONICAL_ALPHA_STRATEGIES
    }
    for strategy_id, family in observed_identity.items():
        mechanism_id = legacy._FAMILY_TO_MECHANISM.get(family)
        if mechanism_id and strategy_id not in canonical:
            canonical[strategy_id] = (
                mechanism_id,
                family,
                strategy_id.replace("_", " ").strip().title(),
            )

    strategy_count = max(1, len(canonical))
    by_mechanism: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for strategy_id, (mechanism_id, family, name) in canonical.items():
        row = legacy.diagnose_alpha_strategy(
            strategy_id=strategy_id,
            name=name,
            family=family,
            signal_count=int(signals.get((strategy_id, family), 0)),
            outcomes=outcomes_by_strategy.get(strategy_id, []),
            allocator=legacy._allocator_stats(allocator_by_strategy.get(strategy_id, [])),
            settings=settings,
            strategy_count=strategy_count,
        )
        row["evidence_cache_complete"] = True
        by_mechanism[mechanism_id].append(row)

    settled_required = max(
        5,
        int(getattr(settings, "operating_certification_min_settled_trials", 20)),
    )
    hit_required = float(
        getattr(settings, "operating_certification_min_profitable_rate_lower", 0.50)
    )
    for mechanism_id, specs in legacy._STRUCTURAL_STRATEGIES.items():
        for strategy_id, name in specs:
            allocator = legacy._allocator_stats(allocator_by_strategy.get(strategy_id, []))
            trial_count = int(supported_trials.get(strategy_id, 0))
            settled = int(allocator.get("count") or 0)
            if trial_count <= 0:
                state_name = "collecting"
                reason = "canonical settlement is enabled and awaiting an eligible allocator trial"
            elif settled < settled_required:
                state_name = "certifying"
                reason = f"allocator settlements are accumulating ({settled}/{settled_required})"
            elif isinstance(allocator.get("mean"), (float, int)) and float(allocator["mean"]) <= 0:
                state_name = "poor_economics"
                reason = "realized paper economics are non-positive after observed settlement and costs"
            elif (
                allocator.get("mean_lower") is None
                or float(allocator["mean_lower"]) <= 0
                or allocator.get("hit_lower") is None
                or float(allocator["hit_lower"]) < hit_required
            ):
                state_name = "statistical_failure"
                reason = "realized paper returns have not cleared the conservative profitability confidence hurdle"
            elif float(allocator.get("realized_profit") or 0.0) > 0:
                state_name = "certified"
                reason = "allocator-level forward paper profitability is certified"
            else:
                state_name = "poor_economics"
                reason = "settled cohort has not produced positive aggregate realized paper profit"
            by_mechanism[mechanism_id].append(
                {
                    "strategy_id": strategy_id,
                    "name": name,
                    "family": "structural",
                    "state": state_name,
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
                    "evidence_cache_complete": True,
                    "allocation_authority_unchanged": True,
                    "paper_only": True,
                }
            )

    return {
        key: sorted(value, key=lambda row: str(row.get("strategy_id")))
        for key, value in by_mechanism.items()
    }


def bounded_strategy_evidence_cache_diagnostics() -> dict[str, object]:
    with _CACHE_LOCK:
        caches = []
        for (_engine_id, database_url), state in _CACHE.items():
            caches.append(
                {
                    "database": database_url,
                    "cache_complete": bool(state["cache_complete"]),
                    "alpha_processed_tail": int(state["alpha_tail"]),
                    "alpha_target_tail": int(state["alpha_target_tail"]),
                    "trial_processed_tail": int(state["trial_tail"]),
                    "trial_target_tail": int(state["trial_target_tail"]),
                    "allocation_processed_tail": int(state["allocation_tail"]),
                    "allocation_target_tail": int(state["allocation_target_tail"]),
                    "raw_outcome_count": len(state["raw_outcomes"]),
                    "last_alpha_batch_rows": int(state["last_alpha_batch_rows"]),
                    "last_trial_batch_rows": int(state["last_trial_batch_rows"]),
                    "last_allocation_batch_rows": int(state["last_allocation_batch_rows"]),
                }
            )
        return {
            "mode": "bounded_exact_bootstrap_then_incremental_tail",
            "batch_rows": _bootstrap_batch_rows(),
            "cache_count": len(caches),
            "all_caches_complete": bool(caches)
            and all(bool(row["cache_complete"]) for row in caches),
            "caches": caches,
        }


def install_bounded_strategy_evidence_runtime() -> None:
    """Install the bounded loader for API reads and canonical reconciliation."""

    from inefficiency_engine import evidence_velocity_runtime
    from inefficiency_engine import strategy_evidence_read

    if bool(getattr(strategy_evidence_read, _PATCH_MARKER, False)):
        evidence_velocity_runtime._load_strategy_evidence = bounded_load_evidence
        return
    strategy_evidence_read._load_evidence = bounded_load_evidence
    evidence_velocity_runtime._load_strategy_evidence = bounded_load_evidence
    setattr(strategy_evidence_read, _PATCH_MARKER, True)


def install_control_database_timeouts(
    store: Any,
    *,
    statement_timeout_seconds: float = 20.0,
    lock_timeout_seconds: float = 3.0,
) -> bool:
    """Give the control process a real PostgreSQL-side SQL/lock deadline.

    PostgreSQL session timeouts bound SQL after checkout; the control process also
    carries a separate pool-checkout and wall-clock deadline. These settings do not
    change evidence or qualification semantics.
    """

    engine = store.engine
    if str(getattr(engine.dialect, "name", "")) != "postgresql":
        return False
    if bool(getattr(engine, _TIMEOUT_MARKER, False)):
        return True

    statement_ms = max(1000, int(float(statement_timeout_seconds) * 1000.0))
    lock_ms = max(250, int(float(lock_timeout_seconds) * 1000.0))

    def apply_timeouts(dbapi_connection, _connection_record, _connection_proxy) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET statement_timeout TO {statement_ms}")
            cursor.execute(f"SET lock_timeout TO {lock_ms}")
        finally:
            cursor.close()

    event.listen(engine, "checkout", apply_timeouts)
    setattr(engine, _TIMEOUT_MARKER, True)
    return True
