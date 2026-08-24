from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Any

from sqlalchemy import event, inspect, text


_PATCH_MARKER = "_cie_bounded_strategy_evidence_runtime"
_TIMEOUT_MARKER = "_cie_control_database_timeouts"
_CACHE_LOCK = threading.RLock()
_CACHE: dict[tuple[int, str], dict[str, Any]] = {}


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
        "signals": defaultdict(int),
        "raw_outcomes": [],
        "observed_identity": {},
        "allocator_by_strategy": defaultdict(list),
        "supported_trials": defaultdict(int),
        "initialized": False,
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


def _load_alpha_initial(db: Any, state: dict[str, Any]) -> None:
    signals = state["signals"]
    observed_identity = state["observed_identity"]
    for strategy_id, family, count in db.execute(
        text(
            "SELECT strategy_id, family, COUNT(*) "
            "FROM alpha_forward_events "
            "WHERE event_type='signal' "
            "GROUP BY strategy_id, family"
        )
    ):
        strategy = str(strategy_id)
        family_name = str(family)
        signals[(strategy, family_name)] = int(count or 0)
        observed_identity[strategy] = family_name

    for strategy_id, family, payload_json in db.execute(
        text(
            "SELECT strategy_id, family, payload_json "
            "FROM alpha_forward_events "
            "WHERE event_type='outcome' ORDER BY id"
        )
    ):
        strategy = str(strategy_id)
        family_name = str(family)
        observed_identity[strategy] = family_name
        payload = _decode_payload(payload_json)
        if payload is not None:
            state["raw_outcomes"].append(payload)


def _load_alpha_incremental(db: Any, state: dict[str, Any], *, after_id: int) -> None:
    for strategy_id, family, event_type, payload_json in db.execute(
        text(
            "SELECT strategy_id, family, event_type, payload_json "
            "FROM alpha_forward_events WHERE id > :after_id ORDER BY id"
        ),
        {"after_id": int(after_id)},
    ):
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


def _load_allocation_outcomes_initial(db: Any, state: dict[str, Any]) -> None:
    for strategy_id, payload_json in db.execute(
        text(
            "SELECT strategy, payload_json "
            "FROM allocation_forward_outcomes ORDER BY id"
        )
    ):
        payload = _decode_payload(payload_json)
        if payload is not None:
            state["allocator_by_strategy"][str(strategy_id)].append(payload)


def _load_allocation_outcomes_incremental(
    db: Any,
    state: dict[str, Any],
    *,
    after_id: int,
) -> None:
    for strategy_id, payload_json in db.execute(
        text(
            "SELECT strategy, payload_json "
            "FROM allocation_forward_outcomes WHERE id > :after_id ORDER BY id"
        ),
        {"after_id": int(after_id)},
    ):
        payload = _decode_payload(payload_json)
        if payload is not None:
            state["allocator_by_strategy"][str(strategy_id)].append(payload)


def _load_trials_initial(db: Any, state: dict[str, Any]) -> None:
    for strategy_id, count in db.execute(
        text(
            "SELECT strategy, COUNT(*) FROM allocation_forward_trials "
            "WHERE settlement_supported = TRUE GROUP BY strategy"
        )
    ):
        state["supported_trials"][str(strategy_id)] = int(count or 0)


def _load_trials_incremental(db: Any, state: dict[str, Any], *, after_id: int) -> None:
    for strategy_id, supported in db.execute(
        text(
            "SELECT strategy, settlement_supported FROM allocation_forward_trials "
            "WHERE id > :after_id ORDER BY id"
        ),
        {"after_id": int(after_id)},
    ):
        if bool(supported):
            state["supported_trials"][str(strategy_id)] += 1


def _refresh_cached_ledgers(store: Any) -> dict[str, Any]:
    available = set(inspect(store.engine).get_table_names())
    cache_key = _cache_key(store)
    with _CACHE_LOCK:
        state = _CACHE.setdefault(cache_key, _new_cache_state())
        with store.engine.connect() as db:
            alpha_tail = _tail_id(db, "alpha_forward_events", available)
            trial_tail = _tail_id(db, "allocation_forward_trials", available)
            allocation_tail = _tail_id(db, "allocation_forward_outcomes", available)

            reset_required = bool(
                state["initialized"]
                and (
                    alpha_tail < int(state["alpha_tail"])
                    or trial_tail < int(state["trial_tail"])
                    or allocation_tail < int(state["allocation_tail"])
                )
            )
            if reset_required:
                state = _new_cache_state()
                _CACHE[cache_key] = state

            if not state["initialized"]:
                if "alpha_forward_events" in available:
                    _load_alpha_initial(db, state)
                if "allocation_forward_outcomes" in available:
                    _load_allocation_outcomes_initial(db, state)
                if "allocation_forward_trials" in available:
                    _load_trials_initial(db, state)
                state["initialized"] = True
            else:
                if alpha_tail > int(state["alpha_tail"]):
                    _load_alpha_incremental(
                        db,
                        state,
                        after_id=int(state["alpha_tail"]),
                    )
                if allocation_tail > int(state["allocation_tail"]):
                    _load_allocation_outcomes_incremental(
                        db,
                        state,
                        after_id=int(state["allocation_tail"]),
                    )
                if trial_tail > int(state["trial_tail"]):
                    _load_trials_incremental(
                        db,
                        state,
                        after_id=int(state["trial_tail"]),
                    )

            state["alpha_tail"] = alpha_tail
            state["trial_tail"] = trial_tail
            state["allocation_tail"] = allocation_tail

        return {
            "key": (alpha_tail, trial_tail, allocation_tail),
            "signals": dict(state["signals"]),
            "raw_outcomes": list(state["raw_outcomes"]),
            "observed_identity": dict(state["observed_identity"]),
            "allocator_by_strategy": {
                key: list(value)
                for key, value in state["allocator_by_strategy"].items()
            },
            "supported_trials": dict(state["supported_trials"]),
        }


def bounded_load_evidence(store: Any, settings: Any) -> dict[str, list[dict[str, Any]]]:
    """Load exact strategy evidence without rescanning append-only signal history.

    Initial startup aggregates signal/trial counts in SQL and reads only outcome
    payloads needed by the existing statistical calculations. Subsequent calls read
    only rows newer than each durable table tail. No evidence is truncated and no
    qualification threshold changes.
    """

    from inefficiency_engine import strategy_evidence_read as legacy

    cached = _refresh_cached_ledgers(store)
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
                state = "collecting"
                reason = "canonical settlement is enabled and awaiting an eligible allocator trial"
            elif settled < settled_required:
                state = "certifying"
                reason = f"allocator settlements are accumulating ({settled}/{settled_required})"
            elif isinstance(allocator.get("mean"), (float, int)) and float(allocator["mean"]) <= 0:
                state = "poor_economics"
                reason = "realized paper economics are non-positive after observed settlement and costs"
            elif (
                allocator.get("mean_lower") is None
                or float(allocator["mean_lower"]) <= 0
                or allocator.get("hit_lower") is None
                or float(allocator["hit_lower"]) < hit_required
            ):
                state = "statistical_failure"
                reason = "realized paper returns have not cleared the conservative profitability confidence hurdle"
            elif float(allocator.get("realized_profit") or 0.0) > 0:
                state = "certified"
                reason = "allocator-level forward paper profitability is certified"
            else:
                state = "poor_economics"
                reason = "settled cohort has not produced positive aggregate realized paper profit"
            by_mechanism[mechanism_id].append(
                {
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
                }
            )

    return {
        key: sorted(value, key=lambda row: str(row.get("strategy_id")))
        for key, value in by_mechanism.items()
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

    ``asyncio.wait_for`` cannot terminate a synchronous DBAPI call already running in
    an executor thread. Applying PostgreSQL session timeouts on every pool checkout
    makes blocked or unexpectedly expensive SQL abort inside the database before the
    25-second control-cycle deadline.
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
