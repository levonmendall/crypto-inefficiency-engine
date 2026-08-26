from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine import read_api_end_to_end_certification_deploy as base


END_TO_END_PATH = "/v3/operations/end-to-end-certification"
CYCLE_HISTORY_INDEX_WORKER_ID = "cycle-history-index-maintenance"
CYCLE_HISTORY_INDEX_STALE_SECONDS = 180.0
CANONICAL_CONTROL_WORKER_ID = "canonical-control-operating-loop"


def _store_or_none():
    try:
        return base.active._store()  # noqa: SLF001 - production diagnostic composition
    except Exception:
        return None


def _raw_canonical_control_status() -> dict[str, object]:
    """Read only the one durable control heartbeat needed by the truth repair.

    The legacy repair re-ran the entire deployment-readiness composition after the base
    certification payload had already done so. That duplicated table/heartbeat reads on
    every request. Read the canonical-control heartbeat directly instead and keep this
    diagnostic fail-soft; absence can never promote certification.
    """

    store = _store_or_none()
    if store is None:
        return {}
    try:
        heartbeat = store.latest_worker_heartbeat(CANONICAL_CONTROL_WORKER_ID)
    except Exception:
        return {}
    if heartbeat is None:
        return {}

    detail = dict(getattr(heartbeat, "detail", {}) or {})
    return {
        "available": True,
        "state": getattr(heartbeat, "state", None),
        "error_type": getattr(heartbeat, "error_type", None),
        "cycle_history_cache_complete": detail.get("cycle_history_cache_complete"),
        "cycle_history_cache_progress": detail.get("cycle_history_cache_progress"),
        "historical_cache_progress": detail.get("historical_cache_progress"),
        "operating_reconciliation_complete": detail.get(
            "operating_reconciliation_complete"
        ),
        "qualified_bridge_publication_complete": detail.get(
            "qualified_bridge_publication_complete"
        ),
    }


def _cycle_history_index_maintenance_status() -> dict[str, object]:
    """Expose the dedicated exact-index owner without granting certification authority."""

    store = _store_or_none()
    if store is None:
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "reason": "index_heartbeat_store_unavailable",
            "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
            "single_owner": True,
            "certification_authority": False,
        }

    try:
        heartbeat = store.latest_worker_heartbeat(CYCLE_HISTORY_INDEX_WORKER_ID)
    except Exception as exc:
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "error_type": type(exc).__name__,
            "reason": "index_heartbeat_read_failed",
            "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
            "single_owner": True,
            "certification_authority": False,
        }
    if heartbeat is None:
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "reason": "index_heartbeat_unobserved",
            "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
            "single_owner": True,
            "certification_authority": False,
        }

    detail = dict(getattr(heartbeat, "detail", {}) or {})
    raw_status = detail.get("index_status")
    index_status = dict(raw_status) if isinstance(raw_status, dict) else {}
    raw_maintenance = detail.get("maintenance_result")
    maintenance_result = (
        dict(raw_maintenance) if isinstance(raw_maintenance, dict) else {}
    )

    observed_at = getattr(heartbeat, "observed_at", None)
    age_seconds = None
    if isinstance(observed_at, datetime):
        observed = (
            observed_at
            if observed_at.tzinfo is not None
            else observed_at.replace(tzinfo=timezone.utc)
        )
        age_seconds = max(
            0.0,
            (datetime.now(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds(),
        )
    stale = age_seconds is None or age_seconds > CYCLE_HISTORY_INDEX_STALE_SECONDS
    state = str(getattr(heartbeat, "state", None) or "unknown")
    stage = str(detail.get("stage") or "") or None
    ready = bool(
        not stale
        and state == "success"
        and stage == "cycle_history_index_ready"
        and index_status.get("ready") is True
    )

    return {
        "available": True,
        "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
        "state": state,
        "stage": stage,
        "error_type": getattr(heartbeat, "error_type", None),
        "observed_at": observed_at,
        "age_seconds": age_seconds,
        "stale": stale,
        "ready": ready,
        "index_status": index_status,
        "maintenance_result": maintenance_result,
        "canonical_index_name": index_status.get("canonical_index_name"),
        "effective_index_name": index_status.get("effective_index_name"),
        "planner_usable_verified": index_status.get("planner_usable_verified"),
        "reason": index_status.get("reason"),
        "current_index": detail.get("current_index"),
        "current_table": detail.get("current_table"),
        "current_index_runtime_seconds": detail.get(
            "current_index_runtime_seconds"
        ),
        "current_index_ok": detail.get("current_index_ok"),
        "current_index_concurrent": detail.get("current_index_concurrent"),
        "message": detail.get("message"),
        "attempt_number": detail.get("attempt_number"),
        "statement_timeout_ms": detail.get("statement_timeout_ms"),
        "previous_attempt_number": detail.get("previous_attempt_number"),
        "previous_stage": detail.get("previous_stage"),
        "previous_error_type": detail.get("previous_error_type"),
        "previous_message": detail.get("previous_message"),
        "previous_effective_index_name": detail.get(
            "previous_effective_index_name"
        ),
        "single_owner": True,
        "generic_runtime_exact_index_maintenance_disabled": True,
        "certification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def repaired_end_to_end_certification_payload() -> dict[str, object]:
    """Separate exact cycle-history truth from generic historical-cache telemetry.

    The legacy endpoint fell back from a missing ``cycle_history_cache_progress`` field
    to the unrelated strategy/outcome ``historical_cache_progress`` object, and also
    allowed generic historical-cache completion to satisfy the cycle-history serving
    target check. This repair recomputes that check from the raw control heartbeat's
    explicit cycle-history field plus the certified background target only.

    The base certification composition already performs the full readiness read. This
    wrapper deliberately does not call ``deployment_readiness`` a second time; it reads
    only the one canonical-control heartbeat needed to correct cycle-history semantics.
    """

    payload = dict(base.end_to_end_certification_payload())
    raw_control = _raw_canonical_control_status()

    background = payload.get("cycle_history_backfill")
    background = dict(background) if isinstance(background, dict) else {}
    background_progress = background.get("progress")
    if not isinstance(background_progress, dict):
        background_progress = {}

    raw_cycle_progress = raw_control.get("cycle_history_cache_progress")
    if not isinstance(raw_cycle_progress, dict):
        raw_cycle_progress = {}
    raw_cycle_complete = bool(raw_control.get("cycle_history_cache_complete"))

    background_cycle_complete = bool(
        background.get("available")
        and not background.get("stale")
        and background.get("cache_complete")
        and background.get("serving_scan_id")
    )
    cycle_history_serving_target_certified = bool(
        raw_cycle_complete or background_cycle_complete
    )

    checks = payload.get("checks")
    checks = dict(checks) if isinstance(checks, dict) else {}
    checks["cycle_history_serving_target_certified"] = (
        cycle_history_serving_target_certified
    )
    blockers = [name for name, passed in checks.items() if not bool(passed)]
    operationally_certified = not blockers

    historical_progress = raw_control.get("historical_cache_progress")
    if not isinstance(historical_progress, dict):
        historical_progress = {}
    cycle_progress = raw_cycle_progress or background_progress
    progress_source = (
        "canonical_control"
        if raw_cycle_progress
        else "background_backfill"
        if background_progress
        else "unavailable"
    )

    control = payload.get("control")
    control = dict(control) if isinstance(control, dict) else {}
    control.update(
        {
            "cycle_history_cache_complete": raw_cycle_complete,
            "cycle_history_cache_progress": dict(cycle_progress),
            "cycle_history_cache_progress_source": progress_source,
            "historical_cache_progress": dict(historical_progress),
            "cycle_history_generic_cache_fallback_disabled": True,
        }
    )

    strategy = historical_progress.get("strategy")
    if isinstance(strategy, dict):
        control["strategy_cache_initialized"] = strategy.get(
            "cache_initialized",
            bool(strategy.get("cache_count")),
        )
        control["strategy_cache_completion_state"] = strategy.get(
            "completion_state"
        )

    exact_index = _cycle_history_index_maintenance_status()
    if (
        background.get("first_certified_target_pending")
        and not exact_index.get("ready")
    ):
        background["waiting_on_exact_index"] = True
        background["exact_index_worker_id"] = CYCLE_HISTORY_INDEX_WORKER_ID

    payload.update(
        {
            "certified": operationally_certified,
            "operationally_certified": operationally_certified,
            "status": "certified" if operationally_certified else "blocked",
            "checks": checks,
            "blockers": blockers,
            "control": control,
            "cycle_history_backfill": background,
            "cycle_history_index_maintenance": exact_index,
            "cycle_history_exact_index_owner": CYCLE_HISTORY_INDEX_WORKER_ID,
            "cycle_history_exact_index_single_owner": True,
            "cycle_history_certification_source": (
                "canonical_control"
                if raw_cycle_complete
                else "background_backfill"
                if background_cycle_complete
                else "none"
            ),
            "cycle_history_generic_cache_fallback_disabled": True,
            "duplicate_readiness_read_disabled": True,
        }
    )
    return payload


__all__ = [
    "CANONICAL_CONTROL_WORKER_ID",
    "CYCLE_HISTORY_INDEX_STALE_SECONDS",
    "CYCLE_HISTORY_INDEX_WORKER_ID",
    "END_TO_END_PATH",
    "_cycle_history_index_maintenance_status",
    "_raw_canonical_control_status",
    "repaired_end_to_end_certification_payload",
]
