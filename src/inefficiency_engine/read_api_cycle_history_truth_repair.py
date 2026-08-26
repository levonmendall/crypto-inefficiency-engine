from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine import read_api_end_to_end_certification_deploy as base


END_TO_END_PATH = "/v3/operations/end-to-end-certification"
CYCLE_HISTORY_INDEX_WORKER_ID = "cycle-history-index-maintenance"
CYCLE_HISTORY_INDEX_STALE_SECONDS = 180.0


def _cycle_history_index_maintenance_status() -> dict[str, object]:
    """Expose the dedicated exact-index owner without granting certification authority."""

    try:
        store = base.active._store()  # noqa: SLF001 - production diagnostic composition
    except Exception as exc:
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "error_type": type(exc).__name__,
            "reason": "index_heartbeat_store_unavailable",
            "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
            "single_owner": True,
            "certification_authority": False,
        }
    if store is None:
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "reason": "index_heartbeat_store_unconfigured",
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
    """

    payload = dict(base.end_to_end_certification_payload())

    raw_control: dict[str, object] = {}
    try:
        ready = dict(base.active.deployment_readiness())
        runtime = ready.get("runtime_heartbeats")
        workers = runtime.get("workers") if isinstance(runtime, dict) else {}
        raw_control = base._worker(workers, "canonical_control")
    except Exception:
        # The base endpoint already failed closed on readiness. A second diagnostic read
        # is advisory; if unavailable, never promote generic history into cycle history.
        raw_control = {}

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
        }
    )
    return payload


__all__ = [
    "CYCLE_HISTORY_INDEX_STALE_SECONDS",
    "CYCLE_HISTORY_INDEX_WORKER_ID",
    "END_TO_END_PATH",
    "_cycle_history_index_maintenance_status",
    "repaired_end_to_end_certification_payload",
]
