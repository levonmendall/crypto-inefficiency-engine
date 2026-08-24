from __future__ import annotations

from typing import Any


CONTROL_LABEL = "canonical_control"
CONTROL_WORKER_ID = "canonical-control-operating-loop"
_INSTALL_MARKER = "_cie_cycle_history_health_observability_installed"

# Keep the public health payload bounded while exposing the exact durable failure that
# canonical control already records. These are diagnostics only; none of them are used
# as liveness, qualification, allocation, or execution authority.
_PROGRESS_FIELDS = (
    "complete",
    "working_complete",
    "rolling_refresh_in_progress",
    "promoted_working_target",
    "serving_scan_id",
    "serving_target_completed_at",
    "working_target_scan_id",
    "working_target_completed_at",
    "mode",
    "query_budget",
    "bucket_queries",
    "checkpoint_writes",
    "stable_rows_retained",
    "boundary_rows_retained",
    "current_pair_count",
    "cached_pair_count",
    "incomplete_pair_count",
    "next_pair_index",
    "rows_per_day",
    "required_history_hours",
    "long_cutoff",
    "recent_cutoff",
    "boundary_day",
    "boundary_cutoff",
    "elapsed_seconds",
    "time_budget_seconds",
    "stopped_for_time_budget",
    "durable_checkpoint_persisted",
    "control_executor_slice_seconds",
    "control_executor_bucket_query_cap",
    "control_executor_supervisor_safe_slice",
    "error_type",
    "message",
)


def _control_detail(base: Any) -> dict[str, object]:
    try:
        store = base._store()  # noqa: SLF001 - deploy-layer diagnostic hook
        if store is None:
            return {}
        heartbeat = store.latest_worker_heartbeat(CONTROL_WORKER_ID)
    except Exception:
        return {}
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def _bounded_cycle_history_progress(detail: dict[str, object]) -> dict[str, object]:
    raw = detail.get("cycle_history_cache_progress")
    if not isinstance(raw, dict):
        return {}
    return {key: raw.get(key) for key in _PROGRESS_FIELDS if key in raw}


def install_cycle_history_health_observability(base: Any) -> None:
    """Expose already-persisted cycle-history failure detail through `/health`.

    The disposable control executor stores the underlying cache exception type/message
    plus bounded cache progress in its parent heartbeat. The existing health adapter
    exposes only the generic ``CycleHistoryCacheError`` control state, which prevents a
    production diagnosis of the exact failed DB/cache operation. This hook copies a
    bounded diagnostic subset into the existing canonical-control health record.
    """

    if bool(getattr(base, _INSTALL_MARKER, False)):
        return

    original = base._runtime_heartbeats  # noqa: SLF001

    def runtime_heartbeats_with_cycle_history_detail() -> dict[str, object]:
        payload = original()
        workers = payload.get("workers")
        if not isinstance(workers, dict):
            return payload

        control = workers.get(CONTROL_LABEL)
        if not isinstance(control, dict) or not bool(control.get("available")):
            payload["cycle_history_cache_error_observability"] = True
            return payload

        detail = _control_detail(base)
        progress = _bounded_cycle_history_progress(detail)
        control.update(
            {
                "cycle_history_cache_complete": detail.get(
                    "cycle_history_cache_complete"
                ),
                "cycle_history_cache_progress": progress,
                "cycle_history_cache_error_type": progress.get("error_type"),
                "cycle_history_cache_error_message": progress.get("message"),
                "cycle_history_cache_diagnostic_only": True,
            }
        )
        workers[CONTROL_LABEL] = control
        payload["cycle_history_cache_error_observability"] = True
        return payload

    base._runtime_heartbeats = runtime_heartbeats_with_cycle_history_detail  # type: ignore[attr-defined]  # noqa: SLF001
    setattr(base, _INSTALL_MARKER, True)
