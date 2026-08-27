from __future__ import annotations

from inefficiency_engine import read_api_end_to_end_certification_deploy as base


END_TO_END_PATH = "/v3/operations/end-to-end-certification"
CYCLE_HISTORY_INDEX_WORKER_ID = "cycle-history-index-maintenance"
CYCLE_HISTORY_INDEX_STALE_SECONDS = 180.0
CYCLE_HISTORY_INDEX_READY_STAGES = frozenset(
    {
        "cycle_history_index_ready",
        "cycle_history_index_ready_observed_before_retry",
        "cycle_history_index_ready_observed_after_child_exit",
    }
)
CANONICAL_CONTROL_WORKER_ID = "canonical-control-operating-loop"


def _raw_canonical_control_status(
    worker: dict[str, object] | None = None,
) -> dict[str, object]:
    """Extract explicit cycle-history truth from the already-batched control row."""

    row = dict(worker) if isinstance(worker, dict) else {}
    if not row:
        return {}
    return {
        "available": bool(row.get("available")),
        "state": row.get("state"),
        "error_type": row.get("error_type"),
        "error_message": row.get("error_message"),
        "cycle_history_cache_complete": row.get("cycle_history_cache_complete"),
        "cycle_history_cache_progress": row.get("cycle_history_cache_progress"),
        "historical_cache_progress": row.get("historical_cache_progress"),
        "operating_reconciliation_complete": row.get(
            "operating_reconciliation_complete"
        ),
        "qualified_bridge_publication_complete": row.get(
            "qualified_bridge_publication_complete"
        ),
    }


def _worker_snapshot_error(workers: dict[str, object]) -> dict[str, object] | None:
    """Surface the shared batched-read failure without another durable lookup."""

    for raw in workers.values():
        if not isinstance(raw, dict) or raw.get("available"):
            continue
        error_type = raw.get("error_type")
        message = raw.get("error_message")
        if not error_type and not message:
            continue
        return {
            "error_type": error_type,
            "message": message,
            "query_strategy": raw.get("heartbeat_query_strategy"),
            "retryable": True,
            "certification_authority": False,
        }
    return None


def _cycle_history_index_maintenance_status(
    worker: dict[str, object] | None = None,
) -> dict[str, object]:
    """Expose exact-index readiness plus supervisor diagnostics from one compact row."""

    row = dict(worker) if isinstance(worker, dict) else {}
    if not row or not bool(row.get("available")):
        return {
            "available": False,
            "stale": True,
            "ready": False,
            "error_type": row.get("error_type") if row else None,
            "error_message": row.get("error_message") if row else None,
            "reason": "index_heartbeat_unavailable_in_compact_snapshot",
            "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
            "single_owner": True,
            "diagnostic_source": "batched_latest_worker_heartbeat",
            "additional_database_reads": 0,
            "certification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

    raw_status = row.get("index_status")
    index_status = dict(raw_status) if isinstance(raw_status, dict) else {}
    raw_maintenance = row.get("maintenance_result")
    maintenance_result = (
        dict(raw_maintenance) if isinstance(raw_maintenance, dict) else {}
    )
    raw_postgres_progress = row.get("postgres_index_progress")
    postgres_index_progress = (
        dict(raw_postgres_progress)
        if isinstance(raw_postgres_progress, dict)
        else {}
    )
    stale = bool(row.get("stale", True))
    state = str(row.get("state") or "unknown")
    stage = str(row.get("stage") or "") or None
    ready = bool(
        not stale
        and state == "success"
        and stage in CYCLE_HISTORY_INDEX_READY_STAGES
        and index_status.get("ready") is True
    )

    return {
        "available": True,
        "worker_id": CYCLE_HISTORY_INDEX_WORKER_ID,
        "state": state,
        "stage": stage,
        "error_type": row.get("error_type"),
        "observed_at": row.get("observed_at"),
        "age_seconds": row.get("age_seconds"),
        "stale": stale,
        "ready": ready,
        "index_status": index_status,
        "maintenance_result": maintenance_result,
        "canonical_index_name": index_status.get("canonical_index_name"),
        "effective_index_name": index_status.get("effective_index_name"),
        "planner_usable_verified": index_status.get("planner_usable_verified"),
        "reason": index_status.get("reason"),
        "current_index": row.get("current_index"),
        "current_table": row.get("current_table"),
        "current_index_runtime_seconds": row.get("current_index_runtime_seconds"),
        "current_index_ok": row.get("current_index_ok"),
        "current_index_concurrent": row.get("current_index_concurrent"),
        "message": row.get("message"),
        "attempt_number": row.get("attempt_number"),
        "statement_timeout_ms": row.get("statement_timeout_ms"),
        "previous_attempt_number": row.get("previous_attempt_number"),
        "previous_stage": row.get("previous_stage"),
        "previous_error_type": row.get("previous_error_type"),
        "previous_message": row.get("previous_message"),
        "previous_effective_index_name": row.get("previous_effective_index_name"),
        "supervisor_observation": row.get("supervisor_observation"),
        "supervisor_executes_ddl": row.get("supervisor_executes_ddl"),
        "child_pid": row.get("child_pid"),
        "child_runtime_seconds": row.get("child_runtime_seconds"),
        "executor_deadline_seconds": row.get("executor_deadline_seconds"),
        "postgres_progress_available": bool(row.get("postgres_progress_available")),
        "postgres_index_progress": postgres_index_progress,
        "child_return_code": row.get("child_return_code"),
        "child_timed_out": row.get("child_timed_out"),
        "child_exit_error_type": row.get("child_exit_error_type"),
        "process_termination_observed_by_supervisor": row.get(
            "process_termination_observed_by_supervisor"
        ),
        "termination_signal_number": row.get("termination_signal_number"),
        "termination_signal": row.get("termination_signal"),
        "possible_oom_or_external_kill": row.get("possible_oom_or_external_kill"),
        "oom_kill_proven": row.get("oom_kill_proven"),
        "consecutive_terminal_failures": row.get("consecutive_terminal_failures"),
        "retry_seconds": row.get("retry_seconds"),
        "retry_backoff_escalated": row.get("retry_backoff_escalated"),
        "ddl_retry_skipped": row.get("ddl_retry_skipped"),
        "diagnostic_source": "batched_latest_worker_heartbeat",
        "additional_database_reads": 0,
        "single_owner": True,
        "generic_runtime_exact_index_maintenance_disabled": True,
        "certification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def repaired_end_to_end_certification_payload() -> dict[str, object]:
    """Apply cycle-history truth semantics without any additional database reads.

    Base certification requests one compact latest-worker snapshot. This wrapper reuses
    those exact rows to keep generic historical caches from masquerading as cycle-history
    completion and to expose the dedicated exact-index owner. No second readiness call,
    heartbeat lookup, archive count, provider work, or PostgreSQL progress query occurs
    on the HTTP request path; supervisor diagnostics are copied from its durable row.
    """

    payload = dict(base.end_to_end_certification_payload(include_worker_truth=True))
    workers = payload.pop("_certification_workers", {})
    workers = dict(workers) if isinstance(workers, dict) else {}
    snapshot_error = _worker_snapshot_error(workers)
    raw_control = _raw_canonical_control_status(
        workers.get("canonical_control")
        if isinstance(workers.get("canonical_control"), dict)
        else None
    )

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
    if raw_control.get("error_message"):
        control["error_message"] = raw_control.get("error_message")

    strategy = historical_progress.get("strategy")
    if isinstance(strategy, dict):
        control["strategy_cache_initialized"] = strategy.get(
            "cache_initialized",
            bool(strategy.get("cache_count")),
        )
        control["strategy_cache_completion_state"] = strategy.get(
            "completion_state"
        )

    exact_row = workers.get("cycle_history_index_maintenance")
    exact_index = _cycle_history_index_maintenance_status(
        exact_row if isinstance(exact_row, dict) else None
    )
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
            "worker_snapshot_error": snapshot_error,
            "cycle_history_generic_cache_fallback_disabled": True,
            "duplicate_readiness_read_disabled": True,
            "truth_repair_additional_database_reads": 0,
        }
    )
    return payload


__all__ = [
    "CANONICAL_CONTROL_WORKER_ID",
    "CYCLE_HISTORY_INDEX_READY_STAGES",
    "CYCLE_HISTORY_INDEX_STALE_SECONDS",
    "CYCLE_HISTORY_INDEX_WORKER_ID",
    "END_TO_END_PATH",
    "_cycle_history_index_maintenance_status",
    "_raw_canonical_control_status",
    "_worker_snapshot_error",
    "repaired_end_to_end_certification_payload",
]
