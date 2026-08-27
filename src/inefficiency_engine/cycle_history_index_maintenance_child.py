from __future__ import annotations

import time
from typing import Any

from inefficiency_engine import runtime_index_maintenance
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_exact_index_direct import (
    EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
    ensure_exact_cycle_history_index_direct,
)
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status
from inefficiency_engine.cycle_history_index_runtime_store import (
    build_cycle_history_index_runtime_store,
)


# Keep this module-level seam for existing regression tests while production resolves to
# the schema-free builder. No EvidenceStore/schema bootstrap is used on this path.
build_evidence_store = build_cycle_history_index_runtime_store

WORKER_ID = "cycle-history-index-maintenance"
INDEX_NOT_READY_EXIT_CODE = 77
INDEX_TERMINAL_HEARTBEAT_UNAVAILABLE_EXIT_CODE = 79
DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS = 3_600_000
TERMINAL_HEARTBEAT_WRITE_ATTEMPTS = 4
TERMINAL_HEARTBEAT_RETRY_SECONDS = 1.0

_PREVIOUS_CHILD_TERMINAL_FIELDS = {
    "child_terminal_stage": "previous_child_terminal_stage",
    "child_sql_error_type": "previous_child_sql_error_type",
    "child_sql_error_message": "previous_child_sql_error_message",
    "child_return_code": "previous_child_return_code",
    "child_timed_out": "previous_child_timed_out",
    "termination_signal": "previous_termination_signal",
    "termination_signal_number": "previous_termination_signal_number",
    "possible_oom_or_external_kill": "previous_possible_oom_or_external_kill",
    "oom_kill_proven": "previous_oom_kill_proven",
}


def _record_heartbeat(
    store: Any,
    *,
    state: str,
    stage: str,
    error_type: str | None = None,
    detail: dict[str, object] | None = None,
    durable_attempts: int = 1,
) -> bool:
    """Write a heartbeat; terminal callers may request bounded persistence retries."""

    attempts = max(1, int(durable_attempts))
    for attempt in range(attempts):
        try:
            store.record_worker_heartbeat(
                worker_id=WORKER_ID,
                state=state,
                error_type=error_type,
                detail={
                    "stage": stage,
                    "background_maintenance_only": True,
                    "dedicated_cycle_history_index_owner": True,
                    "create_index_concurrently_required_in_postgres": True,
                    "schema_free_exact_index_runtime_store": True,
                    "generic_schema_inspector_bypassed": True,
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "qualification_thresholds_unchanged": True,
                    "certification_authority": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                    "terminal_heartbeat_write_attempt": attempt + 1,
                    "terminal_heartbeat_write_attempts_allowed": attempts,
                    **(detail or {}),
                },
            )
            return True
        except Exception:
            if attempt + 1 < attempts:
                time.sleep(TERMINAL_HEARTBEAT_RETRY_SECONDS)
    return False


def _latest_attempt_detail(store: Any) -> tuple[Any | None, dict[str, object]]:
    try:
        heartbeat = store.latest_worker_heartbeat(WORKER_ID)
    except Exception:
        return None, {}
    if heartbeat is None:
        return None, {}
    return heartbeat, dict(getattr(heartbeat, "detail", None) or {})


def _previous_child_terminal_context(detail: dict[str, object]) -> dict[str, object]:
    return {
        target: detail[source]
        for source, target in _PREVIOUS_CHILD_TERMINAL_FIELDS.items()
        if detail.get(source) is not None
    }


def _previous_attempt_context(store: Any) -> tuple[int, dict[str, object]]:
    heartbeat, detail = _latest_attempt_detail(store)
    if heartbeat is None:
        return 1, {}

    try:
        previous_attempt = max(0, int(detail.get("attempt_number") or 0))
    except (TypeError, ValueError):
        previous_attempt = 0

    context: dict[str, object] = _previous_child_terminal_context(detail)
    if previous_attempt:
        context["previous_attempt_number"] = previous_attempt
    previous_stage = detail.get("stage")
    if previous_stage:
        context["previous_stage"] = previous_stage
    previous_error = getattr(heartbeat, "error_type", None) or detail.get("error_type")
    if previous_error:
        context["previous_error_type"] = str(previous_error)
    previous_message = detail.get("message")
    if previous_message:
        context["previous_message"] = str(previous_message)[:500]
    previous_index = detail.get("effective_index_name") or detail.get("current_index")
    if previous_index:
        context["previous_effective_index_name"] = str(previous_index)

    return previous_attempt + 1, context


def _current_attempt_context(store: Any) -> tuple[int, dict[str, object]]:
    _heartbeat, detail = _latest_attempt_detail(store)
    try:
        attempt_number = max(1, int(detail.get("attempt_number") or 1))
    except (TypeError, ValueError):
        attempt_number = 1
    keys = (
        "previous_attempt_number",
        "previous_stage",
        "previous_error_type",
        "previous_message",
        "previous_effective_index_name",
        *_PREVIOUS_CHILD_TERMINAL_FIELDS.values(),
    )
    context = {key: detail[key] for key in keys if detail.get(key) is not None}
    return attempt_number, context


def run_index_maintenance() -> int:
    """Verify/build the exact index without entering generic PostgreSQL schema bootstrap."""

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("cycle-history index maintenance requires durable persistence")

    try:
        attempt_number, previous_context = _previous_attempt_context(store)
        attempt_context: dict[str, object] = {
            "attempt_number": attempt_number,
            "statement_timeout_ms": DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS,
            "pre_ddl_statement_timeout_ms": EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
            **previous_context,
        }

        before = cycle_history_exact_index_status(store)
        if bool(before.get("ready")):
            durable = _record_heartbeat(
                store,
                state="success",
                stage="cycle_history_index_ready",
                detail={**attempt_context, "index_status": before, "ddl_required": False},
                durable_attempts=TERMINAL_HEARTBEAT_WRITE_ATTEMPTS,
            )
            return 0 if durable else INDEX_TERMINAL_HEARTBEAT_UNAVAILABLE_EXIT_CODE

        _record_heartbeat(
            store,
            state="running",
            stage="cycle_history_index_preddl_pending",
            detail={**attempt_context, "index_status": before},
        )

        def progress(row: dict[str, object]) -> None:
            phase = str(row.get("phase") or "running")
            _record_heartbeat(
                store,
                state="degraded" if phase == "failed" else "running",
                stage=f"cycle_history_index_{phase}",
                error_type=(str(row.get("error_type")) if row.get("error_type") else None),
                detail={
                    **attempt_context,
                    "current_index": row.get("index"),
                    "current_table": row.get("table"),
                    "current_index_runtime_seconds": row.get("runtime_seconds"),
                    "current_index_ok": row.get("ok"),
                    "current_index_concurrent": row.get("concurrent"),
                    "message": row.get("message"),
                    "effective_index_name": row.get("effective_index_name"),
                    "pre_ddl_complete": row.get("pre_ddl_complete"),
                    "pre_ddl_runtime_seconds": row.get("pre_ddl_runtime_seconds"),
                    "ddl_runtime_seconds": row.get("ddl_runtime_seconds"),
                },
                durable_attempts=(
                    TERMINAL_HEARTBEAT_WRITE_ATTEMPTS if phase == "failed" else 1
                ),
            )

        previous_timeout_ms = (
            runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        )
        runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS = (
            DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS
        )
        try:
            result = ensure_exact_cycle_history_index_direct(
                store,
                statement_timeout_ms=DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS,
                progress=progress,
            )
        finally:
            runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS = (
                previous_timeout_ms
            )

        after = cycle_history_exact_index_status(store)
        ready = bool(result.get("complete")) and bool(after.get("ready"))
        attempted = result.get("attempted")
        rows = attempted if isinstance(attempted, list) else []
        last = dict(rows[-1]) if rows and isinstance(rows[-1], dict) else {}
        terminal_durable = _record_heartbeat(
            store,
            state="success" if ready else "degraded",
            stage="cycle_history_index_ready" if ready else "cycle_history_index_retry_pending",
            error_type=(None if ready else "CycleHistoryExactIndexUnavailable"),
            detail={
                **attempt_context,
                "maintenance_result": result,
                "index_status": after,
                "ddl_required": last.get("ddl_required", True),
                "effective_index_name": last.get("effective_index_name"),
                "message": last.get("message"),
                "child_terminal_heartbeat_required_before_retry": True,
            },
            durable_attempts=TERMINAL_HEARTBEAT_WRITE_ATTEMPTS,
        )
        if not terminal_durable:
            return INDEX_TERMINAL_HEARTBEAT_UNAVAILABLE_EXIT_CODE
        return 0 if ready else INDEX_NOT_READY_EXIT_CODE
    finally:
        dispose = getattr(store, "dispose", None)
        if callable(dispose):
            dispose()


def main() -> int:
    try:
        return run_index_maintenance()
    except Exception as exc:
        store = None
        try:
            settings = Settings.from_env()
            store = build_evidence_store(settings.evidence_db_path)
            if store is not None:
                attempt_number, previous_context = _current_attempt_context(store)
                _record_heartbeat(
                    store,
                    state="degraded",
                    stage="cycle_history_index_maintenance_failed",
                    error_type=type(exc).__name__,
                    detail={
                        "attempt_number": attempt_number,
                        "statement_timeout_ms": DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS,
                        "pre_ddl_statement_timeout_ms": EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
                        **previous_context,
                        "message": str(exc)[:500],
                        "child_terminal_heartbeat_required_before_retry": True,
                    },
                    durable_attempts=TERMINAL_HEARTBEAT_WRITE_ATTEMPTS,
                )
        except Exception:
            pass
        finally:
            if store is not None:
                dispose = getattr(store, "dispose", None)
                if callable(dispose):
                    dispose()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS",
    "INDEX_NOT_READY_EXIT_CODE",
    "INDEX_TERMINAL_HEARTBEAT_UNAVAILABLE_EXIT_CODE",
    "TERMINAL_HEARTBEAT_RETRY_SECONDS",
    "TERMINAL_HEARTBEAT_WRITE_ATTEMPTS",
    "WORKER_ID",
    "run_index_maintenance",
]
