from __future__ import annotations

from typing import Any

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.runtime_index_maintenance import (
    CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
    ensure_runtime_indexes_after_api_bind,
)


WORKER_ID = "cycle-history-index-maintenance"
INDEX_NOT_READY_EXIT_CODE = 77


def _record_heartbeat(
    store: Any,
    *,
    state: str,
    stage: str,
    error_type: str | None = None,
    detail: dict[str, object] | None = None,
) -> None:
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
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "qualification_thresholds_unchanged": True,
                "certification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
                **(detail or {}),
            },
        )
    except Exception:
        pass


def run_index_maintenance() -> int:
    """Verify or build the one exact index required by cycle-history backfill.

    PostgreSQL DDL is delegated to the existing runtime-index maintainer, which uses
    ``CREATE INDEX CONCURRENTLY`` plus finite statement/lock deadlines and verifies
    ``pg_index.indisvalid``/``indisready`` before reporting success. The process exits
    after one bounded attempt so interrupted DDL cannot strand the runtime parent.
    """

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("cycle-history index maintenance requires durable persistence")

    before = cycle_history_exact_index_status(store)
    if bool(before.get("ready")):
        _record_heartbeat(
            store,
            state="success",
            stage="cycle_history_index_ready",
            detail={
                "index_status": before,
                "ddl_required": False,
            },
        )
        return 0

    _record_heartbeat(
        store,
        state="running",
        stage="cycle_history_index_maintenance_starting",
        detail={"index_status": before},
    )

    def progress(row: dict[str, object]) -> None:
        phase = str(row.get("phase") or "running")
        _record_heartbeat(
            store,
            state="degraded" if phase == "failed" else "running",
            stage=f"cycle_history_index_{phase}",
            error_type=(
                str(row.get("error_type")) if row.get("error_type") else None
            ),
            detail={
                "current_index": row.get("index"),
                "current_table": row.get("table"),
                "current_index_runtime_seconds": row.get("runtime_seconds"),
                "current_index_ok": row.get("ok"),
                "current_index_concurrent": row.get("concurrent"),
                "message": row.get("message"),
            },
        )

    result = ensure_runtime_indexes_after_api_bind(
        store,
        index_specs=CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
        progress=progress,
    )
    after = cycle_history_exact_index_status(store)
    ready = bool(result.get("complete")) and bool(after.get("ready"))
    _record_heartbeat(
        store,
        state="success" if ready else "degraded",
        stage=(
            "cycle_history_index_ready"
            if ready
            else "cycle_history_index_retry_pending"
        ),
        error_type=(None if ready else "CycleHistoryExactIndexUnavailable"),
        detail={
            "maintenance_result": result,
            "index_status": after,
            "ddl_required": True,
        },
    )
    return 0 if ready else INDEX_NOT_READY_EXIT_CODE


def main() -> int:
    try:
        return run_index_maintenance()
    except Exception as exc:
        try:
            settings = Settings.from_env()
            store = build_evidence_store(settings.evidence_db_path)
            if store is not None:
                _record_heartbeat(
                    store,
                    state="degraded",
                    stage="cycle_history_index_maintenance_failed",
                    error_type=type(exc).__name__,
                    detail={"message": str(exc)[:500]},
                )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "INDEX_NOT_READY_EXIT_CODE",
    "WORKER_ID",
    "run_index_maintenance",
]
