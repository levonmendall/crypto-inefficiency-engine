from __future__ import annotations

from typing import Any

from inefficiency_engine import cycle_history_background_backfill as base
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status
from inefficiency_engine.evidence import build_evidence_store


INDEX_NOT_READY_EXIT_CODE = 77
_ORIGINAL_RECORD_HEARTBEAT = base._record_heartbeat


def _latest_progress(store: Any) -> tuple[dict[str, object], str | None, str | None]:
    """Return the last durable backfill progress even if the latest row was a retry."""

    try:
        heartbeat = store.latest_worker_heartbeat(base.WORKER_ID)
    except Exception:
        return {}, None, None
    if heartbeat is None:
        return {}, None, None

    detail = dict(getattr(heartbeat, "detail", {}) or {})
    progress = detail.get("progress")
    if not isinstance(progress, dict) or not progress:
        progress = detail.get("last_progress")
    if not isinstance(progress, dict):
        progress = {}

    stage = str(detail.get("stage") or "") or None
    error_type = str(getattr(heartbeat, "error_type", None) or "") or None
    return dict(progress), stage, error_type


def _record_heartbeat_preserving_progress(
    store: Any,
    *,
    state: str,
    stage: str,
    sequence: int | None = None,
    error_type: str | None = None,
    detail: dict[str, object] | None = None,
) -> None:
    """Keep the last exact checkpoint visible while a new disposable slice starts.

    Previously every ``backfill_batch_starting`` row replaced the useful terminal
    checkpoint as the latest worker heartbeat, so diagnostics repeatedly showed an empty
    progress object. This wrapper carries forward only already-durable progress; it does
    not synthesize or advance any historical evidence.
    """

    merged = dict(detail or {})
    if not isinstance(merged.get("progress"), dict) or not merged.get("progress"):
        previous, previous_stage, previous_error = _latest_progress(store)
        if previous:
            merged["progress"] = previous
            merged["last_progress"] = previous
            merged["progress_is_previous_durable_checkpoint"] = True
        if previous_stage and previous_stage != stage:
            merged["previous_backfill_stage"] = previous_stage
        if previous_error and error_type is None:
            merged["previous_backfill_error_type"] = previous_error

    _ORIGINAL_RECORD_HEARTBEAT(
        store,
        state=state,
        stage=stage,
        sequence=sequence,
        error_type=error_type,
        detail=merged,
    )


def run_backfill_slice() -> int:
    """Refuse raw history reconstruction until the exact PostgreSQL index is ready."""

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("cycle-history background backfill requires durable persistence")

    progress, previous_stage, previous_error = _latest_progress(store)
    index_status = cycle_history_exact_index_status(store)
    if not bool(index_status.get("ready")):
        _ORIGINAL_RECORD_HEARTBEAT(
            store,
            state="degraded",
            stage="cycle_history_exact_index_pending",
            error_type="CycleHistoryExactIndexUnavailable",
            detail={
                "progress": progress,
                "last_progress": progress,
                "progress_is_previous_durable_checkpoint": bool(progress),
                "previous_backfill_stage": previous_stage,
                "previous_backfill_error_type": previous_error,
                "index_status": index_status,
                "raw_history_queries_started": False,
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
        return INDEX_NOT_READY_EXIT_CODE

    original = base._record_heartbeat
    base._record_heartbeat = _record_heartbeat_preserving_progress
    try:
        return int(base.run_backfill_slice())
    finally:
        base._record_heartbeat = original


def main() -> int:
    try:
        return run_backfill_slice()
    except Exception as exc:
        try:
            settings = Settings.from_env()
            store = build_evidence_store(settings.evidence_db_path)
            if store is not None:
                _record_heartbeat_preserving_progress(
                    store,
                    state="degraded",
                    stage="backfill_repair_failed",
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "process_exit_reclaims_heap": True,
                    },
                )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "INDEX_NOT_READY_EXIT_CODE",
    "run_backfill_slice",
]
