from __future__ import annotations

import os
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import event

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_bucket_timeout_runtime import (
    install_index_aligned_cycle_history_bucket_runtime,
)
from inefficiency_engine.durable_control_cache import ensure_durable_control_cache_schema
from inefficiency_engine.durable_control_cycle_history import (
    ensure_durable_control_cycle_history_schema,
)
from inefficiency_engine.durable_control_cycle_history_target_runtime import (
    advance_durable_control_cycle_history_cache,
)
from inefficiency_engine.durable_source_coverage_runtime import (
    install_control_source_coverage_snapshot_reader_runtime,
)
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.heavy_work_lease import HeavyWorkLeaseLedger, HeavyWorkLeaseUnavailable
from inefficiency_engine.instance_memory import instance_memory_snapshot
from inefficiency_engine.permanent_control_worker import _build_control_services
from inefficiency_engine.source_runtime_safety import (
    install_source_coverage_reconciliation_runtime,
)


WORKER_ID = "cycle-history-background-backfill"
TEMPORARY_ADMISSION_EXIT_CODE = 75
INCOMPLETE_PROGRESS_EXIT_CODE = 76
BACKGROUND_BUCKET_STATEMENT_TIMEOUT_SECONDS = 60.0
BACKGROUND_BUCKET_LOCK_TIMEOUT_SECONDS = 5.0
# The child is already externally bounded at 90 seconds and exits after every slice.
# Use the target runtime's maximum finite work window during first-target bootstrap so
# the exact 180-day target cannot take hours merely because each process defaults to an
# eight-second slice. History length, rows/day, filters, qualification and authority are
# unchanged.
BACKGROUND_TARGET_TIME_BUDGET_SECONDS = 15.0
BACKGROUND_BUCKET_QUERY_CAP = 32
_BUCKET_QUERY_BUDGET_ENV = "CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET"
_TIME_BUDGET_ENV = "CIE_CONTROL_CYCLE_HISTORY_TIME_BUDGET_SECONDS"


def _record_heartbeat(
    store: Any,
    *,
    state: str,
    stage: str,
    sequence: int | None = None,
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
                "sequence": sequence,
                "background_maintenance_only": True,
                "disposable_process": True,
                "heavy_work_lease_serialized": True,
                "statement_timeout_seconds": BACKGROUND_BUCKET_STATEMENT_TIMEOUT_SECONDS,
                "lock_timeout_seconds": BACKGROUND_BUCKET_LOCK_TIMEOUT_SECONDS,
                "bucket_query_cap": BACKGROUND_BUCKET_QUERY_CAP,
                "target_time_budget_seconds": BACKGROUND_TARGET_TIME_BUDGET_SECONDS,
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "qualification_thresholds_unchanged": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
                **(detail or {}),
            },
        )
    except Exception:
        pass


def _postgres_background_timeout_statements() -> tuple[str, str]:
    statement_ms = max(1, int(BACKGROUND_BUCKET_STATEMENT_TIMEOUT_SECONDS * 1000.0))
    lock_ms = max(1, int(BACKGROUND_BUCKET_LOCK_TIMEOUT_SECONDS * 1000.0))
    return (
        f"SET LOCAL statement_timeout = {statement_ms}",
        f"SET LOCAL lock_timeout = {lock_ms}",
    )


@contextmanager
def background_cycle_history_database_timeout(store: Any) -> Iterator[None]:
    """Give each background history query a long but finite PostgreSQL budget.

    This scope is intentionally separate from canonical control's short query budget.
    The backfill child has no allocation or execution authority and is disposable, so
    it may spend longer proving exact historical buckets while canonical control stays
    free of raw-ledger reconstruction.
    """

    engine = store.engine
    if str(getattr(engine.dialect, "name", "")) != "postgresql":
        yield
        return

    statements = _postgres_background_timeout_statements()

    def apply_timeout(connection: Any) -> None:
        for statement in statements:
            connection.exec_driver_sql(statement)

    event.listen(engine, "begin", apply_timeout)
    try:
        yield
    finally:
        event.remove(engine, "begin", apply_timeout)


@contextmanager
def _bounded_background_batch() -> Iterator[None]:
    previous_query_budget = os.environ.get(_BUCKET_QUERY_BUDGET_ENV)
    previous_time_budget = os.environ.get(_TIME_BUDGET_ENV)
    os.environ[_BUCKET_QUERY_BUDGET_ENV] = str(BACKGROUND_BUCKET_QUERY_CAP)
    os.environ[_TIME_BUDGET_ENV] = str(BACKGROUND_TARGET_TIME_BUDGET_SECONDS)
    try:
        yield
    finally:
        if previous_query_budget is None:
            os.environ.pop(_BUCKET_QUERY_BUDGET_ENV, None)
        else:
            os.environ[_BUCKET_QUERY_BUDGET_ENV] = previous_query_budget
        if previous_time_budget is None:
            os.environ.pop(_TIME_BUDGET_ENV, None)
        else:
            os.environ[_TIME_BUDGET_ENV] = previous_time_budget


def _bounded_progress(progress: dict[str, object]) -> dict[str, object]:
    fields = (
        "complete",
        "working_complete",
        "rolling_refresh_in_progress",
        "promoted_working_target",
        "serving_scan_id",
        "serving_target_completed_at",
        "working_target_scan_id",
        "working_target_completed_at",
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
        "elapsed_seconds",
        "time_budget_seconds",
        "stopped_for_time_budget",
        "durable_checkpoint_persisted",
        "error_type",
        "message",
    )
    return {key: progress.get(key) for key in fields if key in progress}


def run_backfill_slice() -> int:
    """Advance one bounded batch of an exact frozen cycle-history target.

    Exit 76 means the durable checkpoint advanced but no certified serving target exists
    yet. The lightweight supervisor treats that as healthy bootstrap progress and starts
    another bounded child promptly. Exit 0 means a certified active target is available,
    after which the normal fair-share refresh cadence resumes.
    """

    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("cycle-history background backfill requires durable persistence")

    owner = f"cycle-history:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    lease = HeavyWorkLeaseLedger(store)
    try:
        lease_context = lease.lease(owner)
        lease_context.__enter__()
    except HeavyWorkLeaseUnavailable as exc:
        _record_heartbeat(
            store,
            state="degraded",
            stage="heavy_work_lease_deferred",
            error_type=type(exc).__name__,
            detail={"owner": owner, "temporary_admission_failure": True},
        )
        return TEMPORARY_ADMISSION_EXIT_CODE

    try:
        memory = instance_memory_snapshot()
        if bool(getattr(memory, "terminate_required", False)):
            _record_heartbeat(
                store,
                state="degraded",
                stage="memory_admission_deferred",
                error_type="InstanceMemoryTerminateBlocked",
                detail={
                    "owner": owner,
                    "memory": memory.as_dict(),
                    "temporary_admission_failure": True,
                },
            )
            return TEMPORARY_ADMISSION_EXIT_CODE

        sequence = lease.next_sequence("cycle_history")
        _record_heartbeat(
            store,
            state="running",
            stage="backfill_batch_starting",
            sequence=sequence,
            detail={"owner": owner, "memory_before": memory.as_dict()},
        )

        # Match canonical control's source-view semantics without performing provider
        # requests. The background worker consumes the already-persisted complete source
        # snapshot and only maintains the compact historical projection.
        install_source_coverage_reconciliation_runtime()
        install_control_source_coverage_snapshot_reader_runtime()
        ensure_durable_control_cache_schema(store)
        ensure_durable_control_cycle_history_schema(store)
        install_index_aligned_cycle_history_bucket_runtime()

        _operating, qualified_bridge, _research_projection = _build_control_services(
            settings,
            store,
        )
        alpha_factory = getattr(qualified_bridge.allocator, "alpha_factory", None)
        if alpha_factory is None:
            _record_heartbeat(
                store,
                state="degraded",
                stage="alpha_factory_unavailable",
                sequence=sequence,
                error_type="CycleHistoryAlphaFactoryUnavailable",
                detail={"owner": owner},
            )
            return 1

        source_snapshot = qualified_bridge._latest_scan()
        if source_snapshot is None:
            _record_heartbeat(
                store,
                state="degraded",
                stage="source_snapshot_unavailable",
                sequence=sequence,
                error_type="CycleHistorySourceSnapshotUnavailable",
                detail={"owner": owner},
            )
            return 1

        started = time.monotonic()
        with background_cycle_history_database_timeout(store), _bounded_background_batch():
            progress = dict(
                advance_durable_control_cycle_history_cache(
                    alpha_factory,
                    source_snapshot,
                )
            )
        elapsed = max(0.0, time.monotonic() - started)
        bounded = _bounded_progress(progress)
        complete = bool(progress.get("complete"))
        checkpointed = bool(progress.get("durable_checkpoint_persisted"))
        _record_heartbeat(
            store,
            state="success",
            stage=(
                "certified_target_available"
                if complete
                else "backfill_batch_checkpointed"
            ),
            sequence=sequence,
            detail={
                "owner": owner,
                "source_scan_id": str(source_snapshot.scan_id),
                "slice_runtime_seconds": elapsed,
                "cache_complete": complete,
                "first_certified_target_pending": not complete,
                "durable_progress_checkpointed": checkpointed,
                "progress": bounded,
                "batched_bucket_queries": True,
                "bootstrap_retry_without_fair_share_delay": not complete,
                "process_exit_reclaims_heap": True,
            },
        )
        if complete:
            return 0
        if checkpointed:
            return INCOMPLETE_PROGRESS_EXIT_CODE
        return 1
    except Exception as exc:
        _record_heartbeat(
            store,
            state="degraded",
            stage="backfill_batch_failed",
            error_type=type(exc).__name__,
            detail={
                "owner": owner,
                "message": str(exc)[:500],
                "process_exit_reclaims_heap": True,
            },
        )
        return 1
    finally:
        lease_context.__exit__(None, None, None)


def main() -> int:
    return run_backfill_slice()


if __name__ == "__main__":
    raise SystemExit(main())
