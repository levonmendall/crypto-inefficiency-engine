from __future__ import annotations

import os
from typing import Any


SOURCE_COVERAGE_EXECUTOR_WORKER_ID = "canonical-source-coverage-executor"
SOURCE_COVERAGE_HISTORY_WORKER_ID = "canonical-source-coverage-history"


def _record_executor_stage(
    store: Any,
    *,
    stage: str,
    state: str = "running",
    error_type: str | None = None,
    message: str | None = None,
    snapshot_observed_at: str | None = None,
    stage_timings_seconds: dict[str, float] | None = None,
) -> None:
    """Persist exact child progress so a parent-side kill still leaves the last stage."""

    detail: dict[str, object] = {
        "executor_pid": os.getpid(),
        "parent_sequence": os.getenv("CIE_SOURCE_COVERAGE_REFRESH_SEQUENCE"),
        "stage": stage,
        "stage_timings_seconds": dict(stage_timings_seconds or {}),
        "provider_requests_allowed": False,
        "provider_requests_used": 0,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    if message:
        detail["message"] = message[:1000]
    if snapshot_observed_at:
        detail["snapshot_observed_at"] = snapshot_observed_at
    try:
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_EXECUTOR_WORKER_ID,
            state=state,
            error_type=error_type,
            detail=detail,
        )
    except Exception:
        pass


def _record_history_stage(
    store: Any,
    *,
    state: str,
    detail: dict[str, object],
    error_type: str | None = None,
) -> None:
    try:
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_HISTORY_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={
                **detail,
                "canonical_history_authority": "source_coverage_snapshot_archive",
                "candidate_level_history_synthesized": False,
                "historical_counts_as_forward": False,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def main() -> int:
    """Compute one canonical source snapshot and persist its current history row.

    The old heartbeat archive is deliberately *not* migrated in this child. The source
    snapshot executor has a hard external deadline and must spend that budget on current
    source truth. Historical archive migration is owned by an independent bounded
    supervisor so a slow current snapshot can never strand the historical checkpoint.
    """

    from inefficiency_engine.disposable_executor_memory_guard import (
        MEMORY_PRESSURE_EXIT_CODE,
        DisposableMemoryAdmissionDeferred,
        disposable_executor_memory_guard,
    )

    memory_guard_cm = disposable_executor_memory_guard(
        "source-coverage-snapshot-executor"
    )
    try:
        memory_guard_cm.__enter__()
    except DisposableMemoryAdmissionDeferred as exc:
        print(
            "source-coverage-snapshot-executor memory admission deferred: "
            f"{exc}; memory={exc.snapshot.as_dict()}",
            flush=True,
        )
        return MEMORY_PRESSURE_EXIT_CODE

    from inefficiency_engine.config import Settings
    from inefficiency_engine.durable_source_coverage_runtime import (
        SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    )
    from inefficiency_engine.evidence import build_evidence_store
    from inefficiency_engine.operational_source_probe_runtime import (
        install_current_source_scan_probe_runtime,
    )
    from inefficiency_engine.source_coverage import SourceCoveragePlane
    from inefficiency_engine.source_coverage_history import (
        SourceCoverageHistoryLedger,
        persist_source_coverage_history_snapshot,
    )
    from inefficiency_engine.source_coverage_snapshot_stage_runtime import (
        SourceCoverageSnapshotStageProfiler,
        profile_source_coverage_snapshot,
    )
    from inefficiency_engine.source_runtime_safety import (
        install_source_coverage_reconciliation_runtime,
    )

    install_source_coverage_reconciliation_runtime()
    # The reconciliation runtime bounds source/provider/admission reads. Install the
    # current-scan table projection afterwards so market/funding/L2 probes never fall
    # back to sorting the append-only historical evidence tables.
    install_current_source_scan_probe_runtime()
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("source coverage snapshot executor requires durable persistence")

    _record_executor_stage(store, stage="store_open")
    profiler: SourceCoverageSnapshotStageProfiler | None = None

    def report_stage(stage: str, timings: dict[str, float]) -> None:
        _record_executor_stage(
            store,
            stage=stage,
            stage_timings_seconds=timings,
        )

    try:
        with profile_source_coverage_snapshot(report_stage) as active_profiler:
            profiler = active_profiler
            snapshot = SourceCoveragePlane(store).snapshot()
    except Exception as exc:
        failed_stage = (
            profiler.current_stage
            if profiler is not None and profiler.current_stage
            else "snapshot_compute_and_persist"
        )
        _record_executor_stage(
            store,
            stage=f"{failed_stage}_failed",
            state="degraded",
            error_type=type(exc).__name__,
            message=str(exc),
            stage_timings_seconds=(
                profiler.timings() if profiler is not None else None
            ),
        )
        raise

    timings = profiler.timings() if profiler is not None else {}
    _record_executor_stage(
        store,
        stage="publication_verify",
        snapshot_observed_at=snapshot.observed_at.isoformat(),
        stage_timings_seconds=timings,
    )
    heartbeat = store.latest_worker_heartbeat(SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
    if heartbeat is None:
        message = "source coverage snapshot publication was not persisted"
        _record_executor_stage(
            store,
            stage="publication_verify_failed",
            state="degraded",
            error_type="SourceCoverageSnapshotPublicationMissing",
            message=message,
            snapshot_observed_at=snapshot.observed_at.isoformat(),
            stage_timings_seconds=timings,
        )
        raise RuntimeError(message)
    detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
    if detail.get("snapshot_observed_at") != snapshot.observed_at.isoformat():
        message = "source coverage snapshot publication does not match calculation"
        _record_executor_stage(
            store,
            stage="publication_verify_failed",
            state="degraded",
            error_type="SourceCoverageSnapshotPublicationMismatch",
            message=message,
            snapshot_observed_at=snapshot.observed_at.isoformat(),
            stage_timings_seconds=timings,
        )
        raise RuntimeError(message)

    # Every new canonical snapshot is still dual-written immediately. Only migration of
    # *older* heartbeat rows moved out of this deadline-constrained process.
    try:
        inserted_current = persist_source_coverage_history_snapshot(
            store,
            snapshot,
            published_at=heartbeat.observed_at,
        )
        migration = SourceCoverageHistoryLedger(store).migration_status()
        migration_complete = bool(migration.get("complete"))
        _record_history_stage(
            store,
            state="success" if migration_complete else "running",
            detail={
                "stage": (
                    "canonical_history_ready"
                    if migration_complete
                    else "live_snapshot_persisted_archive_migration_pending"
                ),
                "snapshot_observed_at": snapshot.observed_at.isoformat(),
                "inserted_current_lane_snapshots": inserted_current,
                "archive_migration_owner": "source-coverage-history-migration-supervisor",
                **migration,
            },
        )
    except Exception as exc:
        _record_history_stage(
            store,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "stage": "canonical_history_current_snapshot_persistence_failed",
                "snapshot_observed_at": snapshot.observed_at.isoformat(),
                "message": str(exc)[:1000],
                "retrying": True,
                "archive_migration_owner": "source-coverage-history-migration-supervisor",
            },
        )

    _record_executor_stage(
        store,
        stage="executor_complete",
        state="success",
        snapshot_observed_at=snapshot.observed_at.isoformat(),
        stage_timings_seconds=timings,
    )
    memory_guard_cm.__exit__(None, None, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
