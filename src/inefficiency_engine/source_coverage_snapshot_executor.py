from __future__ import annotations

import os
from typing import Any


SOURCE_COVERAGE_EXECUTOR_WORKER_ID = "canonical-source-coverage-executor"


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


def main() -> int:
    """Compute and persist exactly one source-coverage snapshot without provider calls."""

    from inefficiency_engine.config import Settings
    from inefficiency_engine.durable_source_coverage_runtime import (
        SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    )
    from inefficiency_engine.evidence import build_evidence_store
    from inefficiency_engine.source_coverage import SourceCoveragePlane
    from inefficiency_engine.source_coverage_snapshot_stage_runtime import (
        SourceCoverageSnapshotStageProfiler,
        profile_source_coverage_snapshot,
    )
    from inefficiency_engine.source_runtime_safety import (
        install_source_coverage_reconciliation_runtime,
    )

    install_source_coverage_reconciliation_runtime()
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

    _record_executor_stage(
        store,
        stage="executor_complete",
        state="success",
        snapshot_observed_at=snapshot.observed_at.isoformat(),
        stage_timings_seconds=timings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
