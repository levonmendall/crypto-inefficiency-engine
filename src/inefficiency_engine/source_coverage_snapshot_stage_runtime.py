from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator


StageCallback = Callable[[str, dict[str, float]], None]


class SourceCoverageSnapshotStageProfiler:
    """Process-local profiler for one disposable source-coverage calculation.

    The profiler changes observability only. It never changes source eligibility,
    freshness, redundancy, qualification, or investment authority.
    """

    def __init__(self, callback: StageCallback):
        self.callback = callback
        self.current_stage: str | None = None
        self.current_started_monotonic: float | None = None
        self.completed_timings_seconds: dict[str, float] = {}

    def _close_current(self, now: float) -> None:
        if self.current_stage is None or self.current_started_monotonic is None:
            return
        elapsed = max(0.0, now - self.current_started_monotonic)
        self.completed_timings_seconds[self.current_stage] = (
            self.completed_timings_seconds.get(self.current_stage, 0.0) + elapsed
        )

    def enter(self, stage: str) -> None:
        normalized = str(stage)
        if normalized == self.current_stage:
            return
        now = time.monotonic()
        self._close_current(now)
        self.current_stage = normalized
        self.current_started_monotonic = now
        try:
            self.callback(normalized, dict(self.completed_timings_seconds))
        except Exception:
            # Diagnostics must never change source-coverage semantics.
            pass

    def timings(self, *, include_current: bool = True) -> dict[str, float]:
        result = dict(self.completed_timings_seconds)
        if (
            include_current
            and self.current_stage is not None
            and self.current_started_monotonic is not None
        ):
            result[self.current_stage] = result.get(self.current_stage, 0.0) + max(
                0.0,
                time.monotonic() - self.current_started_monotonic,
            )
        return result


def _table_candidate_stage(spec: dict[str, object]) -> str:
    probe = spec.get("table")
    if not isinstance(probe, tuple) or len(probe) != 3:
        return "table_candidate"
    table_name, column, value = probe
    parts = ["table_candidate", str(table_name)]
    if column is not None:
        parts.append(str(column))
    if value is not None:
        parts.append(str(value))
    return ":".join(part.replace(" ", "_") for part in parts)


@contextmanager
def profile_source_coverage_snapshot(
    callback: StageCallback,
) -> Iterator[SourceCoverageSnapshotStageProfiler]:
    """Expose exact internal source-snapshot boundaries in the disposable executor.

    The production source-coverage implementation is already patched with bounded
    latest-state reads before this context is entered. This wrapper times those exact
    production calls and restores every patched symbol when the calculation exits.
    """

    import inefficiency_engine.durable_source_coverage_runtime as durable_runtime
    import inefficiency_engine.source_coverage as source_coverage

    profiler = SourceCoverageSnapshotStageProfiler(callback)

    original_inspect = source_coverage.inspect
    original_ledger_latest = source_coverage.SourceCoverageLedger.latest
    original_provider_rows = source_coverage.SourceCoveragePlane._provider_rows
    original_admissions = source_coverage.SourceCoveragePlane._admissions
    original_source_status = source_coverage.SourceCoveragePlane._source_status
    original_table_candidate = source_coverage.SourceCoveragePlane._table_candidate
    original_dynamic_lane_priority = source_coverage.dynamic_lane_priority
    original_snapshot = source_coverage.SourceCoveragePlane.snapshot
    original_persist = durable_runtime.persist_source_coverage_snapshot

    def inspect_profiled(bind: Any):
        profiler.enter("table_discovery")
        return original_inspect(bind)

    def ledger_latest_profiled(self, *args: object, **kwargs: object):
        profiler.enter("source_observation_latest")
        return original_ledger_latest(self, *args, **kwargs)

    def provider_rows_profiled(self, *args: object, **kwargs: object):
        profiler.enter("provider_status_latest")
        return original_provider_rows(self, *args, **kwargs)

    def admissions_profiled(self, *args: object, **kwargs: object):
        profiler.enter("admission_latest")
        return original_admissions(self, *args, **kwargs)

    def source_status_profiled(self, *args: object, **kwargs: object):
        profiler.enter("lane_reconciliation")
        return original_source_status(self, *args, **kwargs)

    def table_candidate_profiled(
        self,
        spec: dict[str, object],
        available: set[str],
    ):
        probe = spec.get("table")
        cache = getattr(self, "_cie_snapshot_table_candidate_cache", None)
        key = tuple(probe) if isinstance(probe, tuple) else None
        cache_hit = bool(isinstance(cache, dict) and key is not None and key in cache)
        if not cache_hit:
            profiler.enter(_table_candidate_stage(spec))
        try:
            return original_table_candidate(self, spec, available)
        finally:
            if not cache_hit:
                profiler.enter("lane_reconciliation")

    def dynamic_lane_priority_profiled(store: Any, lanes: Any):
        profiler.enter("dynamic_priority")
        return original_dynamic_lane_priority(store, lanes)

    def persist_profiled(store: Any, snapshot: Any) -> bool:
        profiler.enter("snapshot_persist")
        return original_persist(store, snapshot)

    def snapshot_profiled(self, *args: object, **kwargs: object):
        profiler.enter("snapshot_start")
        result = original_snapshot(self, *args, **kwargs)
        profiler.enter("snapshot_complete")
        return result

    source_coverage.inspect = inspect_profiled  # type: ignore[assignment]
    source_coverage.SourceCoverageLedger.latest = ledger_latest_profiled  # type: ignore[method-assign]
    source_coverage.SourceCoveragePlane._provider_rows = provider_rows_profiled  # type: ignore[method-assign]
    source_coverage.SourceCoveragePlane._admissions = admissions_profiled  # type: ignore[method-assign]
    source_coverage.SourceCoveragePlane._source_status = source_status_profiled  # type: ignore[method-assign]
    source_coverage.SourceCoveragePlane._table_candidate = table_candidate_profiled  # type: ignore[method-assign]
    source_coverage.dynamic_lane_priority = dynamic_lane_priority_profiled  # type: ignore[assignment]
    source_coverage.SourceCoveragePlane.snapshot = snapshot_profiled  # type: ignore[method-assign]
    durable_runtime.persist_source_coverage_snapshot = persist_profiled  # type: ignore[assignment]

    try:
        yield profiler
    finally:
        source_coverage.inspect = original_inspect  # type: ignore[assignment]
        source_coverage.SourceCoverageLedger.latest = original_ledger_latest  # type: ignore[method-assign]
        source_coverage.SourceCoveragePlane._provider_rows = original_provider_rows  # type: ignore[method-assign]
        source_coverage.SourceCoveragePlane._admissions = original_admissions  # type: ignore[method-assign]
        source_coverage.SourceCoveragePlane._source_status = original_source_status  # type: ignore[method-assign]
        source_coverage.SourceCoveragePlane._table_candidate = original_table_candidate  # type: ignore[method-assign]
        source_coverage.dynamic_lane_priority = original_dynamic_lane_priority  # type: ignore[assignment]
        source_coverage.SourceCoveragePlane.snapshot = original_snapshot  # type: ignore[method-assign]
        durable_runtime.persist_source_coverage_snapshot = original_persist  # type: ignore[assignment]
