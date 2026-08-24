from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine import durable_source_coverage_runtime as runtime
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.permanent_source_plane import PermanentSourcePlane
from inefficiency_engine.source_coverage import SourceCoveragePlane


def test_handoff_freshness_uses_publication_time_not_calculation_time(tmp_path):
    store = EvidenceStore(tmp_path / "source-handoff-publication.sqlite")
    calculated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    snapshot = SourceCoveragePlane(store).snapshot(now=calculated_at)

    assert runtime.persist_source_coverage_snapshot(store, snapshot) is True
    heartbeat = store.latest_worker_heartbeat(runtime.SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
    assert heartbeat is not None

    loaded = runtime.load_persisted_source_coverage_snapshot(
        store,
        now=heartbeat.observed_at + timedelta(seconds=30),
        max_age_seconds=90.0,
    )

    assert loaded.observed_at == snapshot.observed_at


def test_handoff_still_fails_closed_when_publication_itself_is_stale(tmp_path):
    store = EvidenceStore(tmp_path / "source-handoff-stale.sqlite")
    snapshot = SourceCoveragePlane(store).snapshot()
    assert runtime.persist_source_coverage_snapshot(store, snapshot) is True
    heartbeat = store.latest_worker_heartbeat(runtime.SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
    assert heartbeat is not None

    with pytest.raises(runtime.DurableSourceCoverageSnapshotStale):
        runtime.load_persisted_source_coverage_snapshot(
            store,
            now=heartbeat.observed_at + timedelta(seconds=91),
            max_age_seconds=90.0,
        )


class _HungProcess:
    pid = 43210

    def __init__(self):
        self.returncode = None
        self.killed = False
        self.terminated = False
        self._done = asyncio.Event()

    async def wait(self):
        await self._done.wait()
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.returncode = -9
        self._done.set()


@pytest.mark.asyncio
async def test_snapshot_refresh_timeout_kills_disposable_executor(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "source-refresh-timeout.sqlite")
    process = _HungProcess()

    async def process_factory(*args, **kwargs):
        return process

    monkeypatch.setattr(runtime, "source_coverage_terminate_grace_seconds", lambda: 0.01)
    result = await runtime._run_one_source_coverage_refresh(
        store,
        sequence=7,
        deadline_seconds=0.01,
        process_factory=process_factory,
    )

    assert result["ok"] is False
    assert result["error_type"] == "SourceCoverageSnapshotRefreshDeadlineExceeded"
    assert result["executor_terminated"] is True
    assert result["executor_killed"] is True
    assert process.terminated is True
    assert process.killed is True

    heartbeat = store.latest_worker_heartbeat(runtime.SOURCE_COVERAGE_REFRESH_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "degraded"
    assert heartbeat.error_type == "SourceCoverageSnapshotRefreshDeadlineExceeded"


def test_permanent_market_refresh_starts_independent_snapshot_cadence():
    runtime.install_source_coverage_snapshot_publisher_runtime()

    source = inspect.getsource(PermanentSourcePlane.refresh_market_l2_snapshot)
    assert "_ensure_source_coverage_snapshot_refresh_loop" in source

    refresh_source = inspect.getsource(runtime._run_one_source_coverage_refresh)
    assert "asyncio.create_subprocess_exec" in refresh_source
    assert "asyncio.wait_for" in refresh_source
    assert "source_coverage_snapshot_executor" in refresh_source


def test_snapshot_executor_contains_no_provider_network_collection():
    from inefficiency_engine import source_coverage_snapshot_executor

    source = inspect.getsource(source_coverage_snapshot_executor.main)
    assert "SourceCoveragePlane(store).snapshot()" in source
    assert "collect_" not in source
    assert "http" not in source.lower()
