from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import asyncio

from inefficiency_engine.control_cycle_runtime import ControlExecutorSupervisor
from inefficiency_engine.canonical_control_plane_runtime import (
    refresh_canonical_control_plane,
)


def _python(source: str) -> list[str]:
    return [sys.executable, "-c", source]


def _status_source(*, final: str) -> str:
    return (
        "import json, os, pathlib; "
        "path=pathlib.Path(os.environ['CIE_CONTROL_EXECUTOR_STATUS_PATH']); "
        "path.write_text(json.dumps({'stage':'source_snapshot',"
        "'observed_at':'2026-08-24T01:00:00+00:00'})); "
        + final
    )


def test_hung_python_executor_is_killed_and_parent_stays_current(tmp_path: Path):
    heartbeats: list[dict[str, object]] = []
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.15,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )

    result = supervisor.run_cycle(
        sequence=1,
        command=_python(_status_source(final="import time; time.sleep(3600)")),
        heartbeat=heartbeats.append,
    )

    assert result.error_type == "ControlExecutorDeadlineExceeded"
    assert result.executor_terminated is True
    assert result.executor_runtime_seconds < 1.0
    assert result.executor_last_stage == "source_snapshot"
    assert len(heartbeats) >= 2
    assert {row["parent_sequence"] for row in heartbeats} == {1}
    assert all(row["parent_heartbeat_current"] is True for row in heartbeats)
    assert all(row["parent_generation"] == supervisor.parent_generation for row in heartbeats)
    try:
        os.kill(result.executor_pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("timed-out reconciliation executor remained alive")


def test_native_block_is_still_bounded_by_parent_process_deadline(tmp_path: Path):
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.15,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )

    result = supervisor.run_cycle(
        sequence=1,
        command=_python(
            _status_source(final="import ctypes; ctypes.CDLL(None).pause()")
        ),
    )

    assert result.error_type == "ControlExecutorDeadlineExceeded"
    assert result.executor_runtime_seconds < 1.0
    assert result.executor_last_stage == "source_snapshot"


def test_nonzero_executor_exit_is_explicit_and_next_cycle_advances(tmp_path: Path):
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.5,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )
    first = supervisor.run_cycle(
        sequence=7,
        command=_python(_status_source(final="raise SystemExit(19)")),
    )

    result_source = (
        "import json, os, pathlib; "
        "status=pathlib.Path(os.environ['CIE_CONTROL_EXECUTOR_STATUS_PATH']); "
        "status.write_text(json.dumps({'stage':'control_executor_complete'})); "
        "result=pathlib.Path(os.environ['CIE_CONTROL_EXECUTOR_RESULT_PATH']); "
        "result.write_text(json.dumps({'ok':True,'control':{"
        "'operating_reconciliation_complete':True,"
        "'qualified_bridge_publication_complete':True,"
        "'research_projection_publication_complete':True}}))"
    )
    second = supervisor.run_cycle(
        sequence=8,
        command=_python(result_source),
    )

    assert first.error_type == "ControlExecutorExitedNonzero"
    assert first.return_code == 19
    assert first.retry_count == 1
    assert second.ok is True
    assert second.payload["control"]["operating_reconciliation_complete"] is True
    assert first.parent_generation == second.parent_generation
    assert first.parent_sequence == 7
    assert second.parent_sequence == 8
    assert supervisor.retry_count == 0


def test_executor_timeout_reports_last_exact_substage(tmp_path: Path):
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.15,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )

    result = supervisor.run_cycle(
        sequence=3,
        command=_python(_status_source(final="import time; time.sleep(3600)")),
    )

    payload = result.telemetry()
    assert payload["executor_last_stage_before_failure"] == "source_snapshot"
    assert payload["last_executor_error_type"] == "ControlExecutorDeadlineExceeded"
    assert payload["executor_deadline_seconds"] == 0.15
    assert payload["provider_requests_allowed"] is False
    assert payload["provider_requests_used"] == 0
    assert payload["paper_only"] is True


def test_sigterm_resistant_executor_is_sigkilled_without_orphan(tmp_path: Path):
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.15,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )
    result = supervisor.run_cycle(
        sequence=4,
        command=_python(
            _status_source(
                final=(
                    "import signal, time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(3600)"
                )
            )
        ),
    )

    assert result.executor_terminated is True
    assert result.executor_killed is True
    assert result.error_type == "ControlExecutorDeadlineExceeded"
    with __import__("pytest").raises(ProcessLookupError):
        os.kill(result.executor_pid, 0)


def test_partial_historical_cache_blocks_bridge_and_projection():
    calls: list[str] = []

    class Operating:
        def reconcile_latest_runtime_truth(self, *, stage_reporter=None):
            calls.append("operating")
            return SimpleNamespace(snapshot_id="operating-1", observed_at=SimpleNamespace(
                isoformat=lambda: "2026-08-24T01:00:00+00:00"
            ))

    class Bridge:
        _latest_scan = None
        allocator = SimpleNamespace(alpha_factory=None)

        async def publish_latest(self, **_kwargs):
            calls.append("bridge")

    class Projection:
        def publish(self, **_kwargs):
            calls.append("projection")

    settings = SimpleNamespace(
        alpha_min_forward_samples=30,
        operating_certification_min_settled_trials=20,
        shadow_horizons_seconds=(60.0,),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=1,
        worker_heartbeat_stale_seconds=180.0,
    )
    result = asyncio.run(
        refresh_canonical_control_plane(
            store=SimpleNamespace(),
            operating_certification=Operating(),
            qualified_bridge=Bridge(),
            research_projection=Projection(),
            settings=settings,
            historical_cache_status=lambda: {
                "complete": False,
                "strategy": {"processed_tail": 100, "target_tail": 200},
            },
        )
    )

    assert calls == ["operating"]
    assert result["operating_reconciliation_complete"] is True
    assert result["qualified_bridge_publication_complete"] is False
    assert result["research_projection_publication_complete"] is False
    assert result["historical_cache_complete"] is False
    assert result["control_plane_errors"] == {
        "historical_evidence_cache": "HistoricalEvidenceCacheRebuilding"
    }
