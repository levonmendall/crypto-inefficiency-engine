from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

from inefficiency_engine import cycle_history_active_target_fallback_runtime as control_runtime
from inefficiency_engine import cycle_history_background_backfill as backfill
from inefficiency_engine import cycle_history_background_supervisor as supervisor
from inefficiency_engine import render_combined_postbind_lane_repair as entrypoint


def test_control_executor_uses_certified_cache_only(monkeypatch):
    monkeypatch.setenv("CIE_CONTROL_EXECUTOR_CYCLE_ID", "cycle-test")
    expected = {
        "complete": False,
        "error_type": "CycleHistoryBackgroundBackfillPending",
        "raw_history_queries_in_control": False,
    }
    monkeypatch.setattr(
        control_runtime,
        "_control_executor_read_only_progress",
        lambda _factory, _snapshot: dict(expected),
    )

    def forbidden_advance(*_args, **_kwargs):
        raise AssertionError("canonical control must not advance raw cycle history")

    monkeypatch.setattr(control_runtime, "_advance_and_pin", forbidden_advance)
    result = control_runtime.advance_durable_control_cycle_history_cache(
        SimpleNamespace(),
        SimpleNamespace(),
    )

    assert result == expected


def test_non_control_call_still_owns_normal_double_buffer_refresh(monkeypatch):
    monkeypatch.delenv("CIE_CONTROL_EXECUTOR_CYCLE_ID", raising=False)
    expected = {"complete": True, "serving_scan_id": "scan-a"}
    monkeypatch.setattr(
        control_runtime,
        "_advance_and_pin",
        lambda _factory, _snapshot, stop_at_monotonic=None: dict(expected),
    )

    result = control_runtime.advance_durable_control_cycle_history_cache(
        SimpleNamespace(),
        SimpleNamespace(),
    )

    assert result == expected


def test_background_bucket_gets_longer_but_finite_database_budget_and_bounded_batch():
    assert backfill._postgres_background_timeout_statements() == (
        "SET LOCAL statement_timeout = 60000",
        "SET LOCAL lock_timeout = 5000",
    )
    assert backfill.BACKGROUND_BUCKET_QUERY_CAP == 32
    assert backfill.BACKGROUND_BUCKET_STATEMENT_TIMEOUT_SECONDS == 60.0


def test_background_backfill_is_disposable_and_heavy_lease_serialized():
    source = inspect.getsource(backfill.run_backfill_slice)

    assert "HeavyWorkLeaseLedger" in source
    assert 'lease.next_sequence("cycle_history")' in source
    assert "advance_durable_control_cycle_history_cache" in source
    assert "_bounded_background_batch" in source
    assert '"provider_requests_allowed": False' not in source


def test_background_supervisor_has_external_deadline_and_fair_research_window():
    source = inspect.getsource(supervisor.run_cycle_history_background_supervisor)

    assert supervisor.BACKFILL_EXECUTOR_DEADLINE_SECONDS == 90.0
    assert supervisor.BACKFILL_SUCCESS_INTERVAL_SECONDS == 30.0
    assert "instance_memory_snapshot" in source
    assert "subprocess.Popen(BACKFILL_COMMAND)" in source
    assert "_terminate(child)" in source
    assert supervisor.BACKFILL_COMMAND[-1] == (
        "inefficiency_engine.cycle_history_background_backfill"
    )


def test_production_entrypoint_starts_history_and_projection_supervisors():
    source = inspect.getsource(entrypoint.main)

    assert "run_cycle_history_background_supervisor" in source
    assert 'name="cycle-history-background-supervisor"' in source
    assert "cycle_history_guard.start()" in source
    assert "cycle_history_guard.join" in source
    assert "run_research_projection_supervisor" in source
    assert 'name="research-projection-refresh-supervisor"' in source
    assert "research_projection_guard.start()" in source
    assert "research_projection_guard.join" in source


def test_health_exposes_cycle_history_background_worker():
    source = Path(
        "src/inefficiency_engine/read_api_bounded_heartbeat_deploy.py"
    ).read_text()

    assert (
        '"cycle_history_backfill": "cycle-history-background-backfill"'
        in source
    )
    assert '"cycle_history_backfill": 180.0' in source
