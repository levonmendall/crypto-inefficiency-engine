from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine import render_combined_runtime as runtime
from inefficiency_engine import runtime_watchdog_readiness_repair as watchdog_repair
from inefficiency_engine import source_coverage_history_migration_supervisor as history_supervisor
from inefficiency_engine.read_api_liveness_deploy import liveness_payload


class _NoSleepStop:
    def is_set(self) -> bool:
        return False

    def wait(self, _seconds: float) -> bool:
        return False


class _FakeStore:
    def __init__(self, *, latest=None):
        self.latest = latest
        self.records: list[dict[str, object]] = []

    def latest_worker_heartbeat(self, _worker_id: str):
        return self.latest

    def record_worker_heartbeat(self, **kwargs):
        self.records.append(dict(kwargs))


def test_internal_watchdog_reads_readiness_without_changing_other_local_paths(monkeypatch):
    calls: list[tuple[str | int, str]] = []

    def fake_local_json(port: str | int, path: str):
        calls.append((port, path))
        return {"path": path}

    monkeypatch.setattr(runtime, "_local_json", fake_local_json)
    monkeypatch.setattr(runtime, watchdog_repair._PATCH_MARKER, False, raising=False)

    watchdog_repair.install_runtime_watchdog_readiness_repair()

    assert runtime._local_json(10000, "/health") == {"path": "/ready"}
    assert runtime._local_json(10000, "/v3/dashboard/snapshot") == {
        "path": "/v3/dashboard/snapshot"
    }
    assert calls == [
        (10000, "/ready"),
        (10000, "/v3/dashboard/snapshot"),
    ]


def test_public_health_contract_remains_database_independent():
    payload = liveness_payload()

    assert payload["liveness_database_independent"] is True
    assert payload["database_check"] == "deferred_to_readiness"
    assert payload["runtime_diagnostics"] == "deferred_to_readiness"
    assert payload["readiness_endpoint"] == "/ready"
    assert "runtime_heartbeats" not in payload


def test_source_history_supervisor_publishes_timeout_truth_and_retries(monkeypatch):
    store = _FakeStore()
    results = [(-9, True), (0, False)]

    monkeypatch.setattr(history_supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(history_supervisor, "_supervisor_store", lambda: store)
    monkeypatch.setattr(
        history_supervisor,
        "_run_bounded_child",
        lambda _stop_event: results.pop(0),
    )

    history_supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert not results
    assert len(store.records) == 1
    heartbeat = store.records[0]
    assert heartbeat["state"] == "degraded"
    assert heartbeat["error_type"] == "SourceCoverageHistoryMigrationChildDeadlineExceeded"
    detail = heartbeat["detail"]
    assert detail["stage"] == "canonical_history_archive_migration_child_timed_out"
    assert detail["attempt_number"] == 1
    assert detail["executor_deadline_seconds"] == 30.0
    assert detail["child_return_code"] == -9
    assert detail["child_timed_out"] is True
    assert detail["termination_signal"] == "SIGKILL"
    assert detail["possible_oom_or_external_kill"] is True
    assert detail["oom_kill_proven"] is False
    assert detail["retrying"] is True
    assert detail["supervisor_executes_migration"] is False
    assert detail["qualification_thresholds_unchanged"] is True
    assert detail["allocation_authority"] is False
    assert detail["live_execution_authority"] is False
    assert detail["paper_only"] is True


def test_source_history_supervisor_survives_child_launch_exception(monkeypatch):
    store = _FakeStore()
    calls = 0

    def fake_run(_stop_event):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("spawn failed")
        return 0, False

    monkeypatch.setattr(history_supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(history_supervisor, "_supervisor_store", lambda: store)
    monkeypatch.setattr(history_supervisor, "_run_bounded_child", fake_run)

    history_supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert calls == 2
    assert len(store.records) == 1
    heartbeat = store.records[0]
    assert heartbeat["error_type"] == "RuntimeError"
    assert heartbeat["detail"]["attempt_number"] == 1
    assert heartbeat["detail"]["retrying"] is True


def test_source_history_supervisor_preserves_current_child_exception(monkeypatch):
    store = _FakeStore(
        latest=SimpleNamespace(
            state="degraded",
            observed_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
    )
    results = [(1, False), (0, False)]

    monkeypatch.setattr(history_supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(history_supervisor, "_supervisor_store", lambda: store)
    monkeypatch.setattr(
        history_supervisor,
        "_run_bounded_child",
        lambda _stop_event: results.pop(0),
    )

    history_supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert not results
    assert store.records == []
