from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine import combined_runtime_parent_heartbeat as parent_heartbeat
from inefficiency_engine import render_combined_runtime as runtime
from inefficiency_engine import runtime_watchdog_readiness_repair as watchdog_repair
from inefficiency_engine import source_coverage_history_migration_supervisor as history_supervisor
from inefficiency_engine import source_history_supervisor_diagnostic_child as history_diagnostic
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


def test_internal_watchdog_reads_bounded_heartbeat_endpoint_without_changing_other_paths(
    monkeypatch,
):
    calls: list[tuple[str | int, str]] = []

    def fake_local_json(port: str | int, path: str):
        calls.append((port, path))
        return {"path": path}

    monkeypatch.setattr(runtime, "_local_json", fake_local_json)
    monkeypatch.setattr(runtime, watchdog_repair._PATCH_MARKER, False, raising=False)
    monkeypatch.setattr(
        watchdog_repair,
        "_bounded_runtime_heartbeat_json",
        lambda port: {"path": watchdog_repair.INTERNAL_RUNTIME_HEARTBEAT_PATH, "port": port},
    )

    watchdog_repair.install_runtime_watchdog_readiness_repair()

    assert runtime._local_json(10000, "/health") == {
        "path": "/v3/internal/runtime-heartbeats",
        "port": 10000,
    }
    assert runtime._local_json(10000, "/v3/dashboard/snapshot") == {
        "path": "/v3/dashboard/snapshot"
    }
    assert calls == [(10000, "/v3/dashboard/snapshot")]
    assert watchdog_repair.WATCHDOG_HEARTBEAT_READ_TIMEOUT_SECONDS == 10.0


def test_public_health_contract_remains_database_independent():
    payload = liveness_payload()

    assert payload["liveness_database_independent"] is True
    assert payload["database_check"] == "deferred_to_readiness"
    assert payload["runtime_diagnostics"] == "deferred_to_readiness"
    assert payload["readiness_endpoint"] == "/ready"
    assert payload["internal_runtime_heartbeat_endpoint"] == (
        "/v3/internal/runtime-heartbeats"
    )
    assert "runtime_heartbeats" not in payload


def test_parent_generation_heartbeat_cannot_touch_database_before_api_bind():
    source = inspect.getsource(parent_heartbeat.main)

    wait_expression = "while not stopping and not _api_is_bound(port):"
    assert wait_expression in source
    assert source.index(wait_expression) < source.index("_record(")
    assert parent_heartbeat.API_BIND_READ_TIMEOUT_SECONDS == 1.0
    assert parent_heartbeat.API_BIND_POLL_SECONDS == 1.0
    assert "api_bound_before_durable_heartbeat" in source


def test_source_history_supervisor_publishes_timeout_truth_out_of_process_and_retries(
    monkeypatch,
):
    results = [(-9, True), (0, False)]
    diagnostic_payloads: list[dict[str, object]] = []

    monkeypatch.setattr(history_supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(
        history_supervisor,
        "_run_bounded_child",
        lambda _stop_event: results.pop(0),
    )
    monkeypatch.setattr(
        history_supervisor,
        "_run_diagnostic_child",
        lambda payload: diagnostic_payloads.append(dict(payload)) or True,
    )

    history_supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert not results
    assert len(diagnostic_payloads) == 1
    payload = diagnostic_payloads[0]
    assert payload["stage"] == "canonical_history_archive_migration_child_timed_out"
    assert payload["error_type"] == "SourceCoverageHistoryMigrationChildDeadlineExceeded"
    assert payload["attempt_number"] == 1
    assert payload["executor_deadline_seconds"] == 30.0
    assert payload["child_return_code"] == -9
    assert payload["child_timed_out"] is True
    assert payload["retry_seconds"] == 10.0
    assert history_supervisor.DIAGNOSTIC_EXECUTOR_DEADLINE_SECONDS == 5.0


def test_source_history_supervisor_survives_child_launch_exception(monkeypatch):
    calls = 0
    diagnostic_payloads: list[dict[str, object]] = []

    def fake_run(_stop_event):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("spawn failed")
        return 0, False

    monkeypatch.setattr(history_supervisor, "_api_is_bound", lambda _port: True)
    monkeypatch.setattr(history_supervisor, "_run_bounded_child", fake_run)
    monkeypatch.setattr(
        history_supervisor,
        "_run_diagnostic_child",
        lambda payload: diagnostic_payloads.append(dict(payload)) or True,
    )

    history_supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert calls == 2
    assert len(diagnostic_payloads) == 1
    assert diagnostic_payloads[0]["error_type"] == "RuntimeError"
    assert diagnostic_payloads[0]["attempt_number"] == 1


def test_source_history_diagnostic_child_preserves_fresher_child_exception(monkeypatch):
    store = _FakeStore(
        latest=SimpleNamespace(
            state="degraded",
            observed_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
    )
    monkeypatch.setattr(
        history_diagnostic,
        "Settings",
        SimpleNamespace(from_env=lambda: SimpleNamespace(evidence_db_path="test")),
    )
    monkeypatch.setattr(history_diagnostic, "build_evidence_store", lambda _path: store)

    payload = {
        "attempt_number": 1,
        "attempt_started_at": datetime.now(timezone.utc).isoformat(),
        "stage": "canonical_history_archive_migration_child_failed",
        "error_type": "SourceCoverageHistoryMigrationChildExitedNonZero",
        "child_return_code": 1,
        "child_timed_out": False,
        "message": "failed",
        "preserve_fresh_child_failure": True,
        "executor_deadline_seconds": 30.0,
        "retry_seconds": 10.0,
    }

    assert history_diagnostic.publish(payload) == 0
    assert store.records == []


def test_source_history_diagnostic_child_records_signal_without_claiming_oom(monkeypatch):
    store = _FakeStore()
    monkeypatch.setattr(
        history_diagnostic,
        "Settings",
        SimpleNamespace(from_env=lambda: SimpleNamespace(evidence_db_path="test")),
    )
    monkeypatch.setattr(history_diagnostic, "build_evidence_store", lambda _path: store)
    payload = {
        "attempt_number": 4,
        "attempt_started_at": datetime.now(timezone.utc).isoformat(),
        "stage": "canonical_history_archive_migration_child_timed_out",
        "error_type": "SourceCoverageHistoryMigrationChildDeadlineExceeded",
        "child_return_code": -9,
        "child_timed_out": True,
        "message": "deadline",
        "preserve_fresh_child_failure": False,
        "executor_deadline_seconds": 30.0,
        "retry_seconds": 10.0,
    }

    assert history_diagnostic.publish(payload) == 0
    assert len(store.records) == 1
    detail = store.records[0]["detail"]
    assert detail["termination_signal"] == "SIGKILL"
    assert detail["possible_oom_or_external_kill"] is True
    assert detail["oom_kill_proven"] is False
    assert detail["supervisor_executes_migration"] is False
    assert detail["qualification_thresholds_unchanged"] is True
    assert detail["paper_only"] is True
