from __future__ import annotations

import inspect
import signal
import threading
from types import SimpleNamespace

from inefficiency_engine import cycle_history_background_supervisor_repair as supervisor
from inefficiency_engine import cycle_history_index_maintenance_child as child
from inefficiency_engine import cycle_history_index_supervisor_probe as probe
from inefficiency_engine.evidence import EvidenceStore


def _record_child_terminal(store: EvidenceStore, *, attempt: int = 284) -> None:
    store.record_worker_heartbeat(
        worker_id=child.WORKER_ID,
        state="degraded",
        error_type="CycleHistoryExactIndexUnavailable",
        detail={
            "stage": "cycle_history_index_retry_pending",
            "attempt_number": attempt,
            "current_index": "ix_runtime_market_quotes_venue_asset_observed_at_id_v325",
            "maintenance_result": {
                "complete": False,
                "attempted": [
                    {
                        "error_type": "OperationalError",
                        "message": "canceling statement due to lock timeout",
                        "index": "ix_runtime_market_quotes_venue_asset_observed_at_id_v325",
                    }
                ],
            },
        },
    )


def _record_observing(store: EvidenceStore, *, attempt: int = 284) -> None:
    store.record_worker_heartbeat(
        worker_id=child.WORKER_ID,
        state="running",
        detail={
            "stage": "cycle_history_index_supervisor_observing",
            "attempt_number": attempt,
            "supervisor_observation": True,
            "current_index": "ix_runtime_market_quotes_venue_asset_observed_at_id_v325",
        },
    )


def test_terminal_capture_recovers_child_sql_cause_beneath_newer_observing_row(tmp_path):
    store = EvidenceStore(tmp_path / "terminal-capture.sqlite")
    _record_child_terminal(store)
    _record_observing(store)

    result = probe._capture_terminal(
        store,
        {
            "expected_attempt_number": 284,
            "state": "degraded",
            "stage": "cycle_history_index_child_retry_pending",
            "error_type": "IndexChildExitedNonZero",
            "detail": {"child_return_code": 77, "child_timed_out": False},
            "allow_supervisor_only_terminal": False,
        },
    )

    assert result["terminal_truth_durable"] is True
    assert result["child_sql_error_type"] == "OperationalError"
    assert result["child_sql_error_message"] == "canceling statement due to lock timeout"

    latest = store.latest_worker_heartbeat(child.WORKER_ID)
    assert latest is not None
    detail = latest.detail
    assert detail["attempt_number"] == 284
    assert detail["terminal_truth_durable"] is True
    assert detail["terminal_capture_verified"] is True
    assert detail["ddl_retry_blocked_until_terminal_truth_durable"] is True
    assert detail["child_terminal_stage"] == "cycle_history_index_retry_pending"
    assert detail["child_sql_error_type"] == "OperationalError"
    assert detail["child_sql_error_message"] == "canceling statement due to lock timeout"
    assert detail["child_return_code"] == 77
    assert detail["child_effective_index_name"].endswith("_v325")


def test_normal_exit_without_child_terminal_fails_closed_and_does_not_publish(tmp_path):
    store = EvidenceStore(tmp_path / "terminal-missing.sqlite")
    _record_observing(store, attempt=285)
    before = store.latest_worker_heartbeat(child.WORKER_ID)

    result = probe._capture_terminal(
        store,
        {
            "expected_attempt_number": 285,
            "state": "degraded",
            "stage": "cycle_history_index_child_retry_pending",
            "error_type": "IndexChildExitedNonZero",
            "detail": {"child_return_code": 77, "child_timed_out": False},
            "allow_supervisor_only_terminal": False,
        },
    )

    assert result["terminal_truth_durable"] is False
    assert result["reason"] == "child_terminal_heartbeat_not_durable"
    after = store.latest_worker_heartbeat(child.WORKER_ID)
    assert after is not None and before is not None
    assert after.observed_at == before.observed_at
    assert after.detail["stage"] == "cycle_history_index_supervisor_observing"


def test_signal_exit_can_publish_durable_supervisor_terminal_without_child_row(tmp_path):
    store = EvidenceStore(tmp_path / "signal-terminal.sqlite")
    _record_observing(store, attempt=286)

    result = probe._capture_terminal(
        store,
        {
            "expected_attempt_number": 286,
            "state": "degraded",
            "stage": "cycle_history_index_child_terminated",
            "error_type": "IndexChildTerminatedBySignal",
            "detail": {
                "child_return_code": -int(signal.SIGKILL),
                "child_timed_out": False,
                "termination_signal": "SIGKILL",
                "termination_signal_number": int(signal.SIGKILL),
                "possible_oom_or_external_kill": True,
                "oom_kill_proven": False,
            },
            "allow_supervisor_only_terminal": True,
        },
    )

    assert result["terminal_truth_durable"] is True
    latest = store.latest_worker_heartbeat(child.WORKER_ID)
    assert latest is not None
    detail = latest.detail
    assert detail["terminal_truth_durable"] is True
    assert detail["child_terminal_heartbeat_unavailable"] is True
    assert detail["termination_signal"] == "SIGKILL"
    assert detail["possible_oom_or_external_kill"] is True
    assert detail["oom_kill_proven"] is False


def test_supervisor_retries_only_terminal_capture_until_verified(monkeypatch):
    calls: list[dict[str, object]] = []
    responses = iter(
        [
            {"ok": True, "terminal_truth_durable": False, "reason": "missing"},
            None,
            {"ok": True, "terminal_truth_durable": True, "reason": "verified"},
        ]
    )

    monkeypatch.setattr(
        supervisor,
        "_run_index_diagnostic",
        lambda payload: calls.append(dict(payload)) or next(responses),
    )
    monkeypatch.setattr(supervisor, "INDEX_TERMINAL_CAPTURE_RETRY_SECONDS", 0.0)

    result = supervisor._capture_index_terminal_truth(
        stop_event=threading.Event(),
        expected_attempt_number=287,
        state="degraded",
        stage="cycle_history_index_child_retry_pending",
        error_type="IndexChildExitedNonZero",
        detail={"child_return_code": 77},
        allow_supervisor_only_terminal=False,
    )

    assert result is not None
    assert result["terminal_truth_durable"] is True
    assert len(calls) == 3
    assert all(call["action"] == "capture_terminal" for call in calls)
    assert all(call["expected_attempt_number"] == 287 for call in calls)


def test_child_terminal_heartbeat_write_retries_are_bounded(monkeypatch):
    calls = {"count": 0}

    def write(**_kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            raise RuntimeError("temporary heartbeat write failure")
        return SimpleNamespace()

    store = SimpleNamespace(record_worker_heartbeat=write)
    monkeypatch.setattr(child.time, "sleep", lambda _seconds: None)

    assert child._record_heartbeat(
        store,
        state="degraded",
        stage="cycle_history_index_retry_pending",
        error_type="CycleHistoryExactIndexUnavailable",
        detail={"attempt_number": 288},
        durable_attempts=4,
    ) is True
    assert calls["count"] == 3


def test_retry_gate_preserves_existing_exact_index_safety_constants():
    source = inspect.getsource(supervisor.run_cycle_history_background_supervisor)

    assert child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS == 3_600_000
    assert supervisor.INDEX_EXECUTOR_DEADLINE_SECONDS == 3660.0
    assert supervisor.INDEX_DIAGNOSTIC_DEADLINE_SECONDS == 8.0
    assert supervisor.INDEX_RETRY_SECONDS == 30.0
    assert "_capture_index_terminal_truth(" in source
    assert source.index("_run_index_child(") < source.index("_capture_index_terminal_truth(")
    assert "DDL launch blocked" in source
    assert "DDL retry blocked" in inspect.getsource(supervisor._capture_index_terminal_truth)
