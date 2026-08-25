from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine import cycle_history_active_target_fallback_runtime as runtime


def _snapshot(scan_id: str, completed_at: datetime):
    return SimpleNamespace(scan_id=scan_id, completed_at=completed_at, market_quotes=[])


def test_refresh_failure_serves_prior_certified_target(monkeypatch):
    active_at = datetime(2026, 8, 24, 23, 30, tzinfo=timezone.utc)
    incoming_at = datetime(2026, 8, 25, 0, 0, tzinfo=timezone.utc)
    active_snapshot = _snapshot("certified-scan", active_at)
    incoming_snapshot = _snapshot("newer-scan", incoming_at)
    store = SimpleNamespace(load_scan=lambda scan_id: active_snapshot)
    factory = SimpleNamespace(store=store)

    checkpoint = {
        "active_target": {
            "scan_id": "certified-scan",
            "completed_at": active_at.isoformat(),
        },
        "working_target": {
            "scan_id": "newer-scan",
            "completed_at": incoming_at.isoformat(),
        },
    }
    monkeypatch.setattr(runtime, "_load_checkpoint", lambda _factory: (checkpoint, True))

    def fail_refresh(*_args, **_kwargs):
        raise TimeoutError("canceling statement due to statement timeout")

    monkeypatch.setattr(runtime, "_advance_and_pin", fail_refresh)

    progress = runtime.advance_durable_control_cycle_history_cache(
        factory,
        incoming_snapshot,
        stop_at_monotonic=123.0,
    )

    assert progress["complete"] is True
    assert progress["serving_scan_id"] == "certified-scan"
    assert progress["working_target_scan_id"] == "newer-scan"
    assert progress["rolling_refresh_in_progress"] is True
    assert progress["partial_working_target_authoritative"] is False
    assert progress["refresh_failure_served_prior_exact_target"] is True
    assert progress["refresh_error_type"] == "TimeoutError"
    assert "statement timeout" in progress["refresh_error_message"]
    assert progress["qualification_thresholds_unchanged"] is True
    assert progress["allocation_authority"] is False
    assert progress["live_execution_authority"] is False
    assert progress["paper_only"] is True
    assert incoming_snapshot.scan_id == "certified-scan"
    assert incoming_snapshot.completed_at == active_at


def test_refresh_failure_without_certified_target_remains_fail_closed(monkeypatch):
    snapshot = _snapshot("newer-scan", datetime(2026, 8, 25, tzinfo=timezone.utc))
    factory = SimpleNamespace(store=SimpleNamespace())
    checkpoint = {"active_target": None, "working_target": None}
    monkeypatch.setattr(runtime, "_load_checkpoint", lambda _factory: (checkpoint, True))

    def fail_refresh(*_args, **_kwargs):
        raise TimeoutError("bootstrap still requires exact history")

    monkeypatch.setattr(runtime, "_advance_and_pin", fail_refresh)

    with pytest.raises(TimeoutError, match="bootstrap still requires exact history"):
        runtime.advance_durable_control_cycle_history_cache(factory, snapshot)


def test_fallback_rejects_mismatched_certified_scan(monkeypatch):
    active_at = datetime(2026, 8, 24, 23, 30, tzinfo=timezone.utc)
    wrong_snapshot = _snapshot(
        "certified-scan",
        datetime(2026, 8, 24, 23, 31, tzinfo=timezone.utc),
    )
    incoming = _snapshot("newer-scan", datetime(2026, 8, 25, tzinfo=timezone.utc))
    factory = SimpleNamespace(
        store=SimpleNamespace(load_scan=lambda _scan_id: wrong_snapshot)
    )
    checkpoint = {
        "active_target": {
            "scan_id": "certified-scan",
            "completed_at": active_at.isoformat(),
        }
    }
    monkeypatch.setattr(runtime, "_load_checkpoint", lambda _factory: (checkpoint, True))
    monkeypatch.setattr(
        runtime,
        "_advance_and_pin",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("refresh failed")),
    )

    with pytest.raises(RuntimeError, match="refresh failed"):
        runtime.advance_durable_control_cycle_history_cache(factory, incoming)
