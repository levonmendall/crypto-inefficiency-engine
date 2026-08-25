from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.cycle_history_health_observability import (
    install_cycle_history_health_observability,
)


class _Store:
    def __init__(self, heartbeat):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, _worker_id: str):
        return self.heartbeat


class _Base:
    def __init__(self, *, control, heartbeat):
        self.control = control
        self.store = _Store(heartbeat)

    def _store(self):
        return self.store

    def _runtime_heartbeats(self):
        return {"available": True, "workers": {"canonical_control": dict(self.control)}}


def test_empty_newer_heartbeat_does_not_erase_terminal_cycle_history_error():
    control = {
        "available": True,
        "state": "degraded",
        "cycle_history_cache_complete": False,
        "cycle_history_cache_progress": {
            "complete": False,
            "error_type": "OperationalError",
            "message": "statement timeout",
        },
    }
    base = _Base(
        control=control,
        heartbeat=SimpleNamespace(
            detail={
                "cycle_history_cache_complete": None,
                "cycle_history_cache_progress": {},
            }
        ),
    )

    install_cycle_history_health_observability(base)
    result = base._runtime_heartbeats()["workers"]["canonical_control"]

    assert result["cycle_history_cache_complete"] is False
    assert result["cycle_history_cache_error_type"] == "OperationalError"
    assert result["cycle_history_cache_error_message"] == "statement timeout"


def test_refresh_fallback_is_visible_without_becoming_control_error():
    progress = {
        "complete": True,
        "rolling_refresh_in_progress": True,
        "refresh_failure_served_prior_exact_target": True,
        "refresh_error_type": "TimeoutError",
        "refresh_error_message": "statement timeout",
        "serving_scan_id": "certified-scan",
    }
    base = _Base(
        control={"available": True, "state": "success"},
        heartbeat=SimpleNamespace(
            detail={
                "cycle_history_cache_complete": True,
                "cycle_history_cache_progress": progress,
            }
        ),
    )

    install_cycle_history_health_observability(base)
    result = base._runtime_heartbeats()["workers"]["canonical_control"]

    assert result["state"] == "success"
    assert result["cycle_history_cache_complete"] is True
    assert result["cycle_history_cache_error_type"] is None
    assert result["cycle_history_cache_refresh_error_type"] == "TimeoutError"
    assert result["cycle_history_cache_refresh_error_message"] == "statement timeout"
    assert result["cycle_history_cache_progress"][
        "refresh_failure_served_prior_exact_target"
    ] is True
