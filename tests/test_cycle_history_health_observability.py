from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from inefficiency_engine.cycle_history_health_observability import (
    install_cycle_history_health_observability,
)


class _Store:
    def __init__(self, heartbeat):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, worker_id: str):
        assert worker_id == "canonical-control-operating-loop"
        return self.heartbeat


class _Base:
    def __init__(self, heartbeat):
        self.store = _Store(heartbeat)

    def _store(self):
        return self.store

    def _runtime_heartbeats(self):
        return {
            "available": True,
            "workers": {
                "canonical_control": {
                    "worker_id": "canonical-control-operating-loop",
                    "available": True,
                    "state": "degraded",
                    "error_type": "CycleHistoryCacheError",
                }
            },
        }


def test_cycle_history_health_exposes_underlying_persisted_failure_without_authority():
    heartbeat = SimpleNamespace(
        detail={
            "cycle_history_cache_complete": False,
            "cycle_history_cache_progress": {
                "complete": False,
                "error_type": "OperationalError",
                "message": "canceling statement due to statement timeout",
                "working_target_scan_id": "scan-123",
                "current_pair_count": 10,
                "next_pair_index": 4,
                "secret_unbounded_field": "must-not-leak",
            },
        }
    )
    base = _Base(heartbeat)

    install_cycle_history_health_observability(base)
    payload = base._runtime_heartbeats()
    control = payload["workers"]["canonical_control"]

    assert control["state"] == "degraded"
    assert control["error_type"] == "CycleHistoryCacheError"
    assert control["cycle_history_cache_complete"] is False
    assert control["cycle_history_cache_error_type"] == "OperationalError"
    assert (
        control["cycle_history_cache_error_message"]
        == "canceling statement due to statement timeout"
    )
    assert control["cycle_history_cache_progress"]["working_target_scan_id"] == "scan-123"
    assert control["cycle_history_cache_progress"]["current_pair_count"] == 10
    assert control["cycle_history_cache_progress"]["next_pair_index"] == 4
    assert "secret_unbounded_field" not in control["cycle_history_cache_progress"]
    assert control["cycle_history_cache_diagnostic_only"] is True
    assert payload["cycle_history_cache_error_observability"] is True


def test_cycle_history_health_hook_is_idempotent():
    heartbeat = SimpleNamespace(detail={"cycle_history_cache_progress": {}})
    base = _Base(heartbeat)

    install_cycle_history_health_observability(base)
    installed = base._runtime_heartbeats
    install_cycle_history_health_observability(base)

    assert base._runtime_heartbeats is installed


def test_production_api_installs_cycle_history_observability_and_bounded_heartbeat_reads():
    source = Path(
        "src/inefficiency_engine/read_api_bounded_heartbeat_deploy.py"
    ).read_text()

    assert "install_bounded_evidence_heartbeat_read()" in source
    assert "install_cycle_history_health_observability(active)" in source


def test_render_entrypoint_isolates_portfolio_while_retaining_bounded_worker():
    entrypoint = Path(
        "src/inefficiency_engine/render_combined_postbind_lane_repair.py"
    ).read_text()
    supervisor = Path(
        "src/inefficiency_engine/portfolio_process_supervisor.py"
    ).read_text()

    assert "inefficiency_engine.portfolio_process_supervisor" in entrypoint
    assert 'commands["portfolio"] = list(PORTFOLIO_BOUNDED_HEARTBEAT_COMMAND)' in entrypoint
    assert "lightweight_portfolio_worker_bounded_heartbeat" in supervisor
    assert "restarting portfolio only" in supervisor
