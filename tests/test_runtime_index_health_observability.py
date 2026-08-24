from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.runtime_index_health_observability import (
    RUNTIME_INDEX_LABEL,
    RUNTIME_INDEX_WORKER_ID,
    install_runtime_index_health_observability,
)


class _Store:
    def __init__(self, heartbeat):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, worker_id):
        assert worker_id == RUNTIME_INDEX_WORKER_ID
        return self.heartbeat


def test_runtime_index_health_exposes_exact_gate_progress():
    heartbeat = SimpleNamespace(
        state="running",
        error_type=None,
        observed_at=datetime.now(timezone.utc),
        detail={
            "attempt": 3,
            "stage": "runtime_index_starting",
            "scope": "control_gate",
            "current_index": "ix_runtime_market_quotes_venue_observed_at",
            "current_table": "market_quotes",
            "current_index_runtime_seconds": None,
            "current_index_ok": None,
            "current_index_concurrent": True,
            "message": None,
            "control_gate_released": False,
            "background_indexes_complete": False,
        },
    )
    base = SimpleNamespace(
        _RUNTIME_HEARTBEATS={},
        _RUNTIME_STALE_AFTER_SECONDS={},
        _store=lambda: _Store(heartbeat),
    )

    def original_runtime_heartbeats():
        return {
            "available": True,
            "workers": {
                RUNTIME_INDEX_LABEL: {
                    "worker_id": RUNTIME_INDEX_WORKER_ID,
                    "available": True,
                    "state": heartbeat.state,
                    "error_type": heartbeat.error_type,
                    "stage": heartbeat.detail["stage"],
                }
            },
        }

    base._runtime_heartbeats = original_runtime_heartbeats
    install_runtime_index_health_observability(base)

    assert base._RUNTIME_HEARTBEATS[RUNTIME_INDEX_LABEL] == RUNTIME_INDEX_WORKER_ID
    payload = base._runtime_heartbeats()
    worker = payload["workers"][RUNTIME_INDEX_LABEL]
    assert payload["runtime_index_gate_observability"] is True
    assert worker["attempt"] == 3
    assert worker["scope"] == "control_gate"
    assert worker["current_index"] == "ix_runtime_market_quotes_venue_observed_at"
    assert worker["current_table"] == "market_quotes"
    assert worker["control_gate_released"] is False
    assert worker["background_indexes_complete"] is False


def test_final_api_installs_runtime_index_worker_in_health_contract():
    from inefficiency_engine import read_api_card_history_deploy as final_deploy

    assert (
        final_deploy._base._RUNTIME_HEARTBEATS[RUNTIME_INDEX_LABEL]
        == RUNTIME_INDEX_WORKER_ID
    )
    assert final_deploy._base._RUNTIME_STALE_AFTER_SECONDS[RUNTIME_INDEX_LABEL] == 1800.0
