from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.disposable_heavy_job import _research_completion_state


class _Store:
    def __init__(self, heartbeat):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, worker_id):
        return self.heartbeat


def test_research_degradation_propagates_to_heavy_worker_status():
    heartbeat = SimpleNamespace(
        state="degraded",
        error_type="ResearchSubsystemDegraded",
        detail={
            "subsystem_error_count": 2,
            "subsystem_error_keys": [
                "alpha_forward_evidence_error_type",
                "qualified_bridge_error_type",
            ],
        },
    )
    state, error_type, detail = _research_completion_state(_Store(heartbeat))
    assert state == "degraded"
    assert error_type == "ResearchSubsystemDegraded"
    assert detail["research_subsystem_error_count"] == 2
    assert len(detail["research_subsystem_error_keys"]) == 2


def test_successful_research_remains_successful_at_heavy_worker_layer():
    heartbeat = SimpleNamespace(
        state="success",
        error_type=None,
        detail={"subsystem_error_count": 0, "subsystem_error_keys": []},
    )
    state, error_type, detail = _research_completion_state(_Store(heartbeat))
    assert state == "success"
    assert error_type is None
    assert detail["research_worker_state"] == "success"


def test_missing_research_heartbeat_is_never_reported_as_success():
    state, error_type, detail = _research_completion_state(_Store(None))
    assert state == "degraded"
    assert error_type == "ResearchHeartbeatUnavailable"
    assert detail["research_worker_state"] == "unavailable"
