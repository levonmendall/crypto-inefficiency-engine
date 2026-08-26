from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from inefficiency_engine.source_coverage_catalog import LANES


def test_durable_lane_history_route_fails_soft_when_persistence_is_unavailable(monkeypatch):
    from inefficiency_engine import read_api_durable_history_projection_deploy as deploy

    monkeypatch.setattr(deploy.read_plane, "_store", lambda: None)
    response = TestClient(deploy.app).get(deploy.DURABLE_HISTORY_PATH)

    assert response.status_code == 200
    payload = response.json()
    assert payload["history_projection_available"] is False
    assert payload["history_projection_reason"] == "evidence_persistence_not_configured"
    assert payload["lane_count"] == 13
    for lane_id, definition in LANES.items():
        row = payload["lanes"][lane_id]
        assert row["required_evidence_class_count"] == len(definition["required"])
        assert row["required_evidence_class_count"] > 0
        assert row["recovered_evidence_class_count"] == 0
    assert payload["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["paper_only"] is True


def test_durable_lane_history_route_preserves_read_only_authority():
    from inefficiency_engine import read_api_durable_history_projection_deploy as deploy

    history = {
        "lane_count": 13,
        "lanes_with_durable_history": 11,
        "lanes_without_durable_history": 2,
        "candidate_level_history_synthesized": False,
        "historical_counts_as_forward": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
        "lanes": {},
    }

    class FakeStore:
        def latest_worker_heartbeat(self, worker_id):
            assert worker_id == deploy.DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID
            return SimpleNamespace(
                observed_at=datetime.now(timezone.utc),
                state="success",
                error_type=None,
                detail={"history": history},
            )

    payload = deploy.durable_history_projection_payload(FakeStore())

    assert payload["lane_count"] == 13
    assert payload["history_projection_available"] is True
    assert payload["history_projection_stale"] is False
    assert payload["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["paper_only"] is True
