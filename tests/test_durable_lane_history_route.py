from __future__ import annotations

from fastapi.testclient import TestClient


def test_durable_lane_history_route_fails_soft_when_persistence_is_unavailable(monkeypatch):
    from inefficiency_engine import read_api_lane_history_ui_deploy as lane_ui

    monkeypatch.setattr(lane_ui.read_plane, "_store", lambda: None)
    response = TestClient(lane_ui.app).get("/v3/dashboard/durable-lane-history")
    assert response.status_code == 503
    assert "evidence persistence is not configured" in response.text


def test_durable_lane_history_route_preserves_read_only_authority(monkeypatch):
    from inefficiency_engine import read_api_lane_history_ui_deploy as lane_ui

    sentinel_store = object()
    monkeypatch.setattr(lane_ui.read_plane, "_store", lambda: sentinel_store)
    monkeypatch.setattr(
        lane_ui,
        "read_durable_lane_history",
        lambda store, *, start: {
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
        },
    )

    response = TestClient(lane_ui.app).get("/v3/dashboard/durable-lane-history")
    assert response.status_code == 200
    payload = response.json()
    assert payload["lane_count"] == 13
    assert payload["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False
    assert payload["paper_only"] is True
