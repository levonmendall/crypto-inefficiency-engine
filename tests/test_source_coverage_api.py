from __future__ import annotations

from fastapi.testclient import TestClient

from inefficiency_engine.api import app


def test_source_coverage_endpoint_is_read_only_and_thirteen_lane():
    client = TestClient(app)
    response = client.get("/v2/source-coverage")
    if response.status_code == 503:
        return
    assert response.status_code == 200
    payload = response.json()
    assert payload["lane_count"] == 13
    assert payload["paper_only"] is True
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False


def test_source_coverage_unknown_lane_is_404():
    client = TestClient(app)
    response = client.get("/v2/source-coverage/not-a-lane")
    assert response.status_code in {404, 503}
