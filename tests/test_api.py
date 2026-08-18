from fastapi.testclient import TestClient

from inefficiency_engine.api import app


def test_health_and_demo():
    client = TestClient(app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["paper_only"] is True
    demo = client.get("/v1/opportunities/demo")
    assert demo.status_code == 200
    assert demo.json()["count"] >= 1
