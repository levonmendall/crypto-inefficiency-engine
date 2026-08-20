from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from inefficiency_engine.read_api import app


def test_read_plane_exposes_dashboard_and_durable_status_routes_only():
    client = TestClient(app)
    root = client.get("/")
    assert root.status_code == 200
    assert "Portfolio Command Center" in root.text

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["read_plane"] is True
    assert health.json()["live_execution"] is False

    paths = set(app.openapi()["paths"])
    expected = {
        "/health",
        "/v3/portfolio/canonical",
        "/v3/portfolio/runtime-status",
        "/v3/portfolio/performance",
        "/v3/portfolio/positions",
        "/v3/portfolio/trades",
        "/v3/portfolio/skips",
        "/v3/portfolio/history",
        "/v3/portfolio/attribution",
        "/v3/operations/certification/latest",
        "/v3/operations/certification/history",
        "/v3/operations/certification/summary",
        "/v3/operations/mechanisms",
        "/v3/operations/action-queue",
        "/v1/worker/health",
        "/v1/evidence/counts",
    }
    assert expected.issubset(paths)

    # Provider-heavy and authority-adjacent manual compute surfaces stay out of the
    # production web process. They remain available in the full development API.
    assert "/v1/opportunities/live" not in paths
    assert "/v1/executability/live" not in paths
    assert "/v1/shadow/cycle" not in paths
    assert "/v3/portfolio/cycle" not in paths
    assert "/v3/operations/certification/cycle" not in paths


def test_render_web_service_uses_read_plane_entrypoint():
    payload = yaml.safe_load(Path("render.yaml").read_text())
    api = next(service for service in payload["services"] if service["name"] == "cie-shadow-api")
    worker = next(service for service in payload["services"] if service["name"] == "cie-shadow-worker")

    assert api["startCommand"] == "uvicorn inefficiency_engine.read_api:app --host 0.0.0.0 --port $PORT"
    assert api["plan"] == "free"
    assert worker["startCommand"] == "cie worker"
    assert "plan" not in worker
