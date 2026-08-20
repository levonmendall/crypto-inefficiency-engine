from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from inefficiency_engine.read_api_research import app


def test_read_plane_exposes_dashboard_and_durable_status_routes_only():
    client = TestClient(app)
    root = client.get("/")
    assert root.status_code == 200
    assert "Portfolio Command Center" in root.text
    assert "Rejection funnel" in root.text
    assert "worker scheduled" in root.text

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
        "/v3/operations/research-closure",
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

    mechanism_routes = [
        route
        for route in app.routes
        if getattr(route, "path", None) == "/v3/operations/mechanisms"
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    action_routes = [
        route
        for route in app.routes
        if getattr(route, "path", None) == "/v3/operations/action-queue"
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    root_routes = [
        route
        for route in app.routes
        if getattr(route, "path", None) == "/"
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    assert len(mechanism_routes) == 1
    assert len(action_routes) == 1
    assert len(root_routes) == 1


def test_mechanism_overlay_never_full_scans_growing_evidence_tables():
    source = Path("src/inefficiency_engine/read_api_fast.py").read_text()
    assert "select(func.count" not in source
    assert "func.max" not in source
    assert "order_by(table.c.id.desc()).limit(1)" in source
    assert "dex_route_quotes.c.observed_at.desc()" in source
    assert '"query_mode": "append_only_primary_key_tail_plus_compact_closure_summary"' in source


def test_reconciled_capability_truth_removes_obsolete_settlement_blockers():
    source = Path("src/inefficiency_engine/read_api_fast.py").read_text()
    assert 'capabilities.get("realized_two_leg_cex_settlement")' in source
    assert 'capabilities.get("perpetual_short_observed_funding_settlement")' in source
    assert '"capital_location_forward_testing": True' in Path(
        "src/inefficiency_engine/research_closure_worker.py"
    ).read_text()


def test_render_web_service_uses_research_closure_read_plane_entrypoint():
    payload = yaml.safe_load(Path("render.yaml").read_text())
    api = next(service for service in payload["services"] if service["name"] == "cie-shadow-api")
    worker = next(service for service in payload["services"] if service["name"] == "cie-shadow-worker")

    assert api["startCommand"] == "uvicorn inefficiency_engine.read_api_research:app --host 0.0.0.0 --port $PORT"
    assert api["plan"] == "free"
    assert worker["startCommand"] == "cie worker"
    assert "plan" not in worker
