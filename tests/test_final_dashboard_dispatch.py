from fastapi.testclient import TestClient

from inefficiency_engine import read_api_card_history_deploy as deploy


def test_final_production_app_serves_v5_root_through_actual_asgi_dispatch():
    client = TestClient(deploy.app)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["x-dashboard-contract"] == "v5_mechanism_truth"
    assert response.headers["x-canonical-api-app"] == deploy.CANONICAL_API_APP
    assert "Mechanism truth" in response.text
    assert "Profit mechanism cards" in response.text
    assert "Evidence accumulation" not in response.text
    assert "EXECUTABLE NOW" not in response.text


def test_dashboard_alias_serves_same_v5_contract_through_asgi_dispatch():
    client = TestClient(deploy.app)

    response = client.get("/dashboard")

    assert response.status_code == 200
    assert response.headers["x-dashboard-contract"] == "v5_mechanism_truth"
    assert "Mechanism truth" in response.text
    assert "Evidence accumulation" not in response.text


def test_v5_snapshot_route_is_owned_by_fresh_final_router(monkeypatch):
    monkeypatch.setattr(
        deploy._base,
        "dashboard_snapshot",
        lambda: {
            "release_commit": "test-release",
            "mechanisms": {
                "observed_at": "2026-08-22T14:00:00+00:00",
                "requirements": {
                    "independent_forward_outcomes": 30,
                    "settled_allocator_outcomes": 20,
                },
                "mechanisms": [],
            },
            "runtime_heartbeats": {"workers": {}},
            "lane_executability": {},
            "research_projection_stale": False,
            "operating_projection_stale": False,
        },
    )
    client = TestClient(deploy.app)

    response = client.get("/v3/dashboard/v5-snapshot")

    assert response.status_code == 200
    payload = response.json()
    assert payload["dashboard_ui_contract_version"] == "v5_mechanism_truth"
    assert payload["card_view_version"] == "v5"
    assert payload["dashboard_route_authority"] == "final-fresh-router"
    assert payload["cards"] == []


def test_legacy_read_routes_are_preserved_without_legacy_dashboard_conflicts():
    get_routes = [
        route
        for route in deploy.app.router.routes
        if "GET" in (getattr(route, "methods", set()) or set())
    ]
    paths = [getattr(route, "path", None) for route in get_routes]

    assert paths.count("/") == 1
    assert paths.count("/dashboard") == 1
    assert paths.count("/health") == 1
    assert paths.count("/ready") == 1
    assert paths.count("/v3/dashboard/snapshot") == 1
    assert paths.count("/v3/dashboard/v5-snapshot") == 1
    assert "/v3/portfolio/canonical" in paths
    assert "/v3/operations/mechanisms" in paths
