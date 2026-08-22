from datetime import datetime, timezone

import inefficiency_engine.dashboard_v5_router as router_module
from inefficiency_engine.dashboard_v5_router import V5_SNAPSHOT_PATH, build_v5_dashboard_router


def test_shared_v5_router_serves_v5_html_and_dedicated_snapshot_path():
    router = build_v5_dashboard_router()
    paths = {getattr(route, "path", None) for route in router.routes}

    assert "/" in paths
    assert "/dashboard" in paths
    assert V5_SNAPSHOT_PATH in paths

    root = next(route for route in router.routes if getattr(route, "path", None) == "/")
    response = root.endpoint()
    html = bytes(response.body).decode("utf-8")

    assert "v5_mechanism_truth" in html
    assert "Current source" in html
    assert "Raw / emitted" in html
    assert "fetch('/v3/dashboard/v5-snapshot'" in html
    assert "fetch('/v3/dashboard/snapshot'" not in html
    assert response.headers["x-dashboard-route"] == "canonical-v5-router"


def test_dedicated_v5_snapshot_builds_from_persisted_compact_payload(monkeypatch):
    now = datetime(2026, 8, 22, 6, 0, tzinfo=timezone.utc)
    legacy = {
        "release_commit": "feedface",
        "runtime_heartbeats": {
            "workers": {
                "research": {
                    "available": True,
                    "state": "running",
                    "stale": False,
                    "error_type": None,
                    "observed_at": now.isoformat(),
                    "age_seconds": 0,
                },
                "portfolio": {
                    "available": True,
                    "state": "running",
                    "stale": False,
                    "error_type": None,
                    "observed_at": now.isoformat(),
                    "age_seconds": 0,
                },
            }
        },
        "current_source_truth": {
            "carry": {
                "provider_status": "connected",
                "connected": True,
                "source_state": "sufficient",
                "evidence_complete": True,
                "current_authoritative_item_count": 9,
                "latest_authoritative_observation_at": now.isoformat(),
                "independent_authoritative_source_count": 2,
                "missing_evidence_classes": [],
                "covered_evidence_classes": ["price", "funding", "executable_depth"],
            }
        },
        "lane_executability": {
            "projection_current_for_execution": True,
            "paper_execution_capable_lanes": [],
        },
        "mechanisms": {
            "observed_at": now.isoformat(),
            "requirements": {
                "independent_forward_outcomes": 30,
                "settled_allocator_outcomes": 20,
            },
            "mechanisms": [
                {
                    "mechanism_id": "carry",
                    "name": "Carry / basis / funding",
                    "state": "collecting",
                    "forward_signal_count": 3,
                    "independent_forward_outcome_count": 2,
                    "current_statistically_qualified_count": 0,
                    "settled_allocator_outcome_count": 0,
                }
            ],
        },
    }
    monkeypatch.setattr(router_module, "_legacy_snapshot", lambda: legacy)

    router = build_v5_dashboard_router()
    route = next(route for route in router.routes if getattr(route, "path", None) == V5_SNAPSHOT_PATH)
    payload = route.endpoint()

    assert payload["dashboard_ui_contract_version"] == "v5_mechanism_truth"
    assert payload["dashboard_route_authority"] == "canonical-v5-router"
    assert payload["cards"][0]["mechanism_id"] == "carry"
    assert payload["cards"][0]["source_item_count"] == 9
    assert payload["cards"][0]["signal_count"] == 3
