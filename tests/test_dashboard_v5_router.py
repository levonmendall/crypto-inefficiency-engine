from datetime import datetime, timezone

import inefficiency_engine.dashboard_v5_router as router_module
from inefficiency_engine.dashboard_v5_router import V5_SNAPSHOT_PATH, build_v5_dashboard_router


def test_shared_v5_router_serves_full_command_center_and_dedicated_snapshot_path():
    router = build_v5_dashboard_router()
    paths = {getattr(route, "path", None) for route in router.routes}

    assert "/" in paths
    assert "/dashboard" in paths
    assert V5_SNAPSHOT_PATH in paths

    root = next(route for route in router.routes if getattr(route, "path", None) == "/")
    response = root.endpoint()
    html = bytes(response.body).decode("utf-8")

    # The full command-center context is restored around the V5 mechanism cards.
    for label in (
        "Current portfolio NAV",
        "Runtime health",
        "Equity curve",
        "P&L attribution",
        "Open paper positions",
        "Recent completed trades",
        "Skipped / rejected allocations",
        "Cycle history backfill",
        "Evidence accumulation",
        "Profit mechanism certification",
        "What needs attention next",
        "Active volume universe",
    ):
        assert label in html

    # Mechanism truth remains V5-only; do not resurrect the misleading legacy cards.
    assert "v5_mechanism_truth" in html
    assert "Current source" in html
    assert "Raw / emitted" in html
    assert "Paper-capable" in html
    assert "OBSERVATIONS" not in html
    assert "Executable now" not in html
    assert "fetch('/v3/dashboard/v5-snapshot'" in html
    assert "fetch('/v3/dashboard/snapshot'" not in html
    assert response.headers["x-dashboard-route"] == "canonical-v5-router"
    assert response.headers["x-dashboard-layout"] == "v6_full_command_center"


def test_dedicated_v5_snapshot_preserves_command_center_and_builds_card_truth(monkeypatch):
    now = datetime(2026, 8, 22, 6, 0, tzinfo=timezone.utc)
    legacy = {
        "release_commit": "feedface",
        "portfolio": {"observed_at": now.isoformat(), "portfolio_id": "canonical"},
        "performance": {
            "current_nav_usd": 250100.0,
            "cash_usd": 249000.0,
            "realized_pnl_usd": 100.0,
        },
        "runtime": {"operational": True, "valuation_status": "cash_only"},
        "positions": {"positions": [{"asset": "BTC"}]},
        "trades": {"trades": [{"asset": "ETH"}]},
        "history": {"count": 2, "snapshots": [{"nav_usd": 250100.0}]},
        "skips": {"skips": [{"asset": "SOL", "reason": "fail_closed"}]},
        "attribution": {"pnl_by_mechanism_usd": {"carry": 100.0}},
        "queue": {"actions": [{"name": "Carry", "state": "collecting"}]},
        "cycle_history": {"available": True, "asset_count": 1, "assets": [{"asset": "BTC"}]},
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
    assert payload["command_center_layout_version"] == "v6_full_command_center"
    assert payload["dashboard_route_authority"] == "canonical-v5-router"
    assert payload["cards"][0]["mechanism_id"] == "carry"
    assert payload["cards"][0]["source_item_count"] == 9
    assert payload["cards"][0]["signal_count"] == 3

    command = payload["command_center"]
    assert command["portfolio"]["portfolio_id"] == "canonical"
    assert command["performance"]["current_nav_usd"] == 250100.0
    assert command["positions"]["positions"][0]["asset"] == "BTC"
    assert command["trades"]["trades"][0]["asset"] == "ETH"
    assert command["history"]["count"] == 2
    assert command["skips"]["skips"][0]["asset"] == "SOL"
    assert command["cycle_history"]["available"] is True
    assert command["runtime_heartbeats"]["workers"]["research"]["state"] == "running"


def test_command_center_snapshot_defaults_missing_optional_sections(monkeypatch):
    monkeypatch.setattr(router_module, "_legacy_snapshot", lambda: {"mechanisms": {"mechanisms": []}})
    router = build_v5_dashboard_router()
    route = next(route for route in router.routes if getattr(route, "path", None) == V5_SNAPSHOT_PATH)
    payload = route.endpoint()
    command = payload["command_center"]

    assert command["positions"] == {"positions": []}
    assert command["trades"] == {"trades": []}
    assert command["history"] == {"count": 0, "snapshots": []}
    assert command["skips"] == {"skips": []}
    assert command["queue"] == {"actions": []}
    assert command["cycle_history"] == {"available": False, "assets": []}
