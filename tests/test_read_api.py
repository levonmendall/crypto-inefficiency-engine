from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from inefficiency_engine.dashboard_research_closure import RESEARCH_CLOSURE_DASHBOARD_HTML
from inefficiency_engine.read_api_research import app


def test_read_plane_exposes_dashboard_and_durable_status_routes_only():
    client = TestClient(app)
    root = client.get("/")
    assert root.status_code == 200
    assert "Portfolio Command Center" in root.text
    assert "evidence-diagnostic" in RESEARCH_CLOSURE_DASHBOARD_HTML
    assert "Rejection funnel" in RESEARCH_CLOSURE_DASHBOARD_HTML
    assert "worker scheduled" in RESEARCH_CLOSURE_DASHBOARD_HTML

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
    assert len(mechanism_routes) == 1
    assert len(action_routes) == 1


def test_mechanism_overlay_uses_primary_keys_only_as_non_count_high_water_diagnostics():
    source = Path("src/inefficiency_engine/read_api_fast.py").read_text()
    assert "select(func.count" not in source
    assert "func.max" not in source
    assert "order_by(table.c.id.desc()).limit(1)" in source
    assert "dex_route_quotes.c.observed_at.desc()" in source
    assert "live_count = max(" not in source
    assert 'row["authoritative_observation_count"] = live_count' not in source
    assert 'row["source_table_high_water_marks_display_authority"] = False' in source
    assert '"high_water_marks_are_counts": False' in source
    assert '"query_mode": "append_only_high_water_plus_compact_closure_summary"' in source


def test_reconciled_capability_truth_removes_obsolete_settlement_blockers():
    source = Path("src/inefficiency_engine/read_api_fast.py").read_text()
    assert 'capabilities.get("realized_two_leg_cex_settlement")' in source
    assert 'capabilities.get("perpetual_short_observed_funding_settlement")' in source
    assert '"capital_location_forward_testing": True' in Path(
        "src/inefficiency_engine/research_closure_worker.py"
    ).read_text()


def test_lightweight_portfolio_worker_refreshes_projection_without_research_authority():
    source = Path("src/inefficiency_engine/lightweight_portfolio_worker.py").read_text()
    assert "ResearchDashboardProjectionLedger" in source
    assert "_research_projection_refresh_loop" in source
    assert "research-dashboard-projection-refresh" in source
    assert '"research_computation": False' in source
    assert '"provider_calls": False' in source
    assert '"allocation_authority": False' in source
    assert '"live_execution_authority": False' in source


def test_render_combined_service_uses_restored_command_center_with_v5_mechanism_truth():
    payload = yaml.safe_load(Path("render.yaml").read_text())
    assert len(payload["services"]) == 1
    runtime = payload["services"][0]

    assert runtime["name"] == "cie-shadow-worker"
    assert runtime["type"] == "web"
    assert runtime["plan"] == "standard"
    assert runtime["startCommand"] == (
        "PYTHONPATH=src python -m inefficiency_engine.render_combined_postbind_history_projection"
    )

    canonical = Path("src/inefficiency_engine/render_combined.py").read_text()
    assert 'CANONICAL_API_APP = "inefficiency_engine.read_api_liveness_deploy:app"' in canonical
    assert "render_combined_runtime" in canonical

    postbind = Path("src/inefficiency_engine/render_combined_postbind.py").read_text()
    assert "from inefficiency_engine import render_combined as base" in postbind
    assert "base._ORIGINAL_MAIN()" in postbind

    repair_bootstrap = Path(
        "src/inefficiency_engine/render_combined_postbind_lane_repair.py"
    ).read_text()
    assert "from inefficiency_engine import render_combined_postbind as base" in repair_bootstrap
    assert 'commands["source"] = list(SOURCE_REPAIR_COMMAND)' in repair_bootstrap
    assert "return_code = base.main()" in repair_bootstrap
    assert "_record_parent_terminal(" in repair_bootstrap
    assert "return return_code" in repair_bootstrap

    projection_bootstrap = Path(
        "src/inefficiency_engine/render_combined_postbind_history_projection.py"
    ).read_text()
    assert "run_durable_lane_history_projection_supervisor" in projection_bootstrap
    assert "from inefficiency_engine import render_combined_postbind_lane_repair as base" in projection_bootstrap
    assert "return base.main()" in projection_bootstrap

    deploy = Path("src/inefficiency_engine/read_api_card_history_deploy.py").read_text()
    assert "dashboard_cards_v5" in deploy
    assert "dashboard_command_center_v6" in deploy
    assert "DASHBOARD_COMMAND_CENTER_HTML" in deploy
    assert "build_dashboard_v5_snapshot" in deploy
    assert '"command_center": _command_center_payload(source)' in deploy
    assert "dashboard_card_history" not in deploy
    assert '"dashboard_inherited_card_overlay_chain_active": False' in deploy
    assert '"dashboard_contract_active": True' in deploy

    history_projection = Path(
        "src/inefficiency_engine/read_api_durable_history_projection_deploy.py"
    ).read_text()
    assert 'DURABLE_HISTORY_PATH = "/v3/dashboard/durable-lane-history"' in history_projection
    assert "durable_history_projection_payload" in history_projection
    assert "history is not being reported as zero" in history_projection

    v5 = Path("src/inefficiency_engine/dashboard_cards_v5.py").read_text()
    assert 'DASHBOARD_UI_CONTRACT_VERSION = "v5_mechanism_truth"' in v5
    assert "function renderCard(c)" in v5
    assert "Current source" in v5
    assert "Raw / emitted" in v5
    assert "Research overdue since" in v5
    assert "_replace_once" not in v5

    command = Path("src/inefficiency_engine/dashboard_command_center_v6.py").read_text()
    assert 'COMMAND_CENTER_LAYOUT_VERSION = "v6_full_command_center"' in command
    assert "Current portfolio NAV" in command
    assert "Profit mechanism certification" in command
    assert "fetch('/v3/dashboard/v5-snapshot'" in command

    assert runtime["healthCheckPath"] == "/health"
    assert runtime["autoDeployTrigger"] == "checksPass"
    assert runtime["buildCommand"] == "python -m pip install --retries 5 --timeout 30 --no-cache-dir ."
