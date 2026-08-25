from __future__ import annotations


def test_executive_dashboard_prioritizes_health_and_strategy_effectiveness():
    from inefficiency_engine.read_api_executive_ui_deploy import executive_dashboard_html

    html = executive_dashboard_html()

    assert "Strategy Performance Dashboard" in html
    assert "Executive scorecard" in html
    assert "System health" in html
    assert "Data quality" in html
    assert "Research progress" in html
    assert "Strategy effectiveness" in html
    assert "Portfolio performance" in html
    assert "Execution readiness" in html
    assert "Opportunity funnel" in html


def test_strategy_table_uses_forward_and_paper_effectiveness_metrics():
    from inefficiency_engine.read_api_executive_ui_deploy import executive_dashboard_html

    html = executive_dashboard_html()

    assert "strategyEffectivenessBody" in html
    assert "forward_outcome_count" in html
    assert "mean_forward_net_return" in html
    assert "forward_hit_rate" in html
    assert "qualified_count" in html
    assert "paper_capable" in html
    assert "certified" in html
    assert "Mature · not qualified" in html
    assert "Building evidence" in html


def test_detailed_operational_surfaces_remain_available_but_collapsed():
    from inefficiency_engine.read_api_executive_ui_deploy import executive_dashboard_html

    html = executive_dashboard_html()

    assert "Diagnostics & implementation detail" in html
    assert "id=\"diagnosticSections\"" in html
    assert "installExecutiveLayout" in html
    assert "sourceProblems" in html
    assert "runtimeGrid" in html
    assert "cycleHistoryList" in html
    assert "actionQueue" in html
    assert "volumeUniverse" in html
    assert "Profit mechanism certification" in html


def test_source_connectivity_updates_scorecard_without_becoming_primary_ui():
    from inefficiency_engine.read_api_executive_ui_deploy import executive_dashboard_html

    html = executive_dashboard_html()

    assert "updateExecutiveDataQuality" in html
    assert "window.__sourceConnectivity=p" in html
    assert "configured sources usable" in html
    assert "refresh warnings" in html
    assert "credential-gated" in html


def test_production_liveness_wraps_executive_ui_and_keeps_health_database_independent():
    from inefficiency_engine import read_api_executive_ui_deploy as executive
    from inefficiency_engine import read_api_liveness_deploy as liveness

    assert liveness.app.inner is executive.app
    payload = liveness.liveness_payload()
    assert payload["database_check"] == "deferred_to_readiness"
    assert payload["liveness_database_independent"] is True
