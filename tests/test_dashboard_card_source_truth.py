from inefficiency_engine.dashboard_research_closure import RESEARCH_CLOSURE_DASHBOARD_HTML


def test_served_dashboard_consumes_audited_lane_summary():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "dashboardMeta" in html
    assert "lane_executability" in html
    assert "production_evidence_connected_count" in html
    assert "decision_grade_outcome_qualified_count" in html
    assert "paper_execution_capable_count" in html
    assert "paper_execution_capable_lanes" in html
    assert 'id="sourceConnectedCount"' in html
    assert 'id="decisionGradeCount"' in html
    assert 'id="paperCapableCount"' in html
    assert 'id="cardTruthStatus"' in html


def test_cards_use_paper_capability_not_promoted_count_as_execution_truth():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "stat('Paper-capable'" in html
    assert "stat('Executable now'" not in html
    assert "evidenceStep('Paper-capable'" in html
    assert "paperCapableIds.has(r.mechanism_id)" in html
    assert "stale or unavailable projection · fail closed" in html


def test_cards_explain_no_forward_return_without_calling_it_no_data():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "Awaiting outcomes" in html
    assert "No raw signal" in html
    assert "Awaiting history" in html
    assert "Economics rejected" in html
    assert "Evidence incomplete" in html
    assert "Redundancy pending" in html
    assert "Awaiting forward outcomes" in html


def test_provider_probe_schema_is_diagnostic_not_card_authority():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "legacy_provider_admission" in html
    assert "diagnostic only; canonical 13-lane source state controls this card" in html
    assert "r.provider_admission&&!r.provider_admission.authoritative_provider_connected" not in html


def test_card_truth_surfaces_stale_or_cached_projection_fail_closed():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "snapshot?.__stale" in html
    assert "snapshot?.research_projection_stale" in html
    assert "snapshot?.operating_projection_stale" in html
    assert "projection_current_for_execution" in html
    assert "Card data is not current:" in html
    assert "Paper-capable status remains fail-closed" in html


def test_mobile_resize_redraws_cached_chart_without_new_dashboard_request():
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    assert "window.__dashboardHistory=history.snapshots||[]" in html
    assert "window.addEventListener('resize',()=>renderChart(window.__dashboardHistory||[]))" in html
    assert "window.addEventListener('resize',()=>refresh())" not in html
