from inefficiency_engine.dashboard_resilience import RESILIENT_DASHBOARD_HTML


def test_served_dashboard_binds_audited_lane_summary_to_cards():
    html = RESILIENT_DASHBOARD_HTML

    assert "const dashboardMeta=await dashboardSnapshot" in html
    assert "dashboardMeta.lane_executability" in html
    assert "production_evidence_connected_count" in html
    assert "decision_grade_outcome_qualified_count" in html
    assert "paper_execution_capable_count" in html
    assert "paper_execution_capable_lanes" in html
    assert 'id="sourceConnectedCount"' in html
    assert 'id="decisionGradeCount"' in html
    assert 'id="paperCapableCount"' in html
    assert 'id="cardTruthStatus"' in html


def test_evidence_cards_use_paper_capability_not_promoted_count_as_execution_truth():
    html = RESILIENT_DASHBOARD_HTML

    assert "stat('Paper-capable'" in html
    assert "stat('Executable now'" not in html
    assert "evidenceStep('Paper-capable'" in html
    assert "paperCapableIds.has(r.mechanism_id)" in html
    assert "stale projection · fail closed" in html


def test_card_truth_surfaces_stale_or_cached_projection_fail_closed():
    html = RESILIENT_DASHBOARD_HTML

    assert "snapshot?.__stale" in html
    assert "snapshot?.research_projection_stale" in html
    assert "snapshot?.operating_projection_stale" in html
    assert "projection_current_for_execution" in html
    assert "Card data is not current:" in html
    assert "Paper-capable lane status remains fail-closed" in html


def test_card_truth_keeps_single_snapshot_request_contract():
    html = RESILIENT_DASHBOARD_HTML

    assert "/v3/dashboard/snapshot" in html
    assert "getJSON('/v3/portfolio/canonical')" not in html
    assert "safeJSON('/v3/portfolio/positions'" not in html
