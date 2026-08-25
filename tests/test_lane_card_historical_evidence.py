from __future__ import annotations


def test_lane_cards_include_historical_observatory_evidence_without_authority():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()

    assert "lane-history-ui" in html
    assert "Historical opportunity evidence" in html
    assert "Aug. 21 → live boundary · diagnostic only" in html
    assert "Source observations" in html
    assert "Operating snapshots" in html
    assert "Historical raw" in html
    assert "Forward selections" in html
    assert "Recovered source / operating history" in html
    assert "laneCoverageFor" in html
    assert "runtime?.detail?.lane_coverage?.lanes" in html
    assert "recovered_source_observations" in html
    assert "recovered_operating_snapshots" in html
    assert "historical_evidence_classes" in html
    assert "missing_historical_evidence_classes" in html
    assert "max_forward_signal_count" in html
    assert "max_independent_forward_outcome_count" in html
    assert "laneForHistoricalCandidate" in html
    assert "laneForHistoricalFunnel" in html
    assert "strategy_evidence" in html
    assert "mechanism_id" in html
    assert "source/operating history is separate from candidate funnels" in html

    # The lane display never mutates or reclassifies live qualification/allocation state.
    assert "qualification_authority" not in html.split("<style id=\"lane-history-ui\">")[1].split("</style>", 1)[0]
    assert "historical replay remains a separate diagnostic source" not in html.lower()


def test_source_history_prevents_false_empty_lane_message():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "hasRecoveredCoverage" in html
    assert "No candidate-level historical selections or funnels were persisted for this lane; recovered source / operating history is shown above." in html
    assert "No persisted historical evidence has been recovered for this lane yet." in html


def test_historical_ui_requests_full_bounded_replay_for_lane_mapping():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "/v3/research/candidate-observatory/history?limit=500" in html
    assert "historical_counts_as_forward" not in html
    assert "Historical replay never changes forward samples, qualification, allocation, or execution." in html


def test_unmapped_candidate_history_is_not_guessed_into_a_lane():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "could not be mapped to a lane from persisted identifiers" in html
    assert "remain visible in the global history section" in html
    assert "return null;" in html


def test_production_liveness_wraps_lane_history_ui():
    from inefficiency_engine import read_api_liveness_deploy as liveness
    from inefficiency_engine import read_api_lane_history_ui_deploy as lane_ui

    assert liveness.app.inner is lane_ui.app
    payload = liveness.liveness_payload()
    assert payload["liveness_database_independent"] is True
    assert payload["live_execution"] is False
