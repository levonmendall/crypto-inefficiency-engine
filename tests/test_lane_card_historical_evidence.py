from __future__ import annotations


def test_lane_cards_include_historical_observatory_evidence_without_authority():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()

    assert "lane-history-ui" in html
    assert "Historical opportunity evidence" in html
    assert "Aug. 21 → live boundary · diagnostic only" in html
    assert "Historical raw" in html
    assert "Historical emitted" in html
    assert "Forward selections" in html
    assert "Hurdle-clearing snapshots" in html
    assert "All mapped historical evidence" in html
    assert "laneForHistoricalCandidate" in html
    assert "laneForHistoricalFunnel" in html
    assert "strategy_evidence" in html
    assert "mechanism_id" in html
    assert "mapped by persisted lane/strategy identifiers" in html

    # The lane display never mutates or reclassifies live qualification/allocation state.
    assert "qualification_authority" not in html.split("<style id=\"lane-history-ui\">")[1].split("</style>", 1)[0]
    assert "historical replay remains a separate diagnostic source" not in html.lower()


def test_historical_ui_requests_full_bounded_replay_for_lane_mapping():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "/v3/research/candidate-observatory/history?limit=500" in html
    assert "historical_counts_as_forward" not in html
    assert "never changes forward samples, qualification, allocation, or execution" in html


def test_unmapped_history_is_not_guessed_into_a_lane():
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
