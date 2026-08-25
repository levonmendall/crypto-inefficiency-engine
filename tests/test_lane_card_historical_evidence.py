from __future__ import annotations


def test_lane_cards_separate_prelive_backfill_from_total_durable_history():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()

    assert "lane-history-ui" in html
    assert "History & backfill evidence" in html
    assert "Strict pre-live backfill and total durable history are separate" in html
    assert "Durable evidence classes" in html
    assert "Durable source records" in html
    assert "Historical candidate raw" in html
    assert "Forward selections" in html
    assert "Strict pre-live backfill" in html
    assert "Total durable history since Aug. 21" in html
    assert "Post-boundary history is visible here but does not retroactively certify strict pre-live coverage." in html
    assert "laneCoverageFor" in html
    assert "durableLaneHistoryFor" in html
    assert "runtime?.detail?.lane_coverage?.lanes" in html
    assert "recovered_source_observations" in html
    assert "recovered_operating_snapshots" in html
    assert "historical_evidence_classes" in html
    assert "missing_historical_evidence_classes" in html
    assert "laneForHistoricalCandidate" in html
    assert "laneForHistoricalFunnel" in html
    assert "strategy_evidence" in html
    assert "mechanism_id" in html

    # The lane display never mutates or reclassifies live qualification/allocation state.
    assert "qualification_authority" not in html.split("<style id=\"lane-history-ui\">")[1].split("</style>", 1)[0]


def test_durable_source_history_prevents_false_empty_lane_message():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "hasDurable" in html
    assert "Trustworthy durable source/operating history is shown separately above." in html
    assert "No trustworthy persisted lane history has been recovered yet." in html


def test_historical_ui_requests_full_bounded_replay_and_durable_lane_history():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "/v3/research/candidate-observatory/history?limit=500" in html
    assert "/v3/dashboard/durable-lane-history" in html
    assert "refreshLaneDurableHistory" in html
    assert "historical_counts_as_forward" not in html


def test_global_history_labels_prelive_and_total_history_separately():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "13-lane strict pre-live backfill certification" in html
    assert "Total durable lane history" in html
    assert "frozen pre-live certification window" in html


def test_unmapped_candidate_history_is_not_guessed_into_a_lane():
    from inefficiency_engine.read_api_lane_history_ui_deploy import lane_history_dashboard_html

    html = lane_history_dashboard_html()
    assert "could not be mapped to a lane from persisted identifiers" in html
    assert "remain visible in the global history section" in html
    assert "return null;" in html


def test_durable_lane_history_route_is_read_only_and_present():
    from inefficiency_engine import read_api_lane_history_ui_deploy as lane_ui

    matches = [
        route
        for route in lane_ui.app.router.routes
        if getattr(route, "path", None) == "/v3/dashboard/durable-lane-history"
    ]
    assert len(matches) == 1
    assert getattr(matches[0], "methods", set()) == {"GET"}


def test_production_liveness_wraps_lane_history_ui():
    from inefficiency_engine import read_api_liveness_deploy as liveness
    from inefficiency_engine import read_api_lane_history_ui_deploy as lane_ui

    assert liveness.app.inner is lane_ui.app
    payload = liveness.liveness_payload()
    assert payload["liveness_database_independent"] is True
    assert payload["live_execution"] is False
