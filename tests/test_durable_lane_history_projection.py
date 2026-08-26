from __future__ import annotations

from inefficiency_engine.durable_lane_history_projection import merge_materialized_lane_coverage
from inefficiency_engine.read_api_durable_history_projection_deploy import (
    DURABLE_HISTORY_PATH,
    app,
    empty_history_payload,
    history_projection_dashboard_html,
)
from inefficiency_engine.source_coverage_catalog import LANES


def _history_shell():
    lanes = {}
    for lane_id, definition in LANES.items():
        required = sorted(str(value) for value in list(definition.get("required") or []))
        lanes[lane_id] = {
            "lane_id": lane_id,
            "history_available": False,
            "evidence_class_history_complete": False,
            "required_evidence_class_count": len(required),
            "recovered_evidence_class_count": 0,
            "recovered_source_observations": 0,
            "recovered_operating_snapshots": 0,
            "earliest_recovered_at": None,
            "latest_recovered_at": None,
            "historical_evidence_classes": [],
            "missing_historical_evidence_classes": required,
            "source_ids": [],
            "source_ledgers": [],
        }
    return {
        "lane_count": len(lanes),
        "lanes_with_durable_history": 0,
        "lanes_without_durable_history": len(lanes),
        "lanes_with_all_required_evidence_classes": 0,
        "lanes": lanes,
    }


def test_overlap_safe_projection_merge_recovers_evidence_without_adding_counts():
    lane_id = "trend_momentum"
    required = sorted(str(value) for value in LANES[lane_id]["required"])
    history = _history_shell()
    history["lanes"][lane_id].update(
        {
            "history_available": True,
            "recovered_source_observations": 10,
            "historical_evidence_classes": [required[0]],
            "source_ids": ["canonical-current"],
            "source_ledgers": ["source_coverage_history"],
            "earliest_recovered_at": "2026-08-25T00:00:00+00:00",
            "latest_recovered_at": "2026-08-26T00:00:00+00:00",
        }
    )
    coverage = {
        "lanes": {
            lane_id: {
                "recovered_source_observations": 7,
                "recovered_operating_snapshots": 4,
                "historical_evidence_classes": required,
                "source_ids": ["historical-source"],
                "source_ledgers": ["provider_statuses"],
                "earliest_recovered_at": "2026-08-21T00:00:00+00:00",
                "latest_recovered_at": "2026-08-25T12:00:00+00:00",
            }
        }
    }

    result = merge_materialized_lane_coverage(history, coverage)
    row = result["lanes"][lane_id]

    assert row["recovered_source_observations"] == 10
    assert row["recovered_operating_snapshots"] == 4
    assert row["historical_evidence_classes"] == required
    assert row["recovered_evidence_class_count"] == len(required)
    assert row["required_evidence_class_count"] == len(required)
    assert row["evidence_class_history_complete"] is True
    assert row["earliest_recovered_at"] == "2026-08-21T00:00:00+00:00"
    assert row["latest_recovered_at"] == "2026-08-26T00:00:00+00:00"
    assert set(row["source_ids"]) == {"canonical-current", "historical-source"}
    assert result["materialized_overlap_safe_merge"] is True
    assert row["candidate_level_history_synthesized"] is False
    assert row["qualification_authority"] is False
    assert row["allocation_authority"] is False
    assert row["live_execution_authority"] is False


def test_fail_soft_history_shell_never_reports_zero_denominators():
    payload = empty_history_payload(reason="projection_not_published_yet")

    assert payload["lane_count"] == 13
    assert payload["history_projection_available"] is False
    for lane_id, definition in LANES.items():
        row = payload["lanes"][lane_id]
        assert row["required_evidence_class_count"] == len(definition["required"])
        assert row["required_evidence_class_count"] > 0
        assert row["recovered_evidence_class_count"] == 0
        assert row["history_available"] is False


def test_projection_route_replaces_request_time_history_route():
    routes = [
        route
        for route in app.router.routes
        if getattr(route, "path", None) == DURABLE_HISTORY_PATH
    ]
    assert len(routes) == 1


def test_dashboard_never_converts_missing_projection_to_zero_history():
    html = history_projection_dashboard_html()

    assert "durableEvidenceLabel" in html
    assert "durableSourceLabel" in html
    assert "'UNAVAILABLE'" in html
    assert "}catch(_e){laneDurableHistory=null}" not in html
    assert "catch(e){if(laneDurableHistory)laneDurableHistory.last_fetch_error=String(e)}" in html
    assert "setInterval(refreshLaneDurableHistory,30000);" in html
    assert "setInterval(refreshLaneDurableHistory,300000);" not in html
