from __future__ import annotations

from inefficiency_engine.read_api_active_volume_deploy import _lane_summary_from_payload
from inefficiency_engine.source_coverage_catalog import LANES


def _payload(rows, *, research_stale=False, operating_stale=False):
    return {
        "mechanisms": {"mechanisms": rows},
        "research_projection_stale": research_stale,
        "operating_projection_stale": operating_stale,
    }


def _row(lane_id: str, *, state="collecting", promoted=0, certified=False):
    return {
        "mechanism_id": lane_id,
        "state": state,
        "current_promoted_count": promoted,
        "profitability_certified": certified,
    }


def test_dashboard_does_not_equate_thirteen_lane_architecture_with_executability():
    rows = [_row(lane_id) for lane_id in LANES]
    summary = _lane_summary_from_payload(_payload(rows))

    assert summary["lane_count"] == 13
    assert summary["architecture_executable_count"] == 13
    assert summary["production_evidence_connected_count"] == 12
    assert summary["decision_grade_outcome_qualified_count"] == 0
    assert summary["paper_execution_capable_count"] == 0
    assert summary["paper_execution_capable_lanes"] == []
    assert summary["all_lanes_paper_execution_capable"] is False


def test_dashboard_requires_decision_grade_state_and_connected_production_evidence():
    rows = [_row(lane_id) for lane_id in LANES]
    rows[0] = _row("price_discrepancy", state="certifying")
    rows[-1] = _row("capital_location_settlement", state="certified", certified=True)

    summary = _lane_summary_from_payload(_payload(rows))

    assert summary["decision_grade_outcome_qualified_count"] == 2
    assert summary["currently_qualified_count"] == 2
    assert summary["paper_execution_capable_count"] == 1
    assert summary["paper_execution_capable_lanes"] == ["price_discrepancy"]
    assert summary["all_lanes_paper_execution_capable"] is False
    assert summary["profitability_certified_count"] == 1


def test_dashboard_fails_closed_on_stale_operating_projection():
    rows = [_row(lane_id, state="certified") for lane_id in LANES]
    summary = _lane_summary_from_payload(
        _payload(rows, operating_stale=True)
    )

    assert summary["decision_grade_outcome_qualified_count"] == 13
    assert summary["projection_current_for_execution"] is False
    assert summary["paper_execution_capable_count"] == 0
    assert summary["paper_execution_capable_lanes"] == []
    assert summary["all_lanes_paper_execution_capable"] is False
