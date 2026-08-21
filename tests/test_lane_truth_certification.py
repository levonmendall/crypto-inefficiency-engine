from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService,
)
from inefficiency_engine.operating_certification import MechanismOperatingStatus


class _Coverage:
    def __init__(self, lane):
        self._lane = lane

    def lane(self, mechanism_id: str):
        assert mechanism_id == self._lane.lane_id
        return self._lane


def _lane(
    lane_id: str,
    *,
    missing: list[str] | None = None,
    forward: bool = True,
    allocation: bool = True,
    redundancy: bool = True,
):
    return SimpleNamespace(
        lane_id=lane_id,
        source_layer_sufficient=allocation,
        healthy_source_count=1 if not allocation else 2,
        source_redundancy_satisfied=redundancy,
        research_eligible=True,
        forward_test_eligible=forward,
        allocation_source_qualified=allocation,
        missing_evidence_classes=list(missing or []),
        downstream_evidence_gaps=[],
        sources=[{"admitted": True, "state": "healthy"}],
    )


def _status(mechanism_id: str, *, state: str = "statistical_failure"):
    return MechanismOperatingStatus(
        mechanism_id=mechanism_id,
        name=mechanism_id,
        state=state,
        stage="profitability_certifiable",
        provider_ready=True,
        current_candidate_count=0,
        current_statistically_qualified_count=0,
        current_promoted_count=0,
        primary_reason="legacy family-level conclusion",
        next_action="legacy",
    )


def _service(lane):
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    service.source_coverage = _Coverage(lane)
    return service


def test_one_failed_strategy_cannot_condemn_lane_while_another_is_collecting():
    service = _service(_lane("trend_momentum"))
    existing = _status("trend_momentum")
    rows = [
        {
            "state": "statistical_failure",
            "forward_signal_count": 40,
            "candidate_local_forward_outcome_count": 30,
            "independent_forward_outcome_count": 30,
        },
        {
            "state": "collecting",
            "forward_signal_count": 5,
            "candidate_local_forward_outcome_count": 2,
            "independent_forward_outcome_count": 8,
        },
    ]

    result = service._alpha_lane_status(existing, rows)

    assert result.state == "collecting"
    assert "best still-viable strategy" in result.primary_reason
    assert result.independent_forward_outcome_count == 8


def test_connected_provider_with_missing_evidence_class_is_not_provider_gap():
    service = _service(
        _lane(
            "fundamental_onchain",
            missing=["protocol_fundamentals"],
            forward=False,
            allocation=False,
            redundancy=False,
        )
    )
    existing = _status("fundamental_onchain", state="provider_gap")

    result = service._alpha_lane_status(existing, [])

    assert result.state == "collecting"
    assert result.provider_ready is True
    assert result.stage == "waiting_for_source:evidence_class_gap"
    assert "protocol_fundamentals" in result.blockers
    assert "poor economics" in result.primary_reason


def test_capital_location_reports_missing_transfer_producer_as_settlement_blocked():
    service = _service(
        _lane(
            "capital_location_settlement",
            missing=["transfer_costs", "transfer_latency"],
            forward=False,
            allocation=False,
            redundancy=False,
        )
    )
    existing = _status("capital_location_settlement", state="collecting")

    result = service._capital_location_truth(existing)

    assert result.state == "settlement_blocked"
    assert result.stage == "upstream_evidence_producer_missing"
    assert "transfer telemetry producer is missing" in result.blockers[0]
    assert "historical location scores alone cannot authorize a trial" in result.primary_reason
