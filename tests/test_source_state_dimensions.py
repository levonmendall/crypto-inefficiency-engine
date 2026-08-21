from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.executable_operating_certification import (
    AllLaneOperatingCertificationService,
)
from inefficiency_engine.operating_certification import MechanismOperatingStatus
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions


def lane(*, sources, sufficient=False, missing=None, redundancy=False):
    admitted_count = sum(bool(row.get("admitted")) for row in sources)
    return SimpleNamespace(
        source_layer_sufficient=sufficient,
        healthy_source_count=admitted_count,
        source_redundancy_satisfied=redundancy,
        missing_evidence_classes=list(missing or []),
        downstream_evidence_gaps=[],
        sources=sources,
    )


def admitted(source_id="primary"):
    return {
        "source_id": source_id,
        "state": "healthy",
        "healthy": True,
        "fresh": True,
        "admitted": True,
        "authoritative": True,
    }


def test_connected_provider_with_redundancy_gap_is_not_provider_gap():
    result = classify_lane_source_dimensions(
        lane(sources=[admitted()], sufficient=False, redundancy=False)
    )
    assert result.provider_connectivity_state == "healthy"
    assert result.provider_ready is True
    assert result.source_sufficiency_state == "redundancy_gap"
    assert result.source_headline_state == "source_gap"
    assert result.source_layer_sufficient is False


def test_connected_provider_with_missing_evidence_class_is_not_provider_gap():
    result = classify_lane_source_dimensions(
        lane(
            sources=[admitted()],
            sufficient=False,
            missing=["executable_depth"],
            redundancy=True,
        )
    )
    assert result.provider_connectivity_state == "healthy"
    assert result.source_sufficiency_state == "evidence_class_gap"
    assert result.source_headline_state == "source_gap"


def test_stale_provider_is_reported_as_stale_not_missing():
    result = classify_lane_source_dimensions(
        lane(
            sources=[{
                "source_id": "primary",
                "state": "stale",
                "healthy": True,
                "fresh": False,
                "admitted": False,
                "authoritative": True,
            }],
            sufficient=False,
        )
    )
    assert result.provider_connectivity_state == "stale"
    assert result.provider_ready is False
    assert result.source_sufficiency_state == "stale"
    assert result.source_headline_state == "degraded"


def test_failed_provider_has_no_usable_provider_but_is_connectivity_degraded():
    result = classify_lane_source_dimensions(
        lane(
            sources=[{
                "source_id": "primary",
                "state": "failed",
                "healthy": False,
                "fresh": True,
                "admitted": False,
                "authoritative": True,
            }],
            sufficient=False,
        )
    )
    assert result.provider_connectivity_state == "degraded"
    assert result.provider_ready is False
    assert result.source_sufficiency_state == "provider_gap"
    assert result.source_headline_state == "degraded"


def _operating_service(source_lane):
    service = object.__new__(AllLaneOperatingCertificationService)
    service.core = SimpleNamespace(settings=SimpleNamespace(
        operating_certification_min_settled_trials=20,
        operating_certification_min_profitable_rate_lower=0.50,
    ))
    service.source_coverage = SimpleNamespace(lane=lambda mechanism_id: source_lane)
    service.mechanism_execution = SimpleNamespace(
        readiness_summary=lambda: {
            "yield": {
                "current_promoted_candidate_count": 0,
                "full_qualified_cohort_count": 0,
                "incremental_qualified_cohort_count": 0,
            }
        },
        ledger=SimpleNamespace(outcomes=lambda mechanism_id: []),
    )
    service.allocation_certification = SimpleNamespace(
        ledger=SimpleNamespace(outcomes=lambda: [])
    )
    return service


def _existing_yield_status():
    return MechanismOperatingStatus(
        mechanism_id="yield",
        name="Yield",
        state="provider_gap",
        stage="research",
        provider_ready=False,
        primary_reason="legacy provider gap",
        next_action="legacy action",
    )


def test_operating_status_reserves_provider_gap_for_no_usable_provider():
    source_lane = lane(
        sources=[admitted("lido")],
        sufficient=False,
        redundancy=False,
    )
    result = _operating_service(source_lane)._mechanism_status(_existing_yield_status())
    assert result.provider_ready is True
    assert result.state == "collecting"
    assert result.stage == "waiting_for_source:redundancy_gap"
    assert "redundancy" in result.primary_reason
    assert result.state != "provider_gap"


def test_operating_status_keeps_true_missing_provider_as_provider_gap():
    source_lane = lane(sources=[], sufficient=False, redundancy=False)
    result = _operating_service(source_lane)._mechanism_status(_existing_yield_status())
    assert result.provider_ready is False
    assert result.state == "provider_gap"
    assert result.stage == "waiting_for_source:provider_gap"
