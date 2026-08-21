from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService,
)
from inefficiency_engine.operating_certification import MechanismOperatingStatus


def test_recovered_structural_source_does_not_preserve_stale_provider_gap():
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    existing = MechanismOperatingStatus(
        mechanism_id="price_discrepancy",
        name="Price discrepancy",
        state="provider_gap",
        stage="waiting_for_source",
        provider_ready=False,
        authoritative_observation_count=0,
        primary_reason="old provider gap",
        next_action="old",
    )
    lane = SimpleNamespace(
        lane_id="price_discrepancy",
        source_layer_sufficient=True,
        healthy_source_count=2,
        source_redundancy_satisfied=True,
        missing_evidence_classes=[],
        downstream_evidence_gaps=[],
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=True,
        sources=[
            {
                "source_id": "one",
                "state": "healthy",
                "healthy": True,
                "fresh": True,
                "admitted": True,
                "authoritative": True,
                "group": "one",
                "item_count": 5,
            },
            {
                "source_id": "two",
                "state": "healthy",
                "healthy": True,
                "fresh": True,
                "admitted": True,
                "authoritative": True,
                "group": "two",
                "item_count": 6,
            },
        ],
    )
    strategy_rows = [
        {
            "strategy_id": "cex_spot_dislocation",
            "state": "certifying",
            "forward_signal_count": 4,
            "settled_allocator_outcome_count": 4,
            "allocator_realized_profit_usd": 12.0,
            "allocator_mean_net_return_ci_lower": 0.001,
            "allocator_profitable_rate_ci_lower": 0.52,
        }
    ]

    reconciled = service._source_reconciled_status(existing, lane, strategy_rows)

    assert reconciled.provider_ready is True
    assert reconciled.state == "certifying"
    assert reconciled.state != "provider_gap"
    assert reconciled.settled_allocator_outcome_count == 4
    assert "current allocator strategy evidence" in reconciled.primary_reason
