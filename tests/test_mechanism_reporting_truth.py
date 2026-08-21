from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.executable_operating_certification import (
    AllLaneOperatingCertificationService,
)
from inefficiency_engine.mechanism_execution import MechanismForwardOutcome
from inefficiency_engine.operating_certification import MechanismOperatingStatus


NOW = datetime(2026, 8, 21, 21, 0, tzinfo=timezone.utc)


def _existing() -> MechanismOperatingStatus:
    return MechanismOperatingStatus(
        mechanism_id="yield",
        name="Yield / staking / lending",
        state="collecting",
        stage="profitability_certifiable",
        provider_ready=True,
        authoritative_observation_count=1,
        economic_candidate_count=1,
        forward_signal_count=0,
        independent_forward_outcome_count=0,
        current_candidate_count=0,
        current_statistically_qualified_count=0,
        current_promoted_count=0,
        settled_allocator_outcome_count=0,
        profitability_certified=False,
        primary_reason="old",
        next_action="old",
        blockers=[],
    )


def _shadow(index: int, value: float) -> MechanismForwardOutcome:
    return MechanismForwardOutcome(
        trial_id=f"shadow-{index}",
        mechanism_id="yield",
        cohort_key="yield|Morpho|USDC|lending",
        asset="USDC",
        matured_at=NOW,
        due_at=NOW,
        predicted_net_return=0.001,
        realized_gross_return=value,
        realized_net_return=value,
        realized_profit_usd=value * 5_000.0,
        profitable=value > 0,
        settlement_method="realized_yield_accrual_plus_exit_liquidity",
        settlement_evidence_complete=False,
        detail={
            "yield_research_shadow": True,
            "protocol_risk_calibration_complete": False,
            "allocation_grade": False,
        },
    )


def test_profitable_yield_shadows_never_appear_allocation_qualified():
    service = AllLaneOperatingCertificationService.__new__(
        AllLaneOperatingCertificationService
    )
    shadows = [_shadow(0, 0.0010), _shadow(1, 0.0012), _shadow(2, 0.0009)]
    ledger = SimpleNamespace(
        outcomes=lambda *, mechanism_id=None: list(shadows),
    )
    service.mechanism_execution = SimpleNamespace(
        ledger=ledger,
        readiness_summary=lambda: {
            "yield": {
                # Deliberately simulate the older generic readiness reader trying to
                # interpret the three profitable shadows as statistically qualified.
                "current_promoted_candidate_count": 1,
                "full_qualified_cohort_count": 1,
                "incremental_qualified_cohort_count": 1,
            }
        },
    )
    lane = SimpleNamespace(
        source_layer_sufficient=True,
        healthy_source_count=1,
        source_redundancy_satisfied=True,
        missing_evidence_classes=[],
        downstream_evidence_gaps=["protocol-loss statistical calibration"],
        sources=[
            {
                "admitted": True,
                "classes": ["yield_rate", "capacity", "exit_liquidity"],
                "economic_fields_complete": False,
                "authoritative": True,
                "group": "morpho",
                "state": "healthy",
            }
        ],
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=True,
    )
    service.source_coverage = SimpleNamespace(lane=lambda mechanism_id: lane)
    service.allocation_certification = SimpleNamespace(
        ledger=SimpleNamespace(outcomes=lambda: [])
    )
    service.core = SimpleNamespace(
        settings=SimpleNamespace(
            operating_certification_min_settled_trials=20,
            operating_certification_min_profitable_rate_lower=0.50,
        )
    )

    status = service._mechanism_status(_existing())

    assert status.state == "collecting"
    assert status.stage == "research_shadow_active_protocol_risk_uncalibrated"
    assert status.forward_signal_count == 3
    assert status.independent_forward_outcome_count == 0
    assert status.current_statistically_qualified_count == 0
    assert status.current_promoted_count == 0
    assert status.profitability_certified is False
    assert "3 research-shadow outcomes" in status.primary_reason
    assert any("protocol-loss economics are uncalibrated" in item for item in status.blockers)
    assert any("excluded from allocation-grade statistics" in item for item in status.blockers)


def test_settlement_complete_outcomes_remain_decision_grade_for_other_mechanisms():
    service = AllLaneOperatingCertificationService.__new__(
        AllLaneOperatingCertificationService
    )
    complete = MechanismForwardOutcome(
        trial_id="vol-complete",
        mechanism_id="volatility",
        cohort_key="vol|Deribit|BTC|short_volatility",
        asset="BTC",
        matured_at=NOW,
        due_at=NOW,
        predicted_net_return=0.002,
        realized_gross_return=0.002,
        realized_net_return=0.001,
        realized_profit_usd=5.0,
        profitable=True,
        settlement_method="option_mark_forward_with_delta_hedge_cost_and_residual_penalty",
        settlement_evidence_complete=True,
    )
    service.mechanism_execution = SimpleNamespace(
        ledger=SimpleNamespace(outcomes=lambda *, mechanism_id=None: [complete]),
        readiness_summary=lambda: {
            "volatility": {
                "current_promoted_candidate_count": 0,
                "full_qualified_cohort_count": 0,
                "incremental_qualified_cohort_count": 0,
            }
        },
    )
    lane = SimpleNamespace(
        source_layer_sufficient=True,
        healthy_source_count=2,
        source_redundancy_satisfied=True,
        missing_evidence_classes=[],
        downstream_evidence_gaps=[],
        sources=[],
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=True,
    )
    service.source_coverage = SimpleNamespace(lane=lambda mechanism_id: lane)
    service.allocation_certification = SimpleNamespace(
        ledger=SimpleNamespace(outcomes=lambda: [])
    )
    service.core = SimpleNamespace(
        settings=SimpleNamespace(
            operating_certification_min_settled_trials=20,
            operating_certification_min_profitable_rate_lower=0.50,
        )
    )
    existing = _existing().model_copy(
        update={
            "mechanism_id": "volatility",
            "name": "Volatility / options risk premia",
        }
    )

    status = service._mechanism_status(existing)

    assert status.independent_forward_outcome_count == 1
    assert status.forward_signal_count == 1
    assert status.state == "collecting"
