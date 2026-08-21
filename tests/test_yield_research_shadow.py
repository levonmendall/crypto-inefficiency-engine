from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessMechanismExecutionService,
)
from inefficiency_engine.mechanism_execution import (
    MechanismForwardOutcome,
    MechanismForwardTrial,
    MechanismSettlementResult,
    MechanismTrialSpec,
)
from inefficiency_engine.yield_shadow_runtime import (
    YieldResearchShadowMechanismExecutionService,
)


NOW = datetime(2026, 8, 21, 20, 30, tzinfo=timezone.utc)


def _spec() -> MechanismTrialSpec:
    return MechanismTrialSpec(
        mechanism_id="yield",
        cohort_key="yield|Morpho|USDC|lending",
        asset="USDC",
        venues=["Morpho"],
        source_observed_at=NOW,
        holding_hours=24.0,
        capital_usd=5_000.0,
        predicted_net_return=0.0002,
        settlement_payload={
            "protocol": "Morpho",
            "asset": "USDC",
            "entry_net_apy": 0.073,
            "source_evidence_gate": {
                "forward_test_eligible": True,
                "allocation_source_qualified": False,
                "semantic_economics_complete": True,
            },
        },
        conflict_keys=["yield:Morpho:USDC"],
    )


def _shadow_outcome(index: int, value: float) -> MechanismForwardOutcome:
    return MechanismForwardOutcome(
        trial_id=f"yield-shadow-{index}",
        mechanism_id="yield",
        cohort_key="yield|Morpho|USDC|lending",
        asset="USDC",
        matured_at=NOW + timedelta(days=index + 1),
        due_at=NOW + timedelta(days=index + 1),
        predicted_net_return=0.0002,
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


def test_yield_is_opened_only_at_research_boundary(monkeypatch):
    service = YieldResearchShadowMechanismExecutionService.__new__(
        YieldResearchShadowMechanismExecutionService
    )
    monkeypatch.setattr(
        EvidenceVelocityLaneSuccessMechanismExecutionService,
        "discover_specs",
        lambda self, snapshot, *, total_capital_usd: [_spec()],
    )

    rows = service.discover_specs(SimpleNamespace(), total_capital_usd=250_000.0)

    assert len(rows) == 1
    payload = rows[0].settlement_payload
    gate = payload["source_evidence_gate"]
    assert payload["yield_research_shadow"] is True
    assert payload["predicted_return_excludes_uncalibrated_protocol_loss"] is True
    assert gate["semantic_economics_complete"] is False
    assert gate["research_shadow_only"] is True
    assert gate["protocol_risk_calibration_complete"] is False
    assert gate["allocation_authority"] is False


def test_yield_shadow_outcome_is_explicitly_not_settlement_complete_for_capital():
    recorded = []
    service = YieldResearchShadowMechanismExecutionService.__new__(
        YieldResearchShadowMechanismExecutionService
    )
    service.lane_success = SimpleNamespace(
        record_mechanism_outcome=lambda trial, outcome, settlement_detail: recorded.append(
            (outcome, settlement_detail)
        )
    )
    spec = _spec()
    payload = dict(spec.settlement_payload)
    payload["yield_research_shadow"] = True
    trial = MechanismForwardTrial(
        mechanism_id="yield",
        cohort_key=spec.cohort_key,
        asset=spec.asset,
        venues=spec.venues,
        source_observed_at=NOW,
        due_at=NOW + timedelta(hours=24),
        capital_usd=spec.capital_usd,
        predicted_net_return=spec.predicted_net_return,
        predicted_profit_usd=spec.capital_usd * spec.predicted_net_return,
        settlement_payload=payload,
        conflict_keys=spec.conflict_keys,
    )
    settlement = MechanismSettlementResult(
        matured_at=trial.due_at,
        gross_return=0.0002,
        net_return=0.0002,
        settlement_method="realized_yield_accrual_plus_exit_liquidity",
        detail={"exit_liquidity_sufficient": True},
    )

    outcome = service._outcome(trial, settlement)

    assert outcome.settlement_evidence_complete is False
    assert outcome.detail["yield_research_shadow"] is True
    assert outcome.detail["protocol_risk_calibration_complete"] is False
    assert outcome.detail["allocation_grade"] is False
    assert recorded and recorded[0][0].outcome_id == outcome.outcome_id


def test_yield_shadow_statistics_can_accumulate_but_never_allocate():
    rows = [
        _shadow_outcome(0, 0.0010),
        _shadow_outcome(1, 0.0008),
        _shadow_outcome(2, 0.0006),
    ]
    service = YieldResearchShadowMechanismExecutionService.__new__(
        YieldResearchShadowMechanismExecutionService
    )
    service.raw_outcomes = lambda **kwargs: rows

    qualification = service.qualification(rows[0].cohort_key, "yield")

    assert qualification.sample_count == 3
    assert qualification.positive_count == 3
    assert qualification.mean_net_return is not None
    assert qualification.mean_net_return > 0
    assert qualification.allocation_fraction == 0.0
    assert qualification.incremental_eligible is False
    assert qualification.fully_statistically_qualified is False
    assert any("protocol-loss" in blocker for blocker in qualification.blockers)


def test_yield_shadow_spec_cannot_become_candidate():
    service = YieldResearchShadowMechanismExecutionService.__new__(
        YieldResearchShadowMechanismExecutionService
    )
    spec = _spec()
    payload = dict(spec.settlement_payload)
    payload["source_evidence_gate"] = {
        "semantic_economics_complete": False,
        "research_shadow_only": True,
        "allocation_source_qualified": True,
    }
    shadow = spec.model_copy(update={"settlement_payload": payload})

    assert service._candidate_from_spec(shadow) is None
