from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaForwardOutcome
from inefficiency_engine.config import Settings
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.disposable_research_worker import (
    AllocationForwardCertificationService as ProductionAllocationCertificationService,
    OperatingCertificationService as ProductionOperatingCertificationService,
    UnifiedPaperAllocatorService as ProductionAllocatorService,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.evidence_velocity import (
    _stagnation_remediation,
    provisional_forward_positive,
)
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.executable_alpha_factory import ExecutableExpandedAlphaFactoryService
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.lightweight_portfolio_worker import (
    CanonicalPaperPortfolioService,
    CanonicalPortfolioAllocatorService,
)
from inefficiency_engine.mechanism_execution import MechanismForwardOutcome, MechanismTrialSpec
from inefficiency_engine.models import MarketKind
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane


NOW = datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc)


def core():
    return SimpleNamespace(settings=Settings())


def _record_source(
    plane: SourceCoveragePlane,
    *,
    source_id: str,
    lane_id: str,
    classes: list[str],
    observed_at: datetime = NOW,
):
    plane.record(
        SourceCoverageObservation(
            source_id=source_id,
            lane_id=lane_id,
            observed_at=observed_at,
            healthy=True,
            item_count=10,
            evidence_classes=classes,
            authoritative=True,
            commercial_use_permitted=True,
            point_in_time=True,
            economic_fields_complete=True,
            forward_testable_evidence=True,
        )
    )


def test_one_authoritative_source_can_learn_but_cannot_allocate(tmp_path):
    store = EvidenceStore(tmp_path / "source-separation.sqlite3")
    plane = SourceCoveragePlane(store)
    _record_source(
        plane,
        source_id="coinbase-market",
        lane_id="trend_momentum",
        classes=["market_history", "execution_costs"],
    )

    one = plane.snapshot(now=NOW)
    lane = next(row for row in one.lanes if row.lane_id == "trend_momentum")
    assert lane.research_eligible is True
    assert lane.forward_test_eligible is True
    assert lane.allocation_source_qualified is False
    assert lane.source_layer_sufficient is False
    assert lane.source_state == "concentration_risk"

    candidate_gate = plane.candidate_sufficiency(
        "trend_momentum",
        required_evidence_classes=["market_history", "execution_costs"],
        primary_groups={"coinbase"},
        now=NOW,
    )
    assert candidate_gate.forward_test_eligible is True
    assert candidate_gate.allocation_source_qualified is False

    _record_source(
        plane,
        source_id="kraken-market",
        lane_id="trend_momentum",
        classes=["market_history", "execution_costs"],
    )
    two = plane.snapshot(now=NOW)
    lane = next(row for row in two.lanes if row.lane_id == "trend_momentum")
    assert lane.allocation_source_qualified is True
    assert lane.source_layer_sufficient is True


def test_candidate_specific_primary_source_prevents_cross_venue_false_admission(tmp_path):
    store = EvidenceStore(tmp_path / "candidate-source.sqlite3")
    plane = SourceCoveragePlane(store)
    for source_id in ("coinbase-market", "kraken-market"):
        _record_source(
            plane,
            source_id=source_id,
            lane_id="trend_momentum",
            classes=["market_history", "execution_costs"],
        )

    gate = plane.candidate_sufficiency(
        "trend_momentum",
        required_evidence_classes=["market_history", "execution_costs"],
        primary_groups={"bybit"},
        now=NOW,
    )
    assert gate.primary_group_satisfied is False
    assert gate.research_eligible is False
    assert gate.forward_test_eligible is False
    assert gate.allocation_source_qualified is False


def test_source_freshness_depends_on_evidence_class(tmp_path):
    store = EvidenceStore(tmp_path / "freshness.sqlite3")
    plane = SourceCoveragePlane(store)
    _record_source(
        plane,
        source_id="coinbase-market",
        lane_id="trend_momentum",
        classes=["market_history", "execution_costs"],
        observed_at=NOW,
    )
    _record_source(
        plane,
        source_id="morpho-markets",
        lane_id="fundamental_onchain",
        classes=["protocol_fundamentals"],
        observed_at=NOW,
    )

    snapshot = plane.snapshot(now=NOW + timedelta(hours=1))
    trend = next(row for row in snapshot.lanes if row.lane_id == "trend_momentum")
    fundamental = next(row for row in snapshot.lanes if row.lane_id == "fundamental_onchain")
    coinbase = next(row for row in trend.sources if row["source_id"] == "coinbase-market")
    morpho = next(row for row in fundamental.sources if row["source_id"] == "morpho-markets")
    assert coinbase["fresh"] is True
    assert coinbase["freshness_ttl_seconds"] >= 21_600.0
    assert morpho["fresh"] is True
    assert morpho["freshness_ttl_seconds"] >= 86_400.0


def test_mechanism_forward_trial_can_learn_before_source_redundancy_but_not_promote(tmp_path):
    store = EvidenceStore(tmp_path / "mechanism-source.sqlite3")
    service = GovernedMechanismExecutionService(core(), store)
    cohort = "yield|Morpho|USDC|lending"
    _record_source(
        service.source_plane,
        source_id="morpho-markets",
        lane_id="yield",
        classes=["yield_rate", "capacity", "exit_liquidity"],
    )
    coverage = service.source_plane.snapshot(now=NOW)
    gate = service._source_gate(
        mechanism_id="yield",
        venues=["Morpho"],
        coverage=coverage,
    )
    assert gate.forward_test_eligible is True
    assert gate.allocation_source_qualified is False

    for index in range(3):
        service.ledger.record_outcome(
            MechanismForwardOutcome(
                trial_id=f"trial-{index}",
                mechanism_id="yield",
                cohort_key=cohort,
                asset="USDC",
                matured_at=NOW + timedelta(hours=index + 1),
                due_at=NOW + timedelta(hours=index + 1),
                predicted_net_return=0.002,
                realized_gross_return=0.002,
                realized_net_return=0.002,
                realized_profit_usd=2.0,
                profitable=True,
                settlement_method="test",
            )
        )

    spec = MechanismTrialSpec(
        mechanism_id="yield",
        cohort_key=cohort,
        asset="USDC",
        venues=["Morpho"],
        source_observed_at=NOW,
        holding_hours=24.0,
        capital_usd=1000.0,
        predicted_net_return=0.002,
        settlement_payload={
            "source_evidence_gate": {
                "forward_test_eligible": True,
                "allocation_source_qualified": False,
            }
        },
    )
    assert service.qualification(cohort, "yield").incremental_eligible is True
    assert service._candidate_from_spec(spec) is None

    _record_source(
        service.source_plane,
        source_id="lido-yield",
        lane_id="yield",
        classes=["yield_rate"],
    )
    coverage = service.source_plane.snapshot(now=NOW)
    qualified_gate = service._source_gate(
        mechanism_id="yield",
        venues=["Morpho"],
        coverage=coverage,
    )
    assert qualified_gate.allocation_source_qualified is True
    qualified_spec = spec.model_copy(
        update={
            "settlement_payload": {
                "source_evidence_gate": {
                    "forward_test_eligible": True,
                    "allocation_source_qualified": True,
                }
            }
        }
    )
    assert service._candidate_from_spec(qualified_spec) is not None


def _alpha_candidate(service: ExecutableExpandedAlphaFactoryService) -> AlphaCandidate:
    manifest = service._base_registry.manifests()[0]
    return AlphaCandidate(
        candidate_id="candidate-btc",
        strategy_id=manifest.strategy_id,
        family=manifest.family,
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=60_000.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.001,
        expected_net_return=0.009,
        expected_profit_usd=90.0,
        notional_usd=10_000.0,
        capital_required_usd=10_000.0,
        confidence_score=0.8,
        regime="normal",
    )


def _record_alpha_outcome(
    service: ExecutableExpandedAlphaFactoryService,
    candidate: AlphaCandidate,
    *,
    asset: str,
    index: int,
    regime: str,
):
    observed = NOW + timedelta(hours=6 * index)
    service.ledger.record_outcome(
        AlphaForwardOutcome(
            signal_id=f"{asset}-{index}",
            strategy_id=candidate.strategy_id,
            family=candidate.family,
            asset=asset,
            direction=candidate.direction,
            venue="Coinbase",
            market_kind=MarketKind.SPOT,
            symbol=f"{asset}-USD",
            observed_at=observed,
            due_at=observed + timedelta(hours=6),
            matured_at=observed + timedelta(hours=6),
            horizon_hours=6.0,
            regime=regime,
            predicted_net_return=0.01,
            entry_price=100.0,
            exit_price=101.0,
            realized_gross_return=0.01,
            realized_net_return=0.009,
            correct_direction=True,
        )
    )


def test_cross_asset_pooling_accelerates_effective_samples_but_requires_local_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "pooled-alpha.sqlite3")
    service = ExecutableExpandedAlphaFactoryService(core(), store)
    candidate = _alpha_candidate(service)

    for index in range(3):
        _record_alpha_outcome(
            service,
            candidate,
            asset="BTC",
            index=index,
            regime="normal" if index < 2 else "high_vol",
        )
    local_only = service.qualification(candidate)
    assert local_only.sample_count == 3
    assert local_only.statistically_qualified is False

    for asset_offset, asset in enumerate(("ETH", "SOL", "AVAX"), start=1):
        for index in range(26):
            _record_alpha_outcome(
                service,
                candidate,
                asset=asset,
                index=100 * asset_offset + index,
                regime="normal" if index % 2 == 0 else "high_vol",
            )
    pooled = service.qualification(candidate)
    assert pooled.sample_count >= service.settings.alpha_min_forward_samples
    assert pooled.statistically_qualified is True
    assert pooled.paper_allocation_authority is True


def test_executable_alpha_refinements_are_on_the_actual_fast_registry(tmp_path):
    store = EvidenceStore(tmp_path / "registry.sqlite3")
    service = ExecutableExpandedAlphaFactoryService(core(), store)
    ids = {item.strategy_id for item in service._base_registry.manifests()}
    assert "mean_reversion_cross_venue_residual_v1" in ids
    assert "mean_reversion_multi_horizon_v1" in ids
    assert "public_trade_flow_lead_lag_v1" in ids
    assert "onchain_factor_breadth_v1" in ids


def test_production_disposable_runtime_uses_integrated_all_lane_services():
    assert issubclass(DisposableExpandedAlphaFactoryService, AllLaneEvidenceFactoryService)
    assert ProductionAllocatorService is EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService
    assert (
        ProductionAllocationCertificationService
        is EvidenceVelocityLaneSuccessAllocationForwardCertificationService
    )
    assert ProductionOperatingCertificationService is EvidenceVelocityAllLaneOperatingCertificationService


def test_lightweight_portfolio_runtime_preserves_integrated_all_lane_settlement():
    assert CanonicalPortfolioAllocatorService is EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService
    assert (
        CanonicalPaperPortfolioService
        is EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService
    )


def test_provisional_forward_state_is_diagnostic_only():
    assert provisional_forward_positive(
        outcome_count=3,
        mean_net_return=0.001,
        hit_rate=2 / 3,
    ) is True
    assert provisional_forward_positive(
        outcome_count=2,
        mean_net_return=0.01,
        hit_rate=1.0,
    ) is False


def test_stagnation_controller_repairs_engineering_gaps_but_never_weakens_bad_economics():
    poor_action, poor_boost = _stagnation_remediation(
        {
            "state": "poor_economics",
            "authoritative_observation_count": 100,
            "forward_signal_count": 20,
            "independent_forward_outcome_count": 20,
        }
    )
    provider_action, provider_boost = _stagnation_remediation(
        {
            "state": "provider_gap",
            "stage": "waiting_for_source:provider_gap",
        }
    )
    assert poor_action == "observe_only_poor_economics"
    assert poor_boost == 0.0
    assert provider_action == "prioritize_missing_or_stale_authoritative_source"
    assert provider_boost > 0.0
