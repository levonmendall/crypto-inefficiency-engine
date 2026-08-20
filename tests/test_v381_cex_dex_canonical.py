from datetime import datetime, timedelta, timezone

from inefficiency_engine.cex_dex_canonical import (
    CEX_DEX_MARKET_KIND,
    CexDexCanonicalAllocationCertificationService,
    _cex_dex_candidate_has_canonical_settlement,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.profit_coverage import build_profit_coverage_summary
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperCandidate


def _candidate(observed_at: datetime) -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id="cex-dex:abc123",
        family="cex_dex",
        strategy="cex_dex",
        asset="ETH",
        venues=["Coinbase", "DEX:ethereum"],
        capital_required_usd=20_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=20.0,
        expected_return_on_reserved_capital=0.001,
        modeled_holding_hours=5.0 / 3600.0,
        source_return_metric="conservative_capture_edge_bps",
        source_return_value=20.0,
        exposure_kind="market_neutral",
        source_observed_at=observed_at,
        instrument_symbol="ETH-USD",
        instrument_market_kind=CEX_DEX_MARKET_KIND,
        entry_reference_price=3000.0,
        modeled_non_slippage_cost_bps=8.0,
        modeled_safety_buffer_bps=2.0,
        capital_multiple=2.0,
        conflict_keys=["cex-dex-composite:abc123"],
        evidence_id="entry-evidence",
        allocation_eligible=True,
        executable_eligible=False,
        paper_only=True,
    )


def test_cex_dex_candidate_has_canonical_settlement_contract():
    candidate = _candidate(datetime(2026, 8, 20, 20, 0, tzinfo=timezone.utc))
    assert _cex_dex_candidate_has_canonical_settlement(candidate)


def test_cex_dex_allocation_builds_supported_forward_trial():
    observed_at = datetime(2026, 8, 20, 20, 0, tzinfo=timezone.utc)
    candidate = _candidate(observed_at)
    allocation = UnifiedPaperAllocation(
        candidate_id=candidate.candidate_id,
        family=candidate.family,
        strategy=candidate.strategy,
        asset=candidate.asset,
        venues=candidate.venues,
        capital_required_usd=candidate.capital_required_usd,
        notional_usd_per_leg=candidate.notional_usd_per_leg,
        expected_profit_usd_per_deployment=candidate.expected_profit_usd_per_deployment,
        expected_return_on_reserved_capital=candidate.expected_return_on_reserved_capital,
        modeled_holding_hours=candidate.modeled_holding_hours,
        source_return_metric=candidate.source_return_metric,
        source_return_value=candidate.source_return_value,
        exposure_kind=candidate.exposure_kind,
        source_observed_at=candidate.source_observed_at,
        instrument_symbol=candidate.instrument_symbol,
        instrument_market_kind=candidate.instrument_market_kind,
        entry_reference_price=candidate.entry_reference_price,
        modeled_non_slippage_cost_bps=candidate.modeled_non_slippage_cost_bps,
        modeled_safety_buffer_bps=candidate.modeled_safety_buffer_bps,
        capital_multiple=candidate.capital_multiple,
        evidence_id=candidate.evidence_id,
        paper_only=True,
    )
    trial = CexDexCanonicalAllocationCertificationService.trial_from_allocation(
        allocation,
        plan_observed_at=observed_at,
    )
    assert trial.settlement_supported
    assert (
        trial.settlement_method
        == CexDexCanonicalAllocationCertificationService.CEX_DEX_SETTLEMENT_METHOD
    )
    assert trial.settlement_blocker is None
    assert trial.due_at == observed_at + timedelta(seconds=5)
    assert trial.cohort_key == "cex_dex|cex-dex:abc123"


def test_readiness_taxonomy_reflects_v381_capabilities_without_threshold_changes():
    summary = build_profit_coverage_summary(
        version="3.8.1",
        alpha_families={"directional_time_series", "directional_reversal"},
    )
    assert summary.mechanism_count == 13
    by_id = {row.mechanism_id: row for row in summary.mechanisms}

    trend = by_id["trend_momentum"]
    assert trend.stage == "profitability_certifiable"
    assert any("7/30/90/180-day" in item for item in trend.implemented_components)
    assert any("halving" in item.lower() for item in trend.implemented_components)
    assert not any("multi-horizon trend ensemble" in item for item in trend.missing_components)
    assert not any("perpetual-short" in item for item in trend.blockers)

    carry = by_id["carry"]
    assert carry.stage == "profitability_certifiable"
    assert carry.profitability_certification_available
    assert any("observed perpetual funding" in item for item in carry.implemented_components)

    discrepancy = by_id["price_discrepancy"]
    assert discrepancy.stage == "profitability_certifiable"
    assert discrepancy.profitability_certification_available
    assert any("CEX↔DEX amount-specific" in item for item in discrepancy.implemented_components)

    assert CycleAwareMultiHorizonTrendStrategy.family == "directional_time_series"
    settings = Settings()
    assert settings.alpha_min_forward_samples == 30
    assert settings.alpha_min_hit_rate_lower_bound == 0.50
    assert settings.alpha_min_regimes == 2
    assert settings.alpha_evidence_every_cycles == 10
