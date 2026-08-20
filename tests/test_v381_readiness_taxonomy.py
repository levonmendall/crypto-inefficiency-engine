from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.profit_coverage import build_profit_coverage_summary


def test_v381_readiness_taxonomy_tracks_current_capabilities_without_lowering_gates():
    summary = build_profit_coverage_summary(
        version="3.8.1",
        alpha_families={
            "directional_time_series",
            "directional_reversal",
            "cross_sectional_relative_value",
        },
    )
    assert summary.mechanism_count == 13
    assert summary.taxonomy_version == "2026-08-20-v3.8.1"
    by_id = {row.mechanism_id: row for row in summary.mechanisms}

    discrepancy = by_id["price_discrepancy"]
    assert discrepancy.stage == "profitability_certifiable"
    assert discrepancy.profitability_certification_available
    assert any(
        "CEX↔DEX amount-specific persisted requalification settlement" in item
        for item in discrepancy.implemented_components
    )
    assert not any(
        "allocator-level realized settlement" in blocker
        for blocker in discrepancy.blockers
    )

    carry = by_id["carry"]
    assert carry.stage == "profitability_certifiable"
    assert carry.profitability_certification_available
    assert any(
        "canonical visible-L2 two-leg settlement" in item
        for item in carry.implemented_components
    )
    assert any(
        "observed perpetual funding accrual" in item
        for item in carry.implemented_components
    )

    trend = by_id["trend_momentum"]
    assert trend.stage == "profitability_certifiable"
    assert any("7/30/90/180-day" in item for item in trend.implemented_components)
    assert any("halving" in item.lower() for item in trend.implemented_components)
    assert not any("multi-horizon trend ensemble" in item for item in trend.missing_components)
    assert not any("perpetual-short" in blocker for blocker in trend.blockers)

    # The strategy id remains unique, so forward qualification is still performed
    # per strategy/asset/direction even though the 13-lane view classifies both
    # trend implementations under the same economic mechanism.
    assert CycleAwareMultiHorizonTrendStrategy.strategy_id == "cycle_aware_multi_horizon_trend_v1"
    assert CycleAwareMultiHorizonTrendStrategy.family == "directional_time_series"

    settings = Settings()
    assert settings.alpha_min_forward_samples == 30
    assert settings.alpha_min_hit_rate_lower_bound == 0.50
    assert settings.alpha_min_regimes == 2
    assert settings.alpha_evidence_every_cycles == 10
