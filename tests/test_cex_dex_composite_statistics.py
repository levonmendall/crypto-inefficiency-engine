from datetime import datetime, timezone

from inefficiency_engine.cex_dex_composite_statistics import build_composite_edge_statistical_qualification
from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_shadow import (
    CexDexCompositeEdgeObservation,
    CexDexCompositeEdgeShadowCycle,
    composite_edge_key,
)
from inefficiency_engine.config import Settings


NOW = datetime.now(timezone.utc)


def evidence() -> CexDexCompositeEvidence:
    return CexDexCompositeEvidence(
        evidence_id="edge-current",
        frontier_id="frontier",
        asset="ETH",
        route_direction="buy_asset",
        target_notional_usd=1000.0,
        route_contiguous_acceptable=True,
        cex_venue="Coinbase",
        cex_symbol="ETH-USD",
        cex_quote_currency="USD",
        cex_reference_price=4000.0,
        route_quote_currency="USDC",
        route_effective_asset_price=3990.0,
        route_quote_notional_usd_proxy=1000.0,
        conversion_depth_quote=None,
        conversion_risk_haircut_bps=2.0,
        cex_taker_fee_bps=10.0,
        gas_cost_bps=5.0,
        gross_edge_after_conversion_depth_bps=67.0,
        net_research_edge_bps=50.0,
        observed_at=NOW,
        evidence_complete=True,
        blocked_reason="research-only",
    )


def cycle(cycle_id: str, current: CexDexCompositeEvidence, *, verification_edge: float = 45.0,
          hurdle_survived: bool = True, survived: bool = True) -> CexDexCompositeEdgeShadowCycle:
    key = composite_edge_key(current)
    retained = verification_edge / current.net_research_edge_bps if survived else None
    observation = CexDexCompositeEdgeObservation(
        cycle_id=cycle_id,
        composite_key=key,
        asset=current.asset,
        route_direction=current.route_direction,
        target_notional_usd=current.target_notional_usd,
        cex_venue=current.cex_venue,
        cex_symbol=current.cex_symbol,
        horizon_seconds=5.0,
        initial_evidence_id=f"{cycle_id}-initial",
        verification_evidence_id=f"{cycle_id}-verification" if survived else None,
        initial_net_edge_bps=current.net_research_edge_bps,
        verification_net_edge_bps=verification_edge if survived else None,
        net_edge_change_bps=verification_edge - current.net_research_edge_bps if survived else None,
        adverse_deterioration_bps=max(0.0, current.net_research_edge_bps - verification_edge) if survived else None,
        retained_edge_fraction=retained,
        initial_above_hurdle=True,
        verification_above_hurdle=hurdle_survived if survived else None,
        hurdle_survived=hurdle_survived if survived else False,
        survived=survived,
        failure_type=None if survived else "CompositeMissing",
        verified_at=NOW,
    )
    return CexDexCompositeEdgeShadowCycle(
        cycle_id=cycle_id,
        started_at=NOW,
        completed_at=NOW,
        horizons_seconds=[5.0],
        min_net_edge_bps=12.0,
        initial_evidence_count=1,
        observations=[observation],
    )


def test_composite_statistics_can_qualify_direct_net_edge_survival():
    current = evidence()
    settings = Settings(
        dex_statistical_reference_horizon_seconds=5.0,
        dex_statistical_min_effective_samples=2,
        dex_statistical_min_tail_samples=2,
        dex_statistical_min_survival_lower_bound=0.20,
        dex_statistical_max_ci_width=1.0,
        dex_statistical_max_p95_deterioration_bps=25.0,
    )

    model = build_composite_edge_statistical_qualification(
        [cycle("a", current, verification_edge=46.0), cycle("b", current, verification_edge=44.0)],
        current,
        settings,
    )

    assert model.effective_sample_count == 2
    assert model.hurdle_survival.successes == 2
    assert model.adverse_tail_sample_count == 2
    assert model.p95_adverse_deterioration_bps is not None
    assert model.p95_adverse_deterioration_bps < 10.0
    assert model.p10_retained_edge_fraction is not None
    assert model.p10_retained_edge_fraction > 0.80
    assert model.statistically_qualified is True
    assert model.allocation_eligible is False


def test_composite_statistics_fail_closed_when_edge_disappears():
    current = evidence()
    settings = Settings(
        dex_statistical_reference_horizon_seconds=5.0,
        dex_statistical_min_effective_samples=2,
        dex_statistical_min_tail_samples=1,
        dex_statistical_min_survival_lower_bound=0.80,
        dex_statistical_max_ci_width=1.0,
        dex_statistical_max_p95_deterioration_bps=25.0,
    )

    model = build_composite_edge_statistical_qualification(
        [cycle("a", current, verification_edge=45.0), cycle("b", current, survived=False)],
        current,
        settings,
    )

    assert model.effective_sample_count == 2
    assert model.hurdle_survival.successes == 1
    assert model.statistically_qualified is False
    assert any("Wilson lower bound" in reason for reason in model.reasons)
