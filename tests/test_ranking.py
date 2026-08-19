from datetime import datetime, timedelta, timezone

from inefficiency_engine.models import (
    CapitalTierQualification,
    MarketKind,
    Opportunity,
    OpportunityExecutability,
    OpportunityLeg,
    Side,
    Strategy,
)
from inefficiency_engine.ranking import rank_qualified_opportunities


NOW = datetime(2026, 8, 19, 0, 20, tzinfo=timezone.utc)


def opportunity(opportunity_id: str, strategy: Strategy, asset: str) -> Opportunity:
    return Opportunity(
        id=opportunity_id,
        strategy=strategy,
        asset=asset,
        legs=[
            OpportunityLeg(venue="VenueA", asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.LONG),
            OpportunityLeg(venue="VenueB", asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=10.0,
        modeled_cost_bps=1.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.1,
        net_edge_bps_per_hour=8.0,
        net_annualized_return=0.5,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        evidence={
            "detector_module": strategy.value,
            "canonical_asset_id": f"crypto:asset:{asset}",
            "canonical_instrument_ids": [f"instrument:{asset}:a", f"instrument:{asset}:b"],
        },
    )


def execution(opportunity_id: str, net_return: float, capacity: float, *, passes: bool = True) -> OpportunityExecutability:
    tier = CapitalTierQualification(
        opportunity_id=opportunity_id,
        notional_usd_per_leg=1000.0,
        executable=True,
        passes_return_hurdle=passes,
        gross_edge_bps_per_hour=10.0,
        static_modeled_cost_bps=1.0,
        capital_required_usd=2000.0,
        total_modeled_cost_bps=5.0,
        net_edge_bps_per_hour=8.0,
        net_annualized_return=net_return,
    )
    return OpportunityExecutability(
        opportunity_id=opportunity_id,
        strategy=Strategy.FUNDING_DISPERSION,
        asset="BTC",
        observed_at=NOW,
        tiers=[tier],
        max_qualified_notional_usd=1000.0 if passes else 0.0,
        visible_depth_ceiling_usd=capacity,
        estimated_capacity_notional_usd=capacity,
    )


def test_ranking_compares_qualified_opportunities_on_common_capital_adjusted_basis():
    opportunities = [
        opportunity("funding", Strategy.FUNDING_DISPERSION, "BTC"),
        opportunity("basis", Strategy.SPOT_PERP_BASIS, "ETH"),
        opportunity("rejected", Strategy.SPOT_PERP_BASIS, "SOL"),
    ]
    executions = [
        execution("funding", 0.22, 50000.0),
        execution("basis", 0.31, 10000.0),
        execution("rejected", 0.80, 100000.0, passes=False),
    ]

    ranked = rank_qualified_opportunities(opportunities, executions)

    assert [item.opportunity_id for item in ranked] == ["basis", "funding"]
    assert [item.rank for item in ranked] == [1, 2]
    assert ranked[0].rank_basis == "capital_adjusted_net_annualized_return"
    assert ranked[0].rank_score == 0.31
    assert ranked[0].estimated_capacity_notional_usd == 10000.0
    assert ranked[0].canonical_asset_id == "crypto:asset:ETH"
    assert all(item.paper_only for item in ranked)
