from datetime import datetime, timedelta, timezone

from inefficiency_engine.allocation import AllocationConstraintSet, allocate_qualified_opportunities
from inefficiency_engine.models import (
    CapitalTierQualification, MarketKind, Opportunity, OpportunityExecutability, OpportunityLeg, Side, Strategy,
)

NOW = datetime(2026,8,19,tzinfo=timezone.utc)

def _op(op_id: str, asset: str, venues: tuple[str,str], instruments: list[str]) -> Opportunity:
    return Opportunity(id=op_id,strategy=Strategy.SPOT_PERP_BASIS,asset=asset,
        legs=[OpportunityLeg(venue=venues[0],asset=asset,market_kind=MarketKind.SPOT,side=Side.LONG),
              OpportunityLeg(venue=venues[1],asset=asset,market_kind=MarketKind.PERPETUAL,side=Side.SHORT)],
        gross_edge_bps_per_hour=2,modeled_cost_bps=0,holding_hours=24,safety_buffer_bps_per_hour=0,
        net_edge_bps_per_hour=2,net_annualized_return=.5,observed_at=NOW,expires_at=NOW+timedelta(minutes=1),
        evidence={"canonical_instrument_ids":instruments})

def _exec(op_id: str, ret: float, capital: float, notional: float) -> OpportunityExecutability:
    tier = CapitalTierQualification(opportunity_id=op_id,notional_usd_per_leg=notional,executable=True,
        passes_return_hurdle=True,gross_edge_bps_per_hour=2,static_modeled_cost_bps=0,total_modeled_cost_bps=1,
        net_edge_bps_per_hour=1,net_annualized_return=ret,capital_required_usd=capital)
    return OpportunityExecutability(opportunity_id=op_id,strategy=Strategy.SPOT_PERP_BASIS,asset="BTC",
        observed_at=NOW,tiers=[tier],estimated_capacity_notional_usd=notional,max_qualified_notional_usd=notional)

def test_allocator_only_uses_qualified_capacity_and_preserves_conflicts():
    first = _op("a","BTC",("Coinbase","HlPerp"),["i1","i2"])
    conflict = _op("b","ETH",("Coinbase","Bybit"),["i1","i3"])
    plan = allocate_qualified_opportunities([first,conflict],[_exec("a",.40,20000,10000),_exec("b",.30,20000,10000)],
        AllocationConstraintSet(total_capital_usd=100000,max_venue_fraction=.5,max_asset_fraction=.5,max_allocations=10))
    assert len(plan.allocations) == 1
    assert plan.allocations[0].opportunity_id == "a"
    assert plan.authorizes_execution is False
    assert plan.unused_cash_usd == 80000
    assert any(row["reason"] == "shared instrument conflict" for row in plan.skipped)
