from datetime import datetime, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)


NOW = datetime(2026, 8, 18, 22, 0, tzinfo=timezone.utc)


def opportunity(gross_bps_hour: float = 5.0) -> Opportunity:
    return Opportunity(
        id="opp-1",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=gross_bps_hour,
        modeled_cost_bps=20.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.02,
        net_edge_bps_per_hour=4.0,
        net_annualized_return=3.5,
        observed_at=NOW,
        expires_at=NOW,
    )


def books(*, thin: bool = False, impact: bool = False):
    size = 20.0 if thin else 100.0
    spot_asks = [OrderBookLevel(price=100.0, size=size)]
    perp_bids = [OrderBookLevel(price=101.0, size=size)]
    if impact:
        spot_asks = [OrderBookLevel(price=100.0, size=0.5), OrderBookLevel(price=103.0, size=100.0)]
        perp_bids = [OrderBookLevel(price=101.0, size=0.5), OrderBookLevel(price=98.0, size=100.0)]
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=99.9, size=size)], asks=spot_asks, observed_at=NOW, source="fixture",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=perp_bids, asks=[OrderBookLevel(price=101.1, size=size)], observed_at=NOW, source="fixture",
        ),
    ]


def settings() -> Settings:
    return Settings(
        min_net_annualized_return=0.08,
        max_order_book_age_seconds=30,
        max_order_book_skew_seconds=2,
        capital_tiers_usd=(1000.0, 10000.0),
    )


def test_pair_qualification_uses_equal_base_quantity_on_both_legs():
    result = qualify_opportunity(opportunity(), books(), settings(), now=NOW)
    tier = result.tiers[0]
    assert tier.executable is True
    assert tier.passes_return_hurdle is True
    assert len(tier.leg_estimates) == 2
    assert tier.leg_estimates[0].filled_base_quantity == pytest.approx(tier.leg_estimates[1].filled_base_quantity)
    assert result.max_qualified_notional_usd == 10000.0


def test_pair_qualification_fails_closed_when_one_book_is_missing():
    result = qualify_opportunity(opportunity(), books()[:1], settings(), now=NOW)
    assert all(not tier.executable for tier in result.tiers)
    assert "missing order book" in result.tiers[0].rejection_reason


def test_pair_qualification_rejects_tier_when_depth_is_insufficient():
    result = qualify_opportunity(opportunity(), books(thin=True), settings(), notionals_usd=(1000.0, 50000.0), now=NOW)
    assert result.tiers[0].executable is True
    assert result.tiers[1].executable is False
    assert result.max_qualified_notional_usd == 1000.0


def test_visible_slippage_can_erase_apparent_edge():
    result = qualify_opportunity(opportunity(gross_bps_hour=1.0), books(impact=True), settings(), notionals_usd=(10000.0,), now=NOW)
    tier = result.tiers[0]
    assert tier.executable is True
    assert tier.observed_entry_slippage_bps > 0
    assert tier.passes_return_hurdle is False
    assert tier.net_annualized_return < settings().min_net_annualized_return
