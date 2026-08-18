from datetime import datetime, timedelta, timezone

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


def basis_opportunity(*, gross_bps_hour: float = 20.0, spot_side: Side = Side.LONG) -> Opportunity:
    return Opportunity(
        id="economic-cost-test",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=spot_side),
            OpportunityLeg(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=gross_bps_hour,
        modeled_cost_bps=20.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=gross_bps_hour,
        net_annualized_return=1.0,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=2),
    )


def books(size: float = 1000.0, observed_at=NOW):
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=99.9, size=size)],
            asks=[OrderBookLevel(price=100.0, size=size)],
            observed_at=observed_at, source="fixture",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=[OrderBookLevel(price=101.0, size=size)],
            asks=[OrderBookLevel(price=101.1, size=size)],
            observed_at=observed_at, source="fixture",
        ),
    ]


def test_default_fee_model_uses_conservative_public_taker_rates_and_capital_adjustment():
    settings = Settings(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_recovery_buffer_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
    )
    result = qualify_opportunity(basis_opportunity(), books(), settings, now=NOW)
    tier = result.tiers[0]
    assert tier.executable is True
    # Coinbase: 60 bps each way = 120. Hyperliquid: 4.5 each way = 9.
    assert tier.venue_roundtrip_fee_bps == 129.0
    assert tier.total_modeled_cost_bps >= 129.0
    assert tier.capital_required_usd == 2000.0
    assert tier.capital_multiple == 2.0
    assert tier.net_annualized_return == tier.leg_notional_net_annualized_return / 2.0


def test_book_age_and_expected_hedge_latency_are_charged_as_risk_cost():
    settings = Settings(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.0,
        expected_hedge_latency_ms=1000.0,
        latency_risk_bps_per_second=2.0,
        hedge_recovery_buffer_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
    )
    aged = NOW - timedelta(seconds=3)
    tier = qualify_opportunity(basis_opportunity(), books(observed_at=aged), settings, now=NOW).tiers[0]
    assert tier.latency_risk_bps == 8.0  # (3 seconds old + 1 second expected hedge latency) * 2


def test_hedge_liquidity_reserve_can_reject_a_fillable_trade():
    settings = Settings(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.25,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_recovery_buffer_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
    )
    thin = books(size=10.0)
    tier = qualify_opportunity(basis_opportunity(), thin, settings, now=NOW).tiers[0]
    # About 9.9 ETH is enough to fill $1K, but not enough to preserve a 1.25x reserve.
    assert tier.executable is False
    assert "hedge liquidity reserve" in tier.rejection_reason


def test_short_spot_fails_closed_without_borrow_cost():
    settings = Settings(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_recovery_buffer_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
    )
    tier = qualify_opportunity(basis_opportunity(spot_side=Side.SHORT), books(), settings, now=NOW).tiers[0]
    assert tier.executable is False
    assert "borrow cost unavailable" in tier.rejection_reason
