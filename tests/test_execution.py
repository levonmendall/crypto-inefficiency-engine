import pytest

from inefficiency_engine.execution import InsufficientDepthError, estimate_market_order, max_executable_notional
from inefficiency_engine.models import MarketKind, OrderBookLevel, OrderBookSnapshot, TradeSide


def book() -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="Test",
        asset="BTC",
        market_kind=MarketKind.PERPETUAL,
        symbol="BTC",
        bids=[OrderBookLevel(price=99, size=1), OrderBookLevel(price=98, size=2)],
        asks=[OrderBookLevel(price=101, size=1), OrderBookLevel(price=102, size=2)],
        source="fixture",
    )


def test_depth_aware_buy_vwap_and_slippage():
    estimate = estimate_market_order(book(), TradeSide.BUY, 203)
    assert estimate.filled_notional_usd == pytest.approx(203)
    assert estimate.base_quantity == pytest.approx(2)
    assert estimate.average_price == pytest.approx(101.5)
    assert estimate.slippage_bps == pytest.approx((101.5 / 101 - 1) * 10_000)
    assert estimate.levels_consumed == 2


def test_insufficient_depth_fails_closed():
    assert max_executable_notional(book(), TradeSide.BUY) == pytest.approx(305)
    with pytest.raises(InsufficientDepthError):
        estimate_market_order(book(), TradeSide.BUY, 500)
