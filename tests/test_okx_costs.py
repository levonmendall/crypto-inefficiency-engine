from inefficiency_engine.config import Settings
from inefficiency_engine.costs import taker_fee_bps
from inefficiency_engine.models import MarketKind, OpportunityLeg, Side


def test_okx_explicit_fee_schedule_is_available_to_core_qualification():
    settings = Settings(okx_spot_taker_fee_bps=10.0, okx_derivatives_taker_fee_bps=5.0)
    spot = OpportunityLeg(
        venue="OKX", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG,
        symbol="BTC-USDT", quote_currency="USDT", contract_key="spot",
    )
    perp = OpportunityLeg(
        venue="OKX", asset="BTC", market_kind=MarketKind.PERPETUAL, side=Side.SHORT,
        symbol="BTC-USDT-SWAP", quote_currency="USDT", contract_key="continuous",
    )
    assert taker_fee_bps(spot, settings) == 10.0
    assert taker_fee_bps(perp, settings) == 5.0
