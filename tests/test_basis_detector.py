from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.basis import SpotPerpBasisDetector
from inefficiency_engine.models import MarketKind, MarketQuote


def test_detects_positive_spot_perp_basis():
    settings = Settings(default_holding_hours=24, pair_roundtrip_cost_bps=10, safety_buffer_bps_per_hour=0, min_net_annualized_return=0.01)
    now = datetime.now(timezone.utc)
    quotes = [
        MarketQuote(venue="Spot", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTC-USD", mid=100000, observed_at=now, source="test"),
        MarketQuote(venue="Perp", asset="BTC", market_kind=MarketKind.PERPETUAL, symbol="BTC", mid=101000, observed_at=now, source="test"),
    ]
    opportunities = SpotPerpBasisDetector(settings).detect(quotes)
    assert len(opportunities) == 1
    assert opportunities[0].net_annualized_return > 0
