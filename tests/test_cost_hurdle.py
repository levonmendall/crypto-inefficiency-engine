from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.funding import FundingDispersionDetector
from inefficiency_engine.models import FundingQuote


def test_rejects_small_apparent_spread_when_costs_dominate():
    settings = Settings(
        default_holding_hours=8,
        pair_roundtrip_cost_bps=40,
        safety_buffer_bps_per_hour=0.05,
        min_net_annualized_return=0.0,
    )
    now = datetime.now(timezone.utc)
    quotes = [
        FundingQuote(venue="A", asset="ETH", rate=0.0001, interval_hours=8, observed_at=now, source="test"),
        FundingQuote(venue="B", asset="ETH", rate=0.0002, interval_hours=8, observed_at=now, source="test"),
    ]
    assert FundingDispersionDetector(settings).detect(quotes) == []
