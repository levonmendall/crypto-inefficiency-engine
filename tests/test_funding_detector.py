from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.detectors.funding import FundingDispersionDetector
from inefficiency_engine.models import FundingQuote, Side


def test_detects_profitable_orientation_after_costs():
    settings = Settings(
        default_holding_hours=24,
        pair_roundtrip_cost_bps=10,
        safety_buffer_bps_per_hour=0.01,
        min_net_annualized_return=0.01,
    )
    now = datetime.now(timezone.utc)
    quotes = [
        FundingQuote(venue="Low", asset="BTC", rate=-0.0008, interval_hours=8, observed_at=now, source="test"),
        FundingQuote(venue="High", asset="BTC", rate=0.0016, interval_hours=8, observed_at=now, source="test"),
    ]
    opportunities = FundingDispersionDetector(settings).detect(quotes)
    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.legs[0].venue == "Low"
    assert opportunity.legs[0].side == Side.LONG
    assert opportunity.legs[1].venue == "High"
    assert opportunity.legs[1].side == Side.SHORT
    assert opportunity.net_annualized_return > 0
