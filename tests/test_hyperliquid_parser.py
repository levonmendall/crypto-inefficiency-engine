import json
from datetime import datetime, timezone
from pathlib import Path

from inefficiency_engine.adapters.hyperliquid import parse_predicted_fundings


def test_parse_predicted_fundings_normalizes_intervals():
    payload = json.loads((Path(__file__).parent / "fixtures" / "hyperliquid_predicted_fundings.json").read_text())
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    quotes = parse_predicted_fundings(payload, observed_at=now)
    btc = [q for q in quotes if q.asset == "BTC"]
    assert len(btc) == 3
    binance = next(q for q in btc if q.venue == "BinPerp")
    hl = next(q for q in btc if q.venue == "HlPerp")
    assert binance.hourly_rate == 0.0016 / 8
    assert hl.hourly_rate == 0.00002


def test_missing_funding_interval_is_rejected_fail_closed():
    payload = [["BTC", [["UnknownPerp", {"fundingRate": "0.001"}]]]]
    assert parse_predicted_fundings(payload) == []
