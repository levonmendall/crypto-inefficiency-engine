from datetime import datetime, timezone

from inefficiency_engine.adapters.hyperliquid import parse_l2_book


def test_parse_hyperliquid_l2_snapshot():
    payload = {
        "coin": "BTC",
        "time": 1754450974231,
        "levels": [
            [{"px": "113377.0", "sz": "7.6699", "n": 17}],
            [{"px": "113397.0", "sz": "0.11543", "n": 3}],
        ],
    }
    book = parse_l2_book(payload)
    assert book.asset == "BTC"
    assert book.bids[0].price == 113377.0
    assert book.asks[0].size == 0.11543
    assert book.observed_at == datetime.fromtimestamp(1754450974231 / 1000, tz=timezone.utc)
