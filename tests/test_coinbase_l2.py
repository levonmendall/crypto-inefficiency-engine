from datetime import timezone

from inefficiency_engine.adapters.coinbase import parse_product_book
from inefficiency_engine.models import MarketKind


def test_parse_coinbase_level2_book():
    book = parse_product_book(
        {
            "sequence": 1,
            "bids": [["99.0", "2.0", 3], ["98.0", "4.0", 1]],
            "asks": [["101.0", "1.5", 2], ["102.0", "3.0", 2]],
            "time": "2026-08-18T22:00:00Z",
        },
        asset="BTC",
        symbol="BTC-USD",
    )
    assert book.venue == "Coinbase"
    assert book.market_kind == MarketKind.SPOT
    assert book.bids[0].price == 99.0
    assert book.bids[0].size == 2.0
    assert book.asks[1].price == 102.0
    assert book.observed_at.tzinfo == timezone.utc
