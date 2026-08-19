from inefficiency_engine.adapters.kraken import parse_pretrade


def test_kraken_pretrade_parses_aggregated_public_book():
    payload = {
        "error": [],
        "result": {
            "symbol": "BTC/USD",
            "base_asset": "BTC",
            "quote_asset": "USD",
            "bids": [
                {"side": "BUY", "price": "100000", "qty": "1.5", "count": 2, "publication_ts": "2026-08-19T00:20:00.000000Z"}
            ],
            "asks": [
                {"side": "SELL", "price": "100100", "qty": "2.5", "count": 3, "publication_ts": "2026-08-19T00:20:00.100000Z"}
            ],
        },
    }

    book = parse_pretrade(payload, asset="BTC", symbol="BTC/USD")

    assert book.venue == "Kraken"
    assert book.quote_currency == "USD"
    assert book.bids[0].price == 100000.0
    assert book.asks[0].size == 2.5
    assert book.observed_at.isoformat().startswith("2026-08-19T00:20:00.100")
