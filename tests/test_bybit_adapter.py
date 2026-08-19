from datetime import datetime, timezone

from inefficiency_engine.adapters.bybit import BybitInstrumentSpec, parse_instruments_info, parse_orderbook, parse_ticker
from inefficiency_engine.models import MarketKind


NOW_MS = 1787099000000


def test_bybit_parses_spot_perpetual_and_dated_future_metadata():
    spot = {
        "retCode": 0,
        "time": NOW_MS,
        "result": {"list": [{
            "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading"
        }]},
    }
    linear = {
        "retCode": 0,
        "time": NOW_MS,
        "result": {"list": [
            {
                "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading",
                "contractType": "LinearPerpetual", "fundingInterval": "480",
            },
            {
                "symbol": "BTCUSDT-25DEC26", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading",
                "contractType": "LinearFutures", "deliveryTime": "1798185600000",
            },
        ]},
    }

    specs = [*parse_instruments_info(spot), *parse_instruments_info(linear)]

    assert {spec.market_kind for spec in specs} == {MarketKind.SPOT, MarketKind.PERPETUAL, MarketKind.FUTURE}
    future = next(spec for spec in specs if spec.market_kind == MarketKind.FUTURE)
    assert future.contract_key.startswith("expiry-")
    assert future.expires_at is not None
    perp = next(spec for spec in specs if spec.market_kind == MarketKind.PERPETUAL)
    assert perp.funding_interval_hours == 8.0


def test_bybit_ticker_and_orderbook_keep_contract_identity():
    expiry = datetime(2026, 12, 25, 8, tzinfo=timezone.utc)
    spec = BybitInstrumentSpec(
        symbol="BTCUSDT-25DEC26",
        asset="BTC",
        quote_currency="USDT",
        market_kind=MarketKind.FUTURE,
        contract_key="expiry-20261225T080000Z",
        expires_at=expiry,
    )
    ticker_payload = {
        "retCode": 0,
        "time": NOW_MS,
        "result": {"list": [{
            "symbol": spec.symbol, "bid1Price": "101000", "ask1Price": "101100", "lastPrice": "101050"
        }]},
    }
    book_payload = {
        "retCode": 0,
        "time": NOW_MS,
        "result": {"s": spec.symbol, "ts": NOW_MS, "b": [["101000", "2"]], "a": [["101100", "3"]]},
    }

    quote, funding = parse_ticker(ticker_payload, spec)
    book = parse_orderbook(book_payload, spec)

    assert funding is None
    assert quote is not None
    assert quote.contract_key == spec.contract_key
    assert quote.expires_at == expiry
    assert book.contract_key == spec.contract_key
    assert book.bids[0].size == 2.0
    assert book.asks[0].price == 101100.0
