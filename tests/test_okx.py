from datetime import datetime, timezone

from inefficiency_engine.adapters.okx import parse_funding_rate, parse_order_book, parse_ticker
from inefficiency_engine.models import MarketKind


def test_okx_public_parsers_preserve_quote_and_latency_ready_identity():
    ticker = parse_ticker({"code":"0","data":[{"bidPx":"99900","askPx":"100100","ts":"1787100000000"}]},
        asset="BTC",market_kind=MarketKind.SPOT,symbol="BTC-USDT",quote_currency="USDT")
    assert ticker.venue == "OKX"
    assert ticker.quote_currency == "USDT"
    assert ticker.contract_key == "spot"
    funding = parse_funding_rate({"code":"0","data":[{"fundingRate":"0.0001","fundingTime":"1787100000000",
        "nextFundingTime":"1787128800000"}]},asset="BTC",symbol="BTC-USDT-SWAP",quote_currency="USDT")
    assert funding.interval_hours == 8.0
    assert funding.venue == "OKX"
    book = parse_order_book({"code":"0","data":[{"bids":[["99900","1","0","1"]],
        "asks":[["100100","2","0","1"]],"ts":"1787100000000"}]},asset="BTC",
        market_kind=MarketKind.PERPETUAL,symbol="BTC-USDT-SWAP",quote_currency="USDT")
    assert book.bids[0].size == 1
    assert book.asks[0].size == 2
    assert book.contract_key == "continuous"
