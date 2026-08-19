from datetime import datetime, timezone

from inefficiency_engine.adapters.universal_public import parse_deribit_option_summaries, parse_dexscreener_pairs
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.universal import detect_dex_routes, detect_option_relative_value

NOW = datetime(2026,8,19,tzinfo=timezone.utc)

def test_dexscreener_pool_is_discovery_proxy_not_exact_depth():
    pools = parse_dexscreener_pairs([{"chainId":"ethereum","dexId":"uniswap","pairAddress":"0xpool",
        "baseToken":{"address":"0xbase","symbol":"WETH","name":"Wrapped Ether"},
        "quoteToken":{"address":"0xquote","symbol":"USDC","name":"USD Coin"},"priceUsd":"4100",
        "liquidity":{"usd":1000000,"base":100,"quote":410000},"volume":{"h24":500000}}],observed_at=NOW)
    assert pools[0].base_token.canonical_asset == "ETH"
    assert pools[0].executable_depth_supported is False
    quotes = [MarketQuote(venue="Coinbase",asset="ETH",market_kind=MarketKind.SPOT,symbol="ETH-USD",
        quote_currency="USD",mid=4000,observed_at=NOW,source="test")]
    candidates = detect_dex_routes(quotes,pools,minimum_edge_bps=10,liquidity_risk_floor_bps=1)
    assert candidates
    assert candidates[0].executable_eligible is False
    assert "exact" in candidates[0].blocked_reason.lower()

def test_deribit_option_surface_anomaly_is_not_executable_without_hedge_model():
    payload = {"result":[
        {"instrument_name":"BTC-30AUG26-100000-C","mark_iv":50,"underlying_price":100000,"open_interest":10},
        {"instrument_name":"BTC-30AUG26-110000-C","mark_iv":51,"underlying_price":100000,"open_interest":10},
        {"instrument_name":"BTC-30AUG26-120000-C","mark_iv":75,"underlying_price":100000,"open_interest":10},
    ]}
    quotes = parse_deribit_option_summaries(payload,observed_at=NOW)
    candidates = detect_option_relative_value(quotes,minimum_iv_deviation=8)
    assert candidates
    assert candidates[0].executable_eligible is False
    assert "hedge" in candidates[0].blocked_reason.lower()
