from datetime import datetime, timezone

from inefficiency_engine.market_graph import (
    GraphRelationship,
    build_market_graph,
    canonical_asset_id,
    canonical_instrument_id,
)
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote


NOW = datetime(2026, 8, 19, 0, 20, tzinfo=timezone.utc)


def test_market_graph_canonicalizes_equivalent_instruments_across_venues():
    market_quotes = [
        MarketQuote(
            venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT,
            symbol="BTC-USD", bid=99900, ask=100100, mid=100000,
            observed_at=NOW, source="coinbase:ticker",
        ),
        MarketQuote(
            venue="HlPerp", asset="BTC", market_kind=MarketKind.PERPETUAL,
            symbol="BTC", mid=100250, observed_at=NOW, source="hyperliquid:context",
        ),
    ]
    funding_quotes = [
        FundingQuote(
            venue="HlPerp", asset="BTC", rate=0.0001, interval_hours=1,
            observed_at=NOW, source="hyperliquid:funding",
        )
    ]

    graph = build_market_graph(funding_quotes, market_quotes)

    assert graph.summary()["asset_count"] == 1
    assert graph.summary()["venue_count"] == 2
    assert graph.summary()["instrument_count"] == 2
    assert canonical_asset_id("btc") == "crypto:asset:BTC"
    assert graph.instrument_id_for("Coinbase", "BTC", MarketKind.SPOT) == canonical_instrument_id(
        "Coinbase", "BTC", MarketKind.SPOT
    )
    perp = next(item for item in graph.instruments if item.venue == "HlPerp")
    assert perp.latest_funding_rate == 0.0001
    equivalence = [edge for edge in graph.edges if edge.relationship == GraphRelationship.ECONOMIC_EQUIVALENCE]
    assert len(equivalence) == 1


def test_provider_symbols_are_aliases_not_canonical_identity():
    quotes = [
        MarketQuote(
            venue="Venue X", asset="ETH", market_kind=MarketKind.SPOT,
            symbol="ETH-USD", mid=4000, observed_at=NOW, source="feed-a",
        ),
        MarketQuote(
            venue="Venue X", asset="ETH", market_kind=MarketKind.SPOT,
            symbol="XETHZUSD", mid=4001, observed_at=NOW, source="feed-b",
        ),
    ]

    graph = build_market_graph([], quotes)

    assert len(graph.instruments) == 1
    instrument = graph.instruments[0]
    assert instrument.provider_symbols == {"feed-a": "ETH-USD", "feed-b": "XETHZUSD"}
    assert instrument.instrument_id == canonical_instrument_id("Venue X", "ETH", MarketKind.SPOT)


def test_dated_futures_have_contract_specific_canonical_identity():
    first_expiry = datetime(2026, 9, 25, 8, tzinfo=timezone.utc)
    second_expiry = datetime(2026, 12, 25, 8, tzinfo=timezone.utc)
    quotes = [
        MarketQuote(
            venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE,
            symbol="BTCUSDT-25SEP26", quote_currency="USDT",
            contract_key="expiry-20260925T080000Z", expires_at=first_expiry,
            mid=101000, observed_at=NOW, source="bybit:first",
        ),
        MarketQuote(
            venue="Bybit", asset="BTC", market_kind=MarketKind.FUTURE,
            symbol="BTCUSDT-25DEC26", quote_currency="USDT",
            contract_key="expiry-20261225T080000Z", expires_at=second_expiry,
            mid=102000, observed_at=NOW, source="bybit:second",
        ),
    ]

    graph = build_market_graph([], quotes)

    assert len(graph.instruments) == 2
    first_id = canonical_instrument_id(
        "Bybit", "BTC", MarketKind.FUTURE, contract_key="expiry-20260925T080000Z"
    )
    second_id = canonical_instrument_id(
        "Bybit", "BTC", MarketKind.FUTURE, contract_key="expiry-20261225T080000Z"
    )
    assert first_id != second_id
    assert graph.instrument_id_for(
        "Bybit", "BTC", MarketKind.FUTURE, "expiry-20260925T080000Z"
    ) == first_id
