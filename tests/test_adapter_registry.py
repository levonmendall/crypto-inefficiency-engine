from __future__ import annotations

from datetime import datetime, timezone

import pytest

from inefficiency_engine.adapters.registry import PublicAdapterRegistry
from inefficiency_engine.models import (
    FundingQuote,
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)


NOW = datetime(2026, 8, 19, 1, 30, tzinfo=timezone.utc)


def market(venue: str, kind: MarketKind, symbol: str, *, quote: str = "USD", mid: float = 100000.0) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=kind,
        symbol=symbol,
        quote_currency=quote,
        contract_key="spot" if kind == MarketKind.SPOT else "continuous",
        bid=mid - 1,
        ask=mid + 1,
        mid=mid,
        observed_at=NOW,
        source=f"{venue}:test",
    )


def funding(venue: str, symbol: str, *, quote: str = "USD") -> FundingQuote:
    return FundingQuote(
        venue=venue,
        asset="BTC",
        rate=0.0001,
        interval_hours=8,
        symbol=symbol,
        quote_currency=quote,
        observed_at=NOW,
        source=f"{venue}:funding-test",
    )


def book(venue: str, kind: MarketKind, symbol: str, *, quote: str = "USD") -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        asset="BTC",
        market_kind=kind,
        symbol=symbol,
        quote_currency=quote,
        contract_key="spot" if kind == MarketKind.SPOT else "continuous",
        bids=[OrderBookLevel(price=99999, size=2)],
        asks=[OrderBookLevel(price=100001, size=2)],
        observed_at=NOW,
        source=f"{venue}:book-test",
        request_latency_ms=12.5,
    )


class FakeAdapter:
    def __init__(self, *, markets=None, fundings=None, venue: str = "Fake"):
        self._markets = list(markets or [])
        self._fundings = list(fundings or [])
        self.venue = venue

    async def market_quotes(self):
        return list(self._markets)

    async def funding_quotes(self):
        return list(self._fundings)

    async def market_snapshot(self):
        return list(self._markets), list(self._fundings)

    async def order_book(self, asset, market_kind=MarketKind.SPOT, *, symbol=None, **kwargs):
        quote = "USDT" if self.venue in {"Bybit", "OKX"} else "USD"
        resolved = symbol or (f"BTC-{quote}" if market_kind == MarketKind.SPOT else f"BTC-{quote}-SWAP")
        return book(self.venue, market_kind, resolved, quote=quote)


def registry(*, empty_coinbase: bool = False) -> PublicAdapterRegistry:
    return PublicAdapterRegistry(
        hyperliquid=FakeAdapter(
            markets=[market("HlPerp", MarketKind.PERPETUAL, "BTC")],
            fundings=[funding("HlPerp", "BTC")],
            venue="HlPerp",
        ),
        coinbase=FakeAdapter(
            markets=[] if empty_coinbase else [market("Coinbase", MarketKind.SPOT, "BTC-USD")],
            venue="Coinbase",
        ),
        bybit=FakeAdapter(
            markets=[
                market("Bybit", MarketKind.SPOT, "BTCUSDT", quote="USDT"),
                market("Bybit", MarketKind.PERPETUAL, "BTCUSDT", quote="USDT"),
            ],
            fundings=[funding("Bybit", "BTCUSDT", quote="USDT")],
            venue="Bybit",
        ),
        kraken=FakeAdapter(
            markets=[market("Kraken", MarketKind.SPOT, "BTC/USD")],
            venue="Kraken",
        ),
        okx=FakeAdapter(
            markets=[
                market("OKX", MarketKind.SPOT, "BTC-USDT", quote="USDT"),
                market("OKX", MarketKind.PERPETUAL, "BTC-USDT-SWAP", quote="USDT"),
            ],
            fundings=[funding("OKX", "BTC-USDT-SWAP", quote="USDT")],
            venue="OKX",
        ),
    )


def test_provider_venue_maps_okx_and_existing_venues():
    assert PublicAdapterRegistry.provider_venue("okx-v5:market:books:BTC-USDT") == "OKX"
    assert PublicAdapterRegistry.provider_venue("hyperliquid:l2Book:BTC") == "HlPerp"
    assert PublicAdapterRegistry.provider_venue("coinbase-exchange:ticker") == "Coinbase"
    assert PublicAdapterRegistry.provider_venue("unknown") is None


@pytest.mark.asyncio
async def test_collect_inputs_promotes_okx_and_fails_closed_on_empty_surface():
    healthy = registry()
    funding_quotes, market_quotes, statuses = await healthy.collect_inputs()
    assert any(item.venue == "OKX" for item in market_quotes)
    assert any(item.venue == "OKX" for item in funding_quotes)
    assert all(status.ok for status in statuses)

    degraded = registry(empty_coinbase=True)
    _, _, statuses = await degraded.collect_inputs()
    coinbase = next(item for item in statuses if item.provider == "coinbase-exchange:ticker")
    assert coinbase.ok is False
    assert coinbase.error_type == "EmptyResult"


@pytest.mark.asyncio
async def test_collect_books_routes_okx_leg_through_registry():
    adapters = registry()
    opportunity = Opportunity(
        id="okx-test",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="OKX",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol="BTC-USDT",
                quote_currency="USDT",
                contract_key="spot",
                reference_price=100000,
            ),
            OpportunityLeg(
                venue="OKX",
                asset="BTC",
                market_kind=MarketKind.PERPETUAL,
                side=Side.SHORT,
                symbol="BTC-USDT-SWAP",
                quote_currency="USDT",
                contract_key="continuous",
                reference_price=100100,
            ),
        ],
        gross_edge_bps_per_hour=1.0,
        modeled_cost_bps=1.0,
        holding_hours=24,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=0.5,
        net_annualized_return=0.4,
        observed_at=NOW,
        expires_at=NOW,
        confidence="low",
        evidence={},
    )
    books, statuses = await adapters.collect_books_for_opportunities([opportunity])
    assert len(books) == 2
    assert {item.market_kind for item in books} == {MarketKind.SPOT, MarketKind.PERPETUAL}
    assert all(item.venue == "OKX" for item in books)
    assert all(status.ok for status in statuses)
    assert all(item.request_latency_ms == 12.5 for item in books)


@pytest.mark.asyncio
async def test_diagnostic_checks_surfaces_and_representative_l2():
    report = await registry().diagnose()
    assert report.paper_only is True
    assert report.healthy is True
    assert report.venue_count == 5
    assert report.market_quote_count >= 7
    assert report.funding_quote_count == 3
    assert any(item.venue == "OKX" and item.ok for item in report.surfaces)
    assert any(item.venue == "OKX" and item.ok and item.request_latency_ms == 12.5 for item in report.order_books)
