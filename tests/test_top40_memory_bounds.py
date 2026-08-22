from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.adapters.dynamic_registry import DynamicVolumePublicAdapterRegistry
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


NOW = datetime(2026, 8, 20, 17, 0, tzinfo=timezone.utc)


class ScalableFakeAdapter:
    def __init__(self, venue: str, assets: tuple[str, ...]):
        self.venue = venue
        self.assets = assets
        self.max_assets_seen = 0
        self.active_books = 0
        self.max_active_books = 0
        self.book_calls = 0

    def _symbol(self, asset: str) -> str:
        if self.venue == "Coinbase":
            return f"{asset}-USD"
        if self.venue == "Kraken":
            return f"{asset}/USD"
        if self.venue == "OKX":
            return f"{asset}-USDT"
        if self.venue == "Bybit":
            return f"{asset}USDT"
        return asset

    async def market_quotes(self):
        self.max_assets_seen = max(self.max_assets_seen, len(self.assets))
        return [
            MarketQuote(
                venue=self.venue,
                asset=asset,
                market_kind=MarketKind.PERPETUAL if self.venue == "HlPerp" else MarketKind.SPOT,
                symbol=self._symbol(asset),
                quote_currency="USDT" if self.venue in {"Bybit", "OKX"} else "USD",
                contract_key="continuous" if self.venue == "HlPerp" else "spot",
                bid=99.0,
                ask=101.0,
                mid=100.0,
                observed_at=NOW,
                source=f"{self.venue}:test",
            )
            for asset in self.assets
        ]

    async def funding_quotes(self):
        self.max_assets_seen = max(self.max_assets_seen, len(self.assets))
        return [
            FundingQuote(
                venue=self.venue,
                asset=asset,
                rate=0.0001,
                interval_hours=8.0,
                symbol=self._symbol(asset),
                quote_currency="USDT" if self.venue in {"Bybit", "OKX"} else "USD",
                observed_at=NOW,
                source=f"{self.venue}:funding-test",
            )
            for asset in self.assets
        ]

    async def market_snapshot(self):
        return await self.market_quotes(), await self.funding_quotes()

    async def order_book(self, asset, market_kind=MarketKind.SPOT, *, symbol=None, **kwargs):
        self.book_calls += 1
        self.active_books += 1
        self.max_active_books = max(self.max_active_books, self.active_books)
        try:
            await asyncio.sleep(0.01)
            resolved = symbol or self._symbol(str(asset))
            return OrderBookSnapshot(
                venue=self.venue,
                asset=str(asset),
                market_kind=market_kind,
                symbol=resolved,
                quote_currency="USDT" if self.venue in {"Bybit", "OKX"} else "USD",
                contract_key="spot" if market_kind == MarketKind.SPOT else "continuous",
                bids=[OrderBookLevel(price=99.0 - index * 0.01, size=1.0) for index in range(250)],
                asks=[OrderBookLevel(price=101.0 + index * 0.01, size=1.0) for index in range(250)],
                observed_at=NOW,
                source=f"{self.venue}:book-test",
                request_latency_ms=10.0,
            )
        finally:
            self.active_books -= 1


class HeartbeatStore:
    def __init__(self):
        self.rows: list[dict[str, object]] = []

    def record_worker_heartbeat(self, **payload):
        self.rows.append(dict(payload))


def registry(assets: tuple[str, ...], **kwargs) -> tuple[DynamicVolumePublicAdapterRegistry, dict[str, ScalableFakeAdapter]]:
    adapters = {
        "hyperliquid": ScalableFakeAdapter("HlPerp", assets),
        "coinbase": ScalableFakeAdapter("Coinbase", assets),
        "bybit": ScalableFakeAdapter("Bybit", assets),
        "kraken": ScalableFakeAdapter("Kraken", assets),
        "okx": ScalableFakeAdapter("OKX", assets),
    }
    value = DynamicVolumePublicAdapterRegistry(
        hyperliquid=adapters["hyperliquid"],
        coinbase=adapters["coinbase"],
        bybit=adapters["bybit"],
        kraken=adapters["kraken"],
        okx=adapters["okx"],
        memory_soft_limit_mb=999999.0,
        **kwargs,
    )
    return value, adapters


def opportunity(asset: str) -> Opportunity:
    return Opportunity(
        id=f"op-{asset}",
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset=asset,
        legs=[
            OpportunityLeg(
                venue="OKX",
                asset=asset,
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol=f"{asset}-USDT",
                quote_currency="USDT",
                contract_key="spot",
                reference_price=100.0,
            )
        ],
        gross_edge_bps_per_hour=1.0,
        modeled_cost_bps=0.1,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=0.9,
        net_annualized_return=0.10,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        confidence="low",
        evidence={},
    )


@pytest.mark.asyncio
async def test_top40_managed_cex_collection_is_chunked():
    assets = tuple(f"A{index:02d}" for index in range(40))
    value, adapters = registry(assets, asset_chunk_size=4, provider_group_concurrency=2)
    value._managed_coinbase = True
    value._managed_kraken = True
    value._managed_okx = True

    funding, markets, statuses = await value.collect_inputs()

    assert markets
    assert funding
    assert statuses
    assert adapters["coinbase"].max_assets_seen <= 4
    assert adapters["kraken"].max_assets_seen <= 4
    assert adapters["okx"].max_assets_seen <= 4
    assert tuple(adapters["coinbase"].assets) == assets
    assert tuple(adapters["kraken"].assets) == assets
    assert tuple(adapters["okx"].assets) == assets


@pytest.mark.asyncio
async def test_raw_collection_survives_cold_volume_universe_failure(monkeypatch):
    assets = ("BTC", "ETH", "SOL")
    store = HeartbeatStore()
    adapters = {
        "hyperliquid": ScalableFakeAdapter("HlPerp", assets),
        "coinbase": ScalableFakeAdapter("Coinbase", assets),
        "bybit": ScalableFakeAdapter("Bybit", assets),
        "kraken": ScalableFakeAdapter("Kraken", assets),
        "okx": ScalableFakeAdapter("OKX", assets),
    }

    async def unavailable(_store):
        raise RuntimeError("cold CoinGecko bootstrap unavailable")

    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    monkeypatch.setattr(
        "inefficiency_engine.adapters.dynamic_registry.resolve_top_volume_assets",
        unavailable,
    )
    value = DynamicVolumePublicAdapterRegistry(
        evidence_store=store,
        hyperliquid=adapters["hyperliquid"],
        coinbase=adapters["coinbase"],
        bybit=adapters["bybit"],
        kraken=adapters["kraken"],
        okx=adapters["okx"],
        memory_soft_limit_mb=999999.0,
    )
    # Exercise the production managed-CEX routing path while keeping all provider
    # calls local to deterministic fakes.
    value._managed_coinbase = True
    value._managed_kraken = True
    value._managed_okx = True

    funding, markets, statuses = await value.collect_inputs()

    assert markets
    assert funding
    assert statuses
    assert {quote.venue for quote in markets} >= {"HlPerp", "Coinbase", "Kraken", "OKX"}
    routing = [row for row in store.rows if row.get("worker_id") == "market-universe-routing"]
    assert routing
    assert routing[-1]["state"] == "degraded"
    assert routing[-1]["error_type"] == "RuntimeError"
    detail = routing[-1]["detail"]
    assert detail["routing_state"] == "fallback_acquisition_only"
    assert detail["raw_acquisition_continues"] is True
    assert detail["fallback_has_research_universe_authority"] is False
    assert detail["allocation_authority"] is False
    assert detail["live_execution_authority"] is False


@pytest.mark.asyncio
async def test_top40_l2_is_batched_and_retained_depth_is_capped():
    assets = tuple(f"A{index:02d}" for index in range(40))
    value, adapters = registry(
        assets,
        order_book_batch_size=3,
        max_order_book_levels=25,
    )

    books, statuses = await value.collect_books_for_opportunities([opportunity(asset) for asset in assets])

    assert len(books) == 40
    assert len(statuses) == 40
    assert all(status.ok for status in statuses)
    assert adapters["okx"].max_active_books <= 3
    assert all(len(book.bids) == 25 for book in books)
    assert all(len(book.asks) == 25 for book in books)


@pytest.mark.asyncio
async def test_l2_fails_closed_when_soft_memory_budget_is_reached(monkeypatch):
    assets = ("BTC", "ETH", "SOL")
    value, adapters = registry(assets, order_book_batch_size=2)
    monkeypatch.setattr(
        "inefficiency_engine.adapters.dynamic_registry.memory_budget_exceeded",
        lambda _limit: True,
    )

    books, statuses = await value.collect_books_for_opportunities([opportunity(asset) for asset in assets])

    assert books == []
    assert len(statuses) == 3
    assert all(status.ok is False for status in statuses)
    assert all(status.error_type == "MemoryBudgetDeferred" for status in statuses)
    assert adapters["okx"].book_calls == 0
