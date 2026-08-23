from __future__ import annotations

import asyncio
from typing import Iterable

from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.volume_universe import (
    read_latest_volume_universe,
    validated_volume_assets,
)


FAST_BOOK_BATCH_SIZE = 8


def _clean_assets(values: Iterable[object]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        asset = str(raw or "").upper().strip()
        if asset and asset not in seen:
            result.append(asset)
            seen.add(asset)
    return tuple(result)


def persisted_routing_assets(store, registry) -> tuple[str, ...]:
    """Read current routing membership without putting CoinGecko on the hot path."""

    try:
        assets = validated_volume_assets(read_latest_volume_universe(store))
    except Exception:
        assets = ()
    if assets:
        return assets

    for adapter_name in ("coinbase", "kraken", "okx"):
        adapter = getattr(registry, adapter_name, None)
        fallback = _clean_assets(getattr(adapter, "assets", ()) or ())
        if fallback:
            return fallback
    return ("BTC",)


def rotating_assets(
    store,
    registry,
    *,
    cycle: int,
    count: int,
) -> tuple[str, ...]:
    universe = persisted_routing_assets(store, registry)
    if not universe:
        return ()
    width = max(1, min(len(universe), int(count)))
    start = (max(0, int(cycle)) * width) % len(universe)
    return tuple(universe[(start + offset) % len(universe)] for offset in range(width))


def _subset_status(status: ProviderStatus, rows: list[object]) -> ProviderStatus:
    if rows:
        return ProviderStatus(
            provider=status.provider,
            ok=True,
            item_count=len(rows),
            error_type=None,
        )
    return ProviderStatus(
        provider=status.provider,
        ok=False,
        item_count=0,
        error_type=status.error_type or "EmptyResult",
    )


def _filter_assets(rows: Iterable[object], selected: set[str]) -> list[object]:
    return [
        row
        for row in rows
        if str(getattr(row, "asset", "")).upper().strip() in selected
    ]


class FastExecutableMarketCollector:
    """Acquire only the rotating executable cohort with bounded parallel fanout.

    The full top-volume research universe is intentionally not refreshed here. This
    collector performs the minimum quote/funding fanout required to pair fresh market
    truth with visible L2 before the downstream 120-second freshness boundary. It does
    not alter any qualification, cost, statistical, risk, or paper-only gate.
    """

    def __init__(self, registry):
        self.registry = registry

    async def _managed_market(self, provider: str, adapter, assets: tuple[str, ...]):
        original_assets = tuple(getattr(adapter, "assets", assets) or assets)
        try:
            setattr(adapter, "assets", assets)
            return await self.registry._capture_list(provider, adapter.market_quotes())
        finally:
            setattr(adapter, "assets", original_assets)

    async def _okx(self, assets: tuple[str, ...]):
        adapter = self.registry.okx
        original_assets = tuple(getattr(adapter, "assets", assets) or assets)
        try:
            adapter.assets = assets
            market_result, funding_result = await asyncio.gather(
                self.registry._capture_list(
                    "okx-v5:market:ticker",
                    adapter.market_quotes(),
                ),
                self.registry._capture_list(
                    "okx-v5:public:funding-rate",
                    adapter.funding_quotes(),
                ),
            )
            return market_result, funding_result
        finally:
            adapter.assets = original_assets

    async def _hyperliquid(self, assets: tuple[str, ...]):
        selected = set(assets)
        funding_result, market_result = await asyncio.gather(
            self.registry._capture_list(
                "hyperliquid:predictedFundings",
                self.registry.hyperliquid.funding_quotes(),
            ),
            self.registry._capture_list(
                "hyperliquid:metaAndAssetCtxs",
                self.registry.hyperliquid.market_quotes(),
            ),
        )
        funding, funding_status = funding_result
        market, market_status = market_result
        funding = _filter_assets(funding, selected)
        market = _filter_assets(market, selected)
        return (
            funding,
            market,
            _subset_status(funding_status, funding),
            _subset_status(market_status, market),
        )

    async def _bybit(self, assets: tuple[str, ...]):
        if not bool(getattr(self.registry, "bybit_public_enabled", True)):
            return [], [], None
        adapter = self.registry.bybit
        original_assets = tuple(getattr(adapter, "assets", assets) or assets)
        try:
            adapter.assets = assets
            market, funding, status = await self.registry._capture_bybit()
        finally:
            adapter.assets = original_assets
        selected = set(assets)
        market = _filter_assets(market, selected)
        funding = _filter_assets(funding, selected)
        filtered_status = ProviderStatus(
            provider=status.provider,
            ok=bool(market or funding),
            item_count=len(market) + len(funding),
            error_type=None if (market or funding) else (status.error_type or "EmptyResult"),
        )
        return market, funding, filtered_status

    async def collect_inputs(
        self,
        assets: tuple[str, ...],
    ) -> tuple[list[object], list[object], list[ProviderStatus]]:
        """Collect the bounded cohort with provider groups running concurrently."""

        selected = tuple(_clean_assets(assets))
        if not selected:
            return [], [], []

        hyper_task = self._hyperliquid(selected)
        coinbase_task = self._managed_market(
            "coinbase-exchange:ticker",
            self.registry.coinbase,
            selected,
        )
        bybit_task = self._bybit(selected)
        kraken_task = self._managed_market(
            "kraken:PreTrade",
            self.registry.kraken,
            selected,
        )
        okx_task = self._okx(selected)

        hyper, coinbase, bybit, kraken, okx = await asyncio.gather(
            hyper_task,
            coinbase_task,
            bybit_task,
            kraken_task,
            okx_task,
        )
        hyper_funding, hyper_market, hyper_funding_status, hyper_market_status = hyper
        coinbase_market, coinbase_status = coinbase
        bybit_market, bybit_funding, bybit_status = bybit
        kraken_market, kraken_status = kraken
        (okx_market, okx_market_status), (okx_funding, okx_funding_status) = okx

        funding_quotes = [*hyper_funding, *bybit_funding, *okx_funding]
        market_quotes = [
            *hyper_market,
            *coinbase_market,
            *bybit_market,
            *kraken_market,
            *okx_market,
        ]
        statuses = [
            hyper_funding_status,
            hyper_market_status,
            coinbase_status,
            *([bybit_status] if bybit_status is not None else []),
            kraken_status,
            okx_market_status,
            okx_funding_status,
        ]
        return funding_quotes, market_quotes, statuses

    async def collect_books(self, requests: list[object]):
        """Use a slightly wider bounded L2 batch only for the tiny hot cohort."""

        previous = int(getattr(self.registry, "order_book_batch_size", 1) or 1)
        try:
            self.registry.order_book_batch_size = max(previous, FAST_BOOK_BATCH_SIZE)
            return await self.registry.collect_books_for_opportunities(requests)
        finally:
            self.registry.order_book_batch_size = previous
