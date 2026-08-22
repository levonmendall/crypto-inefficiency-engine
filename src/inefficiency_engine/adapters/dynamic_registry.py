from __future__ import annotations

import asyncio
import gc
import os

from inefficiency_engine.adapters.registry import PublicAdapterRegistry
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.instrument_identity import book_identity
from inefficiency_engine.memory_budget import (
    DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB,
    memory_budget_exceeded,
    memory_snapshot,
)
from inefficiency_engine.models import FundingQuote, MarketQuote, Opportunity, OrderBookSnapshot
from inefficiency_engine.runtime_provider_policy import bybit_public_enabled
from inefficiency_engine.volume_universe import resolve_top_volume_assets


DEFAULT_ASSET_CHUNK_SIZE = 4
DEFAULT_PROVIDER_GROUP_CONCURRENCY = 2
DEFAULT_ORDER_BOOK_BATCH_SIZE = 4
DEFAULT_MAX_ORDER_BOOK_LEVELS = 100


def _chunks(values: tuple[str, ...], size: int) -> tuple[tuple[str, ...], ...]:
    return tuple(values[index:index + size] for index in range(0, len(values), size))


class DynamicVolumePublicAdapterRegistry(PublicAdapterRegistry):
    """Top-volume public-data registry with universe-size-independent peak work.

    The liquid universe is expected to grow to at least 40 assets. The registry
    therefore bounds provider fanout by asset chunks and bounds L2 acquisition by
    batches instead of allowing concurrency to scale with universe size. Full-depth
    provider responses are reduced to a conservative top-of-book depth window before
    they are retained for qualification. If process RSS reaches the soft budget,
    remaining L2 work fails closed instead of risking a Render OOM restart.

    Hyperliquid remains full-universe. Only default-managed Coinbase, Bybit,
    Kraken and OKX adapters are updated; explicitly supplied/custom adapters are
    never mutated. Universe selection is research routing only and creates no
    allocation or execution authority.
    """

    def __init__(
        self,
        *,
        evidence_store: EvidenceStore | None = None,
        hyperliquid: object | None = None,
        coinbase: object | None = None,
        bybit: object | None = None,
        kraken: object | None = None,
        okx: object | None = None,
        provider_surface_timeout_seconds: float = 8.0,
        order_book_timeout_seconds: float = 8.0,
        asset_chunk_size: int = DEFAULT_ASSET_CHUNK_SIZE,
        provider_group_concurrency: int = DEFAULT_PROVIDER_GROUP_CONCURRENCY,
        order_book_batch_size: int = DEFAULT_ORDER_BOOK_BATCH_SIZE,
        max_order_book_levels: int = DEFAULT_MAX_ORDER_BOOK_LEVELS,
        memory_soft_limit_mb: float = DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB,
    ):
        self.evidence_store = evidence_store
        self.bybit_public_enabled = bybit_public_enabled()
        self._managed_coinbase = coinbase is None
        self._managed_bybit = bybit is None
        self._managed_kraken = kraken is None
        self._managed_okx = okx is None
        self.asset_chunk_size = max(
            1,
            int(os.getenv("CIE_MARKET_ASSET_CHUNK_SIZE", str(asset_chunk_size))),
        )
        self.provider_group_concurrency = max(
            1,
            int(os.getenv("CIE_MARKET_PROVIDER_CONCURRENCY", str(provider_group_concurrency))),
        )
        self.order_book_batch_size = max(
            1,
            int(os.getenv("CIE_ORDER_BOOK_BATCH_SIZE", str(order_book_batch_size))),
        )
        self.max_order_book_levels = max(
            1,
            int(os.getenv("CIE_MAX_ORDER_BOOK_LEVELS", str(max_order_book_levels))),
        )
        self.memory_soft_limit_mb = max(
            128.0,
            float(os.getenv("CIE_RESEARCH_MEMORY_SOFT_LIMIT_MB", str(memory_soft_limit_mb))),
        )
        super().__init__(
            hyperliquid=hyperliquid,
            coinbase=coinbase,
            bybit=bybit,
            kraken=kraken,
            okx=okx,
            provider_surface_timeout_seconds=provider_surface_timeout_seconds,
            order_book_timeout_seconds=order_book_timeout_seconds,
        )

    def _record_memory(self, stage: str, **detail: object) -> None:
        if self.evidence_store is None:
            return
        snapshot = memory_snapshot()
        rss = snapshot.get("rss_mb")
        over_budget = bool(rss is not None and rss >= self.memory_soft_limit_mb)
        try:
            self.evidence_store.record_worker_heartbeat(
                worker_id="market-data-memory-budget",
                state="degraded" if over_budget else "running",
                detail={
                    "stage": stage,
                    **snapshot,
                    "soft_limit_mb": self.memory_soft_limit_mb,
                    "asset_chunk_size": self.asset_chunk_size,
                    "provider_group_concurrency": self.provider_group_concurrency,
                    "order_book_batch_size": self.order_book_batch_size,
                    "max_order_book_levels": self.max_order_book_levels,
                    "memory_budget_exceeded": over_budget,
                    "bybit_public_enabled": self.bybit_public_enabled,
                    "paper_only": True,
                    "live_execution_authority": False,
                    **detail,
                },
            )
        except Exception:
            # Memory telemetry must never become market-data authority.
            pass

    async def _refresh_managed_assets(self) -> tuple[str, ...] | None:
        if self.evidence_store is None:
            return None
        assets = await resolve_top_volume_assets(self.evidence_store)
        if self._managed_coinbase:
            self.coinbase.assets = assets
        if self._managed_bybit and self.bybit_public_enabled:
            self.bybit.assets = assets
        if self._managed_kraken:
            self.kraken.assets = assets
        if self._managed_okx:
            self.okx.assets = assets
        return assets

    async def _chunked_market_surface(
        self,
        *,
        provider: str,
        adapter: object,
        assets: tuple[str, ...],
    ) -> tuple[list[object], ProviderStatus]:
        original_assets = tuple(getattr(adapter, "assets", assets))
        rows: list[object] = []
        errors: list[str] = []
        try:
            for chunk in _chunks(assets, self.asset_chunk_size):
                setattr(adapter, "assets", chunk)
                part, status = await self._capture_list(provider, adapter.market_quotes())
                rows.extend(part)
                if not status.ok and status.error_type not in {None, "EmptyResult"}:
                    errors.append(status.error_type)
                del part
        finally:
            setattr(adapter, "assets", original_assets)
        ok = bool(rows) and not errors
        return rows, ProviderStatus(
            provider=provider,
            ok=ok,
            item_count=len(rows),
            error_type=None if ok else (errors[0] if errors else "EmptyResult"),
        )

    async def _chunked_okx_surface(
        self,
        assets: tuple[str, ...],
    ) -> tuple[list[object], list[object], ProviderStatus, ProviderStatus]:
        original_assets = tuple(getattr(self.okx, "assets", assets))
        markets: list[object] = []
        fundings: list[object] = []
        market_errors: list[str] = []
        funding_errors: list[str] = []
        try:
            for chunk in _chunks(assets, self.asset_chunk_size):
                self.okx.assets = chunk
                market_part, market_status = await self._capture_list(
                    "okx-v5:market:ticker",
                    self.okx.market_quotes(),
                )
                funding_part, funding_status = await self._capture_list(
                    "okx-v5:public:funding-rate",
                    self.okx.funding_quotes(),
                )
                markets.extend(market_part)
                fundings.extend(funding_part)
                if not market_status.ok and market_status.error_type not in {None, "EmptyResult"}:
                    market_errors.append(market_status.error_type)
                if not funding_status.ok and funding_status.error_type not in {None, "EmptyResult"}:
                    funding_errors.append(funding_status.error_type)
                del market_part, funding_part
        finally:
            self.okx.assets = original_assets
        market_ok = bool(markets) and not market_errors
        funding_ok = bool(fundings) and not funding_errors
        return (
            markets,
            fundings,
            ProviderStatus(
                provider="okx-v5:market:ticker",
                ok=market_ok,
                item_count=len(markets),
                error_type=None if market_ok else (market_errors[0] if market_errors else "EmptyResult"),
            ),
            ProviderStatus(
                provider="okx-v5:public:funding-rate",
                ok=funding_ok,
                item_count=len(fundings),
                error_type=None if funding_ok else (funding_errors[0] if funding_errors else "EmptyResult"),
            ),
        )

    async def collect_inputs(
        self,
    ) -> tuple[list[FundingQuote], list[MarketQuote], list[ProviderStatus]]:
        refreshed = await self._refresh_managed_assets()
        universe = refreshed or tuple(getattr(self.coinbase, "assets", ()))
        gate = asyncio.Semaphore(self.provider_group_concurrency)

        async def gated(factory):
            async with gate:
                result = await factory()
                gc.collect()
                return result

        async def hyper_group():
            funding, funding_status = await self._capture_list(
                "hyperliquid:predictedFundings",
                self.hyperliquid.funding_quotes(),
            )
            market, market_status = await self._capture_list(
                "hyperliquid:metaAndAssetCtxs",
                self.hyperliquid.market_quotes(),
            )
            return funding, market, funding_status, market_status

        async def coinbase_group():
            assets = tuple(getattr(self.coinbase, "assets", ()))
            if self._managed_coinbase and assets:
                return await self._chunked_market_surface(
                    provider="coinbase-exchange:ticker",
                    adapter=self.coinbase,
                    assets=assets,
                )
            return await self._capture_list("coinbase-exchange:ticker", self.coinbase.market_quotes())

        async def bybit_group():
            if not self.bybit_public_enabled:
                return [], [], None
            market, funding, status = await self._capture_bybit()
            return market, funding, status

        async def kraken_group():
            assets = tuple(getattr(self.kraken, "assets", ()))
            if self._managed_kraken and assets:
                return await self._chunked_market_surface(
                    provider="kraken:PreTrade",
                    adapter=self.kraken,
                    assets=assets,
                )
            return await self._capture_list("kraken:PreTrade", self.kraken.market_quotes())

        async def okx_group():
            assets = tuple(getattr(self.okx, "assets", ()))
            if self._managed_okx and assets:
                return await self._chunked_okx_surface(assets)
            market, market_status = await self._capture_list(
                "okx-v5:market:ticker",
                self.okx.market_quotes(),
            )
            funding, funding_status = await self._capture_list(
                "okx-v5:public:funding-rate",
                self.okx.funding_quotes(),
            )
            return market, funding, market_status, funding_status

        hyper, coinbase, bybit, kraken, okx = await asyncio.gather(
            gated(hyper_group),
            gated(coinbase_group),
            gated(bybit_group),
            gated(kraken_group),
            gated(okx_group),
        )
        hyper_funding, hyper_market, hyper_funding_status, hyper_market_status = hyper
        coinbase_market, coinbase_status = coinbase
        bybit_market, bybit_funding, bybit_status = bybit
        kraken_market, kraken_status = kraken
        okx_market, okx_funding, okx_market_status, okx_funding_status = okx
        funding_quotes = [*hyper_funding, *bybit_funding, *okx_funding]
        market_quotes = [*hyper_market, *coinbase_market, *bybit_market, *kraken_market, *okx_market]
        statuses = [
            hyper_funding_status,
            hyper_market_status,
            coinbase_status,
            *([bybit_status] if bybit_status is not None else []),
            kraken_status,
            okx_market_status,
            okx_funding_status,
        ]
        self._record_memory(
            "market_inputs_complete",
            universe_asset_count=len(universe),
            market_quote_count=len(market_quotes),
            funding_quote_count=len(funding_quotes),
        )
        return funding_quotes, market_quotes, statuses

    def book_request(self, leg):
        if leg.venue == "Bybit" and not self.bybit_public_enabled:
            return None
        return super().book_request(leg)

    def _trim_book(self, book: OrderBookSnapshot) -> OrderBookSnapshot:
        book.bids.sort(key=lambda level: level.price, reverse=True)
        book.asks.sort(key=lambda level: level.price)
        del book.bids[self.max_order_book_levels:]
        del book.asks[self.max_order_book_levels:]
        return book

    @staticmethod
    def _close_unstarted_request(request: object) -> None:
        awaitable = getattr(request, "awaitable", None)
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()

    async def collect_books_for_opportunities(
        self,
        opportunities: list[Opportunity],
    ) -> tuple[list[OrderBookSnapshot], list[ProviderStatus]]:
        requests: dict[tuple[str, str, str, str], object] = {}
        for opportunity in opportunities:
            for leg in opportunity.legs:
                key = book_identity(leg.venue, leg.asset, leg.market_kind, leg.contract_key)
                if key in requests:
                    continue
                request = self.book_request(leg)
                if request is not None:
                    requests[key] = request

        if not requests:
            self._record_memory("l2_complete", requested_book_count=0, retained_book_count=0)
            return [], []

        request_rows = list(requests.values())
        books: list[OrderBookSnapshot] = []
        statuses: list[ProviderStatus] = []

        async def capture(request: object):
            provider = str(getattr(request, "provider", "unknown"))
            try:
                book = await asyncio.wait_for(
                    getattr(request, "awaitable"),
                    timeout=self.order_book_timeout_seconds,
                )
                return self._trim_book(book), ProviderStatus(
                    provider=provider,
                    ok=True,
                    item_count=1,
                )
            except TimeoutError:
                return None, ProviderStatus(
                    provider=provider,
                    ok=False,
                    item_count=0,
                    error_type="TimeoutError",
                )
            except Exception as exc:
                return None, ProviderStatus(
                    provider=provider,
                    ok=False,
                    item_count=0,
                    error_type=type(exc).__name__,
                )

        for start in range(0, len(request_rows), self.order_book_batch_size):
            if memory_budget_exceeded(self.memory_soft_limit_mb):
                remaining = request_rows[start:]
                for request in remaining:
                    self._close_unstarted_request(request)
                    statuses.append(ProviderStatus(
                        provider=str(getattr(request, "provider", "unknown")),
                        ok=False,
                        item_count=0,
                        error_type="MemoryBudgetDeferred",
                    ))
                self._record_memory(
                    "l2_deferred",
                    requested_book_count=len(request_rows),
                    retained_book_count=len(books),
                    deferred_book_count=len(remaining),
                )
                break

            batch = request_rows[start:start + self.order_book_batch_size]
            results = await asyncio.gather(*(capture(request) for request in batch))
            for book, status in results:
                if book is not None:
                    books.append(book)
                statuses.append(status)
            del results, batch
            gc.collect()
            self._record_memory(
                "l2_batch_complete",
                requested_book_count=len(request_rows),
                retained_book_count=len(books),
                completed_book_count=len(statuses),
            )

        self._record_memory(
            "l2_complete",
            requested_book_count=len(request_rows),
            retained_book_count=len(books),
            completed_book_count=len(statuses),
        )
        return books, statuses
