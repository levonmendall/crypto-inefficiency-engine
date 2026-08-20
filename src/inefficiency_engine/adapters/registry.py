from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable

from pydantic import BaseModel, Field

from inefficiency_engine.adapters.bybit import BybitPublicAdapter
from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.hyperliquid import HyperliquidAdapter
from inefficiency_engine.adapters.kraken import KrakenSpotAdapter
from inefficiency_engine.adapters.okx import OKXPublicAdapter
from inefficiency_engine.asset_universe import configured_liquid_research_assets
from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.instrument_identity import book_identity
from inefficiency_engine.models import (
    FundingQuote,
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityLeg,
    OrderBookSnapshot,
    Side,
)


DEFAULT_PROVIDER_SURFACE_TIMEOUT_SECONDS = 8.0
DEFAULT_ORDER_BOOK_TIMEOUT_SECONDS = 8.0


class ProviderSurfaceDiagnostic(BaseModel):
    provider: str
    venue: str | None = None
    capability: str
    ok: bool
    item_count: int = 0
    error_type: str | None = None
    sample_symbols: list[str] = Field(default_factory=list)


class OrderBookDiagnostic(BaseModel):
    provider: str
    venue: str
    asset: str
    market_kind: MarketKind
    symbol: str | None = None
    ok: bool
    bid_levels: int = 0
    ask_levels: int = 0
    request_latency_ms: float | None = None
    error_type: str | None = None


class ProviderDiagnosticReport(BaseModel):
    started_at: datetime
    completed_at: datetime
    healthy: bool
    paper_only: bool = True
    venue_count: int
    market_quote_count: int
    funding_quote_count: int
    surfaces: list[ProviderSurfaceDiagnostic] = Field(default_factory=list)
    order_books: list[OrderBookDiagnostic] = Field(default_factory=list)


@dataclass(frozen=True)
class BookRequest:
    provider: str
    awaitable: Awaitable[OrderBookSnapshot]


class PublicAdapterRegistry:
    """Single routing boundary for public market data and visible L2.

    Adapter-specific quirks live here instead of leaking into OpportunityService.
    The registry has no order-entry or private-account capability. Provider and
    order-book fanout is independently time-bounded: a wedged venue becomes a
    failed provider status instead of preventing the entire point-in-time scan
    from being persisted.
    """

    def __init__(
        self,
        *,
        hyperliquid: object | None = None,
        coinbase: object | None = None,
        bybit: object | None = None,
        kraken: object | None = None,
        okx: object | None = None,
        provider_surface_timeout_seconds: float = DEFAULT_PROVIDER_SURFACE_TIMEOUT_SECONDS,
        order_book_timeout_seconds: float = DEFAULT_ORDER_BOOK_TIMEOUT_SECONDS,
    ):
        research_assets = configured_liquid_research_assets()
        self.hyperliquid = hyperliquid or HyperliquidAdapter()
        self.coinbase = coinbase or CoinbaseSpotAdapter(assets=research_assets)
        self.bybit = bybit or BybitPublicAdapter(assets=research_assets)
        self.kraken = kraken or KrakenSpotAdapter(assets=research_assets)
        self.okx = okx or OKXPublicAdapter(assets=research_assets)
        self.provider_surface_timeout_seconds = max(0.05, float(provider_surface_timeout_seconds))
        self.order_book_timeout_seconds = max(0.05, float(order_book_timeout_seconds))

    @staticmethod
    def provider_venue(provider: str) -> str | None:
        lowered = provider.lower()
        prefixes = (
            ("hyperliquid:", "HlPerp"),
            ("coinbase", "Coinbase"),
            ("bybit", "Bybit"),
            ("kraken", "Kraken"),
            ("okx", "OKX"),
        )
        for prefix, venue in prefixes:
            if lowered.startswith(prefix):
                return venue
        return None

    async def _capture_list(self, provider: str, awaitable) -> tuple[list[object], ProviderStatus]:
        try:
            items = list(await asyncio.wait_for(
                awaitable,
                timeout=self.provider_surface_timeout_seconds,
            ))
            if not items:
                return [], ProviderStatus(provider=provider, ok=False, item_count=0, error_type="EmptyResult")
            return items, ProviderStatus(provider=provider, ok=True, item_count=len(items))
        except TimeoutError:
            return [], ProviderStatus(provider=provider, ok=False, item_count=0, error_type="TimeoutError")
        except Exception as exc:
            return [], ProviderStatus(provider=provider, ok=False, item_count=0, error_type=type(exc).__name__)

    async def _capture_bybit(self) -> tuple[list[MarketQuote], list[FundingQuote], ProviderStatus]:
        try:
            market, funding = await asyncio.wait_for(
                self.bybit.market_snapshot(),
                timeout=self.provider_surface_timeout_seconds,
            )
            ok = bool(market) and bool(funding)
            return list(market), list(funding), ProviderStatus(
                provider="bybit-v5:market-snapshot",
                ok=ok,
                item_count=len(market) + len(funding),
                error_type=None if ok else "PartialOrEmptyResult",
            )
        except TimeoutError:
            return [], [], ProviderStatus(
                provider="bybit-v5:market-snapshot", ok=False, item_count=0, error_type="TimeoutError"
            )
        except Exception as exc:
            return [], [], ProviderStatus(
                provider="bybit-v5:market-snapshot", ok=False, item_count=0, error_type=type(exc).__name__
            )

    async def collect_inputs(
        self,
    ) -> tuple[list[FundingQuote], list[MarketQuote], list[ProviderStatus]]:
        results = await asyncio.gather(
            self._capture_list("hyperliquid:predictedFundings", self.hyperliquid.funding_quotes()),
            self._capture_list("hyperliquid:metaAndAssetCtxs", self.hyperliquid.market_quotes()),
            self._capture_list("coinbase-exchange:ticker", self.coinbase.market_quotes()),
            self._capture_bybit(),
            self._capture_list("kraken:PreTrade", self.kraken.market_quotes()),
            self._capture_list("okx-v5:market:ticker", self.okx.market_quotes()),
            self._capture_list("okx-v5:public:funding-rate", self.okx.funding_quotes()),
        )
        hyper_funding, hyper_funding_status = results[0]
        hyper_market, hyper_market_status = results[1]
        coinbase_market, coinbase_status = results[2]
        bybit_market, bybit_funding, bybit_status = results[3]
        kraken_market, kraken_status = results[4]
        okx_market, okx_market_status = results[5]
        okx_funding, okx_funding_status = results[6]
        funding_quotes = [*hyper_funding, *bybit_funding, *okx_funding]
        market_quotes = [*hyper_market, *coinbase_market, *bybit_market, *kraken_market, *okx_market]
        statuses = [
            hyper_funding_status,
            hyper_market_status,
            coinbase_status,
            bybit_status,
            kraken_status,
            okx_market_status,
            okx_funding_status,
        ]
        return funding_quotes, market_quotes, statuses

    def book_request(self, leg: OpportunityLeg) -> BookRequest | None:
        if leg.venue == "HlPerp" and leg.market_kind == MarketKind.PERPETUAL:
            return BookRequest(
                provider=f"hyperliquid:l2Book:{leg.asset}",
                awaitable=self.hyperliquid.order_book(leg.asset),
            )
        if leg.venue == "Coinbase" and leg.market_kind == MarketKind.SPOT:
            return BookRequest(
                provider=f"coinbase-exchange:book-level2:{leg.asset}",
                awaitable=self.coinbase.order_book(leg.asset),
            )
        if leg.venue == "Kraken" and leg.market_kind == MarketKind.SPOT:
            return BookRequest(
                provider=f"kraken:PreTrade:{leg.asset}",
                awaitable=self.kraken.order_book(leg.asset, symbol=leg.symbol),
            )
        if leg.venue == "Bybit" and leg.symbol is not None:
            return BookRequest(
                provider=f"bybit-v5:orderbook:{leg.symbol}",
                awaitable=self.bybit.order_book(
                    asset=leg.asset,
                    market_kind=leg.market_kind,
                    symbol=leg.symbol,
                    quote_currency=leg.quote_currency,
                    contract_key=leg.contract_key,
                    expires_at=leg.expires_at,
                ),
            )
        if leg.venue == "OKX" and leg.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
            return BookRequest(
                provider=f"okx-v5:market:books:{leg.symbol or leg.asset}",
                awaitable=self.okx.order_book(leg.asset, leg.market_kind, symbol=leg.symbol),
            )
        return None

    async def collect_books_for_opportunities(
        self, opportunities: list[Opportunity]
    ) -> tuple[list[OrderBookSnapshot], list[ProviderStatus]]:
        requests: dict[tuple[str, str, str, str], BookRequest] = {}
        for opportunity in opportunities:
            for leg in opportunity.legs:
                key = book_identity(leg.venue, leg.asset, leg.market_kind, leg.contract_key)
                if key in requests:
                    continue
                request = self.book_request(leg)
                if request is not None:
                    requests[key] = request

        async def capture(request: BookRequest):
            try:
                book = await asyncio.wait_for(
                    request.awaitable,
                    timeout=self.order_book_timeout_seconds,
                )
                return book, ProviderStatus(provider=request.provider, ok=True, item_count=1)
            except TimeoutError:
                return None, ProviderStatus(
                    provider=request.provider, ok=False, item_count=0, error_type="TimeoutError"
                )
            except Exception as exc:
                return None, ProviderStatus(
                    provider=request.provider, ok=False, item_count=0, error_type=type(exc).__name__
                )

        if not requests:
            return [], []
        results = await asyncio.gather(*(capture(request) for request in requests.values()))
        books = [book for book, _ in results if book is not None]
        statuses = [status for _, status in results]
        return books, statuses

    @staticmethod
    def _sample_symbols(
        status: ProviderStatus, market_quotes: list[MarketQuote], funding_quotes: list[FundingQuote]
    ) -> list[str]:
        venue = PublicAdapterRegistry.provider_venue(status.provider)
        values: list[str] = []
        if venue is not None:
            values.extend(q.symbol for q in market_quotes if q.venue == venue and q.symbol)
            values.extend(q.symbol for q in funding_quotes if q.venue == venue and q.symbol)
        return sorted(set(values))[:5]

    async def diagnose(self) -> ProviderDiagnosticReport:
        started_at = datetime.now(timezone.utc)
        funding_quotes, market_quotes, statuses = await self.collect_inputs()
        surfaces = [
            ProviderSurfaceDiagnostic(
                provider=status.provider,
                venue=self.provider_venue(status.provider),
                capability=(
                    "funding"
                    if "funding" in status.provider.lower()
                    else "market_and_funding"
                    if status.provider.startswith("bybit")
                    else "market"
                ),
                ok=status.ok,
                item_count=status.item_count,
                error_type=status.error_type,
                sample_symbols=self._sample_symbols(status, market_quotes, funding_quotes),
            )
            for status in statuses
        ]

        representative: dict[tuple[str, MarketKind], MarketQuote] = {}
        for quote in market_quotes:
            representative.setdefault((quote.venue, quote.market_kind), quote)

        async def check_book(quote: MarketQuote) -> OrderBookDiagnostic | None:
            leg = OpportunityLeg(
                venue=quote.venue,
                asset=quote.asset,
                market_kind=quote.market_kind,
                side=Side.LONG,
                symbol=quote.symbol,
                quote_currency=quote.quote_currency,
                contract_key=quote.contract_key,
                expires_at=quote.expires_at,
                reference_price=quote.mid,
            )
            request = self.book_request(leg)
            if request is None:
                return None
            try:
                book = await asyncio.wait_for(
                    request.awaitable,
                    timeout=self.order_book_timeout_seconds,
                )
                return OrderBookDiagnostic(
                    provider=request.provider,
                    venue=book.venue,
                    asset=book.asset,
                    market_kind=book.market_kind,
                    symbol=book.symbol,
                    ok=True,
                    bid_levels=len(book.bids),
                    ask_levels=len(book.asks),
                    request_latency_ms=book.request_latency_ms,
                )
            except TimeoutError:
                return OrderBookDiagnostic(
                    provider=request.provider,
                    venue=quote.venue,
                    asset=quote.asset,
                    market_kind=quote.market_kind,
                    symbol=quote.symbol,
                    ok=False,
                    error_type="TimeoutError",
                )
            except Exception as exc:
                return OrderBookDiagnostic(
                    provider=request.provider,
                    venue=quote.venue,
                    asset=quote.asset,
                    market_kind=quote.market_kind,
                    symbol=quote.symbol,
                    ok=False,
                    error_type=type(exc).__name__,
                )

        book_results = await asyncio.gather(*(check_book(quote) for quote in representative.values()))
        order_books = [item for item in book_results if item is not None]
        completed_at = datetime.now(timezone.utc)
        healthy = bool(surfaces) and all(item.ok for item in surfaces) and all(item.ok for item in order_books)
        return ProviderDiagnosticReport(
            started_at=started_at,
            completed_at=completed_at,
            healthy=healthy,
            paper_only=True,
            venue_count=len({q.venue for q in market_quotes} | {q.venue for q in funding_quotes}),
            market_quote_count=len(market_quotes),
            funding_quote_count=len(funding_quotes),
            surfaces=surfaces,
            order_books=order_books,
        )
