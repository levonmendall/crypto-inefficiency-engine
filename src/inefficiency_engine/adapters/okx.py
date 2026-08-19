from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import httpx

from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


DEFAULT_BASE_URL = "https://www.okx.com"


def _utc_ms(value: str | int | float | None) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)


def _unwrap(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        raise ValueError("OKX response is not successful")
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("OKX response data must be a list")
    return [row for row in data if isinstance(row, dict)]


def parse_ticker(payload: Any, *, asset: str, market_kind: MarketKind, symbol: str, quote_currency: str) -> MarketQuote:
    rows = _unwrap(payload)
    if not rows:
        raise ValueError("OKX ticker response is empty")
    row = rows[0]
    bid, ask = float(row["bidPx"]), float(row["askPx"])
    return MarketQuote(
        venue="OKX", asset=asset.upper(), market_kind=market_kind, symbol=symbol,
        quote_currency=quote_currency.upper(), contract_key="spot" if market_kind == MarketKind.SPOT else "continuous",
        bid=bid, ask=ask, mid=(bid + ask) / 2.0,
        observed_at=_utc_ms(row.get("ts")) or datetime.now(timezone.utc), source="okx-v5:market:ticker",
    )


def parse_funding_rate(payload: Any, *, asset: str, symbol: str, quote_currency: str) -> FundingQuote:
    rows = _unwrap(payload)
    if not rows:
        raise ValueError("OKX funding response is empty")
    row = rows[0]
    funding_time, next_funding = _utc_ms(row.get("fundingTime")), _utc_ms(row.get("nextFundingTime"))
    interval_hours = 8.0
    if funding_time is not None and next_funding is not None and next_funding > funding_time:
        interval_hours = (next_funding - funding_time).total_seconds() / 3600.0
    rate_raw = row.get("fundingRate") or row.get("nextFundingRate")
    if rate_raw in (None, ""):
        raise ValueError("OKX funding response has no funding rate")
    return FundingQuote(
        venue="OKX", asset=asset.upper(), rate=float(rate_raw), interval_hours=max(0.25, min(24.0, interval_hours)),
        symbol=symbol, quote_currency=quote_currency.upper(), contract_key="continuous",
        next_funding_time=funding_time or next_funding, observed_at=datetime.now(timezone.utc),
        source="okx-v5:public:funding-rate",
    )


def parse_order_book(payload: Any, *, asset: str, market_kind: MarketKind, symbol: str, quote_currency: str) -> OrderBookSnapshot:
    rows = _unwrap(payload)
    if not rows:
        raise ValueError("OKX order book response is empty")
    row = rows[0]
    def side(values: Any) -> list[OrderBookLevel]:
        if not isinstance(values, list):
            return []
        return [OrderBookLevel(price=float(item[0]), size=float(item[1])) for item in values
                if isinstance(item, list) and len(item) >= 2]
    return OrderBookSnapshot(
        venue="OKX", asset=asset.upper(), market_kind=market_kind, symbol=symbol,
        quote_currency=quote_currency.upper(), contract_key="spot" if market_kind == MarketKind.SPOT else "continuous",
        bids=side(row.get("bids")), asks=side(row.get("asks")),
        observed_at=_utc_ms(row.get("ts")) or datetime.now(timezone.utc), source="okx-v5:market:books",
    )


class OKXPublicAdapter:
    """Public OKX market-data adapter; no credentials or trading endpoints."""
    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH", "SOL"), quote_currency: str = "USDT",
                 base_url: str = DEFAULT_BASE_URL, client: httpx.AsyncClient | None = None):
        self.assets = tuple(asset.upper() for asset in assets)
        self.quote_currency = quote_currency.upper()
        self.base_url = base_url.rstrip("/")
        self._client = client

    async def _get(self, path: str, *, params: dict[str, object]) -> Any:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0, headers={"Cache-Control": "no-cache"})
        try:
            response = await client.get(f"{self.base_url}{path}", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns:
                await client.aclose()

    def symbol(self, asset: str, market_kind: MarketKind) -> str:
        asset = asset.upper()
        if market_kind == MarketKind.SPOT:
            return f"{asset}-{self.quote_currency}"
        if market_kind == MarketKind.PERPETUAL:
            return f"{asset}-{self.quote_currency}-SWAP"
        raise ValueError("OKX adapter currently supports spot and perpetual only")

    async def market_quotes(self) -> list[MarketQuote]:
        quotes: list[MarketQuote] = []
        for asset in self.assets:
            for kind in (MarketKind.SPOT, MarketKind.PERPETUAL):
                symbol = self.symbol(asset, kind)
                try:
                    payload = await self._get("/api/v5/market/ticker", params={"instId": symbol})
                    quotes.append(parse_ticker(payload, asset=asset, market_kind=kind, symbol=symbol,
                                               quote_currency=self.quote_currency))
                except (httpx.HTTPStatusError, ValueError, KeyError):
                    continue
        return quotes

    async def funding_quotes(self) -> list[FundingQuote]:
        quotes: list[FundingQuote] = []
        for asset in self.assets:
            symbol = self.symbol(asset, MarketKind.PERPETUAL)
            try:
                payload = await self._get("/api/v5/public/funding-rate", params={"instId": symbol})
                quotes.append(parse_funding_rate(payload, asset=asset, symbol=symbol, quote_currency=self.quote_currency))
            except (httpx.HTTPStatusError, ValueError, KeyError):
                continue
        return quotes

    async def order_book(self, asset: str, market_kind: MarketKind, *, symbol: str | None = None) -> OrderBookSnapshot:
        symbol = symbol or self.symbol(asset, market_kind)
        started = perf_counter()
        payload = await self._get("/api/v5/market/books", params={"instId": symbol, "sz": 100})
        latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
        book = parse_order_book(payload, asset=asset, market_kind=market_kind, symbol=symbol,
                                quote_currency=self.quote_currency)
        book.request_latency_ms = latency_ms
        return book
