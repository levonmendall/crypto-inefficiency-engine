from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import httpx

from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


BASE_URL = "https://api.kraken.com"


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _pretrade_result(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Kraken PreTrade response must be an object")
    errors = payload.get("error")
    if isinstance(errors, list) and errors:
        raise ValueError(f"Kraken PreTrade error: {errors[0]}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Kraken PreTrade response must contain result")
    if isinstance(result.get("bids"), list) and isinstance(result.get("asks"), list):
        return result
    for value in result.values():
        if isinstance(value, dict) and isinstance(value.get("bids"), list) and isinstance(value.get("asks"), list):
            return value
    raise ValueError("Kraken PreTrade result has no order book")


def parse_pretrade(payload: Any, *, asset: str, symbol: str) -> OrderBookSnapshot:
    result = _pretrade_result(payload)

    def parse_side(name: str) -> list[OrderBookLevel]:
        rows = result.get(name)
        if not isinstance(rows, list):
            raise ValueError("Kraken PreTrade side must be a list")
        levels: list[OrderBookLevel] = []
        for row in rows:
            if isinstance(row, dict) and row.get("price") is not None and row.get("qty") is not None:
                levels.append(OrderBookLevel(price=float(row["price"]), size=float(row["qty"])))
            elif isinstance(row, list) and len(row) >= 2:
                levels.append(OrderBookLevel(price=float(row[0]), size=float(row[1])))
        return levels

    timestamps = []
    for side_name in ("bids", "asks"):
        for row in result.get(side_name, []):
            if isinstance(row, dict):
                parsed = _parse_time(row.get("publication_ts"))
                if parsed is not None:
                    timestamps.append(parsed)
    observed_at = max(timestamps) if timestamps else datetime.now(timezone.utc)
    return OrderBookSnapshot(
        venue="Kraken",
        asset=asset.upper(),
        market_kind=MarketKind.SPOT,
        symbol=symbol,
        quote_currency="USD",
        contract_key="spot",
        bids=parse_side("bids"),
        asks=parse_side("asks"),
        observed_at=observed_at,
        source="kraken:PreTrade",
    )


class KrakenSpotAdapter:
    """Public Kraken USD spot market-data adapter; no credentials required."""

    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH", "SOL"), client: httpx.AsyncClient | None = None):
        self.assets = tuple(asset.upper() for asset in assets)
        self._client = client

    async def _get(self, *, symbol: str) -> Any:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0, headers={"Cache-Control": "no-cache"})
        try:
            response = await client.get(f"{BASE_URL}/0/public/PreTrade", params={"symbol": symbol})
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    async def market_quotes(self) -> list[MarketQuote]:
        async def for_asset(asset: str) -> MarketQuote | None:
            symbol = f"{asset}/USD"
            try:
                book = parse_pretrade(await self._get(symbol=symbol), asset=asset, symbol=symbol)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {400, 404}:
                    return None
                raise
            except ValueError:
                # Kraken represents unsupported pairs as a successful HTTP response
                # with an error payload. Skip only that asset; do not collapse the
                # rest of the bounded provider surface.
                return None
            bid = max(level.price for level in book.bids)
            ask = min(level.price for level in book.asks)
            return MarketQuote(
                venue="Kraken",
                asset=asset,
                market_kind=MarketKind.SPOT,
                symbol=symbol,
                quote_currency="USD",
                contract_key="spot",
                bid=bid,
                ask=ask,
                mid=(bid + ask) / 2.0,
                observed_at=book.observed_at,
                source="kraken:PreTrade",
            )

        rows = await asyncio.gather(*(for_asset(asset) for asset in self.assets))
        return [row for row in rows if row is not None]

    async def order_book(self, asset: str, *, symbol: str | None = None) -> OrderBookSnapshot:
        symbol = symbol or f"{asset.upper()}/USD"
        started = perf_counter()
        payload = await self._get(symbol=symbol)
        latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
        book = parse_pretrade(payload, asset=asset, symbol=symbol)
        book.request_latency_ms = latency_ms
        return book
