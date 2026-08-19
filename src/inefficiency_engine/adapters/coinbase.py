from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import httpx

from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


BASE_URL = "https://api.exchange.coinbase.com"


def parse_product_book(payload: Any, *, asset: str, symbol: str) -> OrderBookSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("Coinbase product book must be an object")

    def parse_side(rows: Any) -> list[OrderBookLevel]:
        if not isinstance(rows, list):
            raise ValueError("Coinbase product book side must be a list")
        levels: list[OrderBookLevel] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            levels.append(OrderBookLevel(price=float(row[0]), size=float(row[1])))
        return levels

    raw_time = payload.get("time")
    observed_at = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00")) if raw_time else datetime.now(timezone.utc)
    return OrderBookSnapshot(
        venue="Coinbase",
        asset=asset.upper(),
        market_kind=MarketKind.SPOT,
        symbol=symbol,
        quote_currency="USD",
        contract_key="spot",
        bids=parse_side(payload.get("bids")),
        asks=parse_side(payload.get("asks")),
        observed_at=observed_at,
        source="coinbase-exchange:book-level2",
    )


class CoinbaseSpotAdapter:
    """Public Coinbase Exchange market-data adapter; no credentials required."""

    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH", "SOL"), client: httpx.AsyncClient | None = None):
        self.assets = assets
        self._client = client

    async def _get(self, path: str, *, params: dict[str, object] | None = None) -> Any:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": "crypto-inefficiency-engine/0.9", "Cache-Control": "no-cache"},
        )
        try:
            response = await client.get(f"{BASE_URL}{path}", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    async def market_quotes(self) -> list[MarketQuote]:
        quotes: list[MarketQuote] = []
        for asset in self.assets:
            symbol = f"{asset}-USD"
            try:
                data = await self._get(f"/products/{symbol}/ticker")
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            bid = float(data["bid"])
            ask = float(data["ask"])
            quotes.append(
                MarketQuote(
                    venue="Coinbase",
                    asset=asset,
                    market_kind=MarketKind.SPOT,
                    symbol=symbol,
                    quote_currency="USD",
                    contract_key="spot",
                    bid=bid,
                    ask=ask,
                    mid=(bid + ask) / 2.0,
                    observed_at=datetime.now(timezone.utc),
                    source="coinbase-exchange:ticker",
                )
            )
        return quotes

    async def order_book(self, asset: str) -> OrderBookSnapshot:
        symbol = f"{asset.upper()}-USD"
        started = perf_counter()
        payload = await self._get(f"/products/{symbol}/book", params={"level": 2})
        latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
        book = parse_product_book(payload, asset=asset, symbol=symbol)
        book.request_latency_ms = latency_ms
        return book
