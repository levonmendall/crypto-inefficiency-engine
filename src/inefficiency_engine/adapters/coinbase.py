from __future__ import annotations

from datetime import datetime, timezone

import httpx

from inefficiency_engine.models import MarketKind, MarketQuote


BASE_URL = "https://api.exchange.coinbase.com"


class CoinbaseSpotAdapter:
    """Public Coinbase Exchange ticker adapter; no credentials required."""

    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH", "SOL"), client: httpx.AsyncClient | None = None):
        self.assets = assets
        self._client = client

    async def market_quotes(self) -> list[MarketQuote]:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "crypto-inefficiency-engine/0.1"})
        quotes: list[MarketQuote] = []
        try:
            for asset in self.assets:
                symbol = f"{asset}-USD"
                response = await client.get(f"{BASE_URL}/products/{symbol}/ticker")
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                data = response.json()
                bid = float(data["bid"])
                ask = float(data["ask"])
                quotes.append(
                    MarketQuote(
                        venue="Coinbase",
                        asset=asset,
                        market_kind=MarketKind.SPOT,
                        symbol=symbol,
                        bid=bid,
                        ask=ask,
                        mid=(bid + ask) / 2.0,
                        observed_at=datetime.now(timezone.utc),
                        source="coinbase-exchange:ticker",
                    )
                )
            return quotes
        finally:
            if owns_client:
                await client.aclose()
