from __future__ import annotations

import asyncio

import httpx

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.models import OrderBookSnapshot


class CoinbaseStablecoinDepthAdapter:
    """Public Coinbase Exchange level-2 books for USDC-USD and USDT-USD."""

    assets: tuple[str, ...] = ("USDC", "USDT")

    def __init__(self, client: httpx.AsyncClient | None = None):
        self._spot = CoinbaseSpotAdapter(client=client, assets=self.assets)

    async def books(self) -> list[OrderBookSnapshot]:
        results = await asyncio.gather(
            *(self._spot.order_book(asset) for asset in self.assets),
            return_exceptions=True,
        )
        books: list[OrderBookSnapshot] = []
        errors: list[BaseException] = []
        for result in results:
            if isinstance(result, BaseException):
                errors.append(result)
            else:
                books.append(result)
        if errors:
            raise RuntimeError(
                "stablecoin conversion depth requires both USDC-USD and USDT-USD public books"
            ) from errors[0]
        if len(books) != len(self.assets):
            raise RuntimeError("stablecoin conversion depth returned incomplete public books")
        return books
