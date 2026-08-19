from __future__ import annotations

import asyncio

import httpx

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.models import OrderBookSnapshot


class CoinbaseStablecoinDepthAdapter:
    """Public Coinbase Exchange level-2 books for USDC-USD and USDT-USD.

    Each public book is collected independently. A provider/data failure for one
    stablecoin must not erase valid depth for the other. Downstream conversion
    qualification remains fail-closed: a route that requires a missing book is
    rejected by ``quote_stablecoin_conversion_depth``.

    Unexpected programming/runtime exceptions are deliberately re-raised rather
    than being mistaken for ordinary provider degradation.
    """

    assets: tuple[str, ...] = ("USDC", "USDT")

    def __init__(self, client: httpx.AsyncClient | None = None):
        self._spot = CoinbaseSpotAdapter(client=client, assets=self.assets)

    async def books(self) -> list[OrderBookSnapshot]:
        results = await asyncio.gather(
            *(self._spot.order_book(asset) for asset in self.assets),
            return_exceptions=True,
        )
        books: list[OrderBookSnapshot] = []
        for result in results:
            if isinstance(result, BaseException):
                # HTTP/network failures and malformed public payloads make only
                # that book unavailable. Route-level conversion qualification
                # will reject any opportunity that actually requires it.
                if isinstance(result, (httpx.HTTPError, ValueError)):
                    continue
                raise result
            books.append(result)
        return books
