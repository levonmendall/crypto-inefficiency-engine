from __future__ import annotations

from inefficiency_engine.adapters.stablecoin_depth import CoinbaseStablecoinDepthAdapter
from inefficiency_engine.config import Settings
from inefficiency_engine.conversion_depth import (
    StablecoinConversionDepthQuote,
    quote_stablecoin_conversion_depth,
)
from inefficiency_engine.models import OrderBookSnapshot


class StablecoinConversionDepthService:
    def __init__(
        self,
        settings: Settings,
        *,
        adapter: CoinbaseStablecoinDepthAdapter | None = None,
    ):
        self.settings = settings
        self.adapter = adapter or CoinbaseStablecoinDepthAdapter()

    async def collect_books(self) -> list[OrderBookSnapshot]:
        return await self.adapter.books()

    async def quote(
        self,
        source_currency: str,
        target_currency: str,
        input_amount: float,
    ) -> StablecoinConversionDepthQuote:
        books = await self.collect_books()
        return quote_stablecoin_conversion_depth(
            source_currency,
            target_currency,
            input_amount,
            books,
            max_book_age_seconds=self.settings.max_order_book_age_seconds,
            max_book_skew_seconds=self.settings.max_order_book_skew_seconds,
        )
