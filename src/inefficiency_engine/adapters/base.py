from __future__ import annotations

from typing import Protocol

from inefficiency_engine.models import FundingQuote, MarketQuote


class FundingAdapter(Protocol):
    async def funding_quotes(self) -> list[FundingQuote]: ...


class MarketAdapter(Protocol):
    async def market_quotes(self) -> list[MarketQuote]: ...
