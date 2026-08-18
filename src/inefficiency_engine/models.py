from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class MarketKind(str, Enum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Strategy(str, Enum):
    FUNDING_DISPERSION = "funding_dispersion"
    SPOT_PERP_BASIS = "spot_perp_basis"


class MarketQuote(BaseModel):
    venue: str
    asset: str
    market_kind: MarketKind
    symbol: str
    bid: float | None = None
    ask: float | None = None
    mid: float
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str

    @model_validator(mode="after")
    def validate_prices(self):
        values = [self.mid, *[x for x in (self.bid, self.ask) if x is not None]]
        if any((not isfinite(x) or x <= 0) for x in values):
            raise ValueError("prices must be positive finite numbers")
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("bid cannot exceed ask")
        return self


class OrderBookLevel(BaseModel):
    price: float = Field(gt=0)
    size: float = Field(gt=0)


class OrderBookSnapshot(BaseModel):
    venue: str
    asset: str
    market_kind: MarketKind
    symbol: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str

    @model_validator(mode="after")
    def validate_book(self):
        if not self.bids or not self.asks:
            raise ValueError("order book must have both bids and asks")
        if max(level.price for level in self.bids) >= min(level.price for level in self.asks):
            raise ValueError("order book must have a positive spread")
        return self


class FundingQuote(BaseModel):
    venue: str
    asset: str
    rate: float
    interval_hours: float = Field(gt=0, le=24)
    next_funding_time: datetime | None = None
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str

    @property
    def hourly_rate(self) -> float:
        return self.rate / self.interval_hours

    @property
    def annualized_simple(self) -> float:
        return self.hourly_rate * 24 * 365


class OpportunityLeg(BaseModel):
    venue: str
    asset: str
    market_kind: MarketKind
    side: Side
    reference_price: float | None = None


class Opportunity(BaseModel):
    id: str
    strategy: Strategy
    asset: str
    legs: list[OpportunityLeg]
    gross_edge_bps_per_hour: float
    modeled_cost_bps: float
    holding_hours: float
    safety_buffer_bps_per_hour: float
    net_edge_bps_per_hour: float
    net_annualized_return: float
    observed_at: datetime
    expires_at: datetime
    confidence: Literal["low", "medium", "high"] = "medium"
    evidence: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True
