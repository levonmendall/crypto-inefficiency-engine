from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.execution import InsufficientDepthError, estimate_market_order, estimate_market_order_quantity
from inefficiency_engine.models import MarketKind, OrderBookSnapshot, TradeSide


SUPPORTED_STABLECOINS = {"USDC", "USDT"}
SUPPORTED_CURRENCIES = {"USD", *SUPPORTED_STABLECOINS}


class StablecoinConversionDepthLeg(BaseModel):
    source_currency: str
    target_currency: str
    input_amount: float = Field(gt=0)
    output_amount: float = Field(gt=0)
    effective_rate: float = Field(gt=0)
    best_rate: float = Field(gt=0)
    slippage_bps: float = Field(ge=0)
    levels_consumed: int = Field(gt=0)
    book_symbol: str
    book_observed_at: datetime
    request_latency_ms: float | None = Field(default=None, ge=0)
    visible_depth_only: bool = True


class StablecoinConversionDepthQuote(BaseModel):
    quote_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    venue: str = "Coinbase"
    source_currency: str
    target_currency: str
    input_amount: float = Field(gt=0)
    output_amount: float = Field(gt=0)
    effective_rate: float = Field(gt=0)
    total_slippage_bps: float = Field(ge=0)
    legs: list[StablecoinConversionDepthLeg] = Field(min_length=1, max_length=2)
    observed_at: datetime
    max_book_age_seconds: float = Field(ge=0)
    book_skew_seconds: float = Field(ge=0)
    source: str = "coinbase-exchange:book-level2"
    visible_depth_only: bool = True
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def _validate_currency(currency: str) -> str:
    value = currency.upper()
    if value not in SUPPORTED_CURRENCIES:
        raise ValueError(f"unsupported stablecoin conversion currency: {currency}")
    return value


def _stablecoin_book(books: list[OrderBookSnapshot], stablecoin: str) -> OrderBookSnapshot:
    expected_symbol = f"{stablecoin}-USD"
    matches = [
        book for book in books
        if book.venue == "Coinbase"
        and book.asset.upper() == stablecoin
        and book.market_kind == MarketKind.SPOT
        and book.symbol == expected_symbol
        and (book.quote_currency or "").upper() == "USD"
    ]
    if len(matches) != 1:
        raise ValueError(f"exactly one Coinbase {expected_symbol} level-2 book is required")
    return matches[0]


def _book_age_seconds(book: OrderBookSnapshot, now: datetime) -> float:
    return max(0.0, (now - book.observed_at).total_seconds())


def _stablecoin_to_usd(stablecoin: str, amount: float, book: OrderBookSnapshot) -> StablecoinConversionDepthLeg:
    estimate = estimate_market_order_quantity(book, TradeSide.SELL, amount)
    output = estimate.filled_notional_usd
    return StablecoinConversionDepthLeg(
        source_currency=stablecoin,
        target_currency="USD",
        input_amount=amount,
        output_amount=output,
        effective_rate=output / amount,
        best_rate=estimate.best_price,
        slippage_bps=estimate.slippage_bps,
        levels_consumed=estimate.levels_consumed,
        book_symbol=book.symbol,
        book_observed_at=book.observed_at,
        request_latency_ms=book.request_latency_ms,
    )


def _usd_to_stablecoin(stablecoin: str, amount_usd: float, book: OrderBookSnapshot) -> StablecoinConversionDepthLeg:
    # USD is the quote-currency budget. estimate_market_order walks asks by
    # notional and returns the purchased stablecoin quantity as base_quantity.
    estimate = estimate_market_order(book, TradeSide.BUY, amount_usd)
    output = estimate.base_quantity
    best_rate = 1.0 / estimate.best_price
    effective_rate = output / amount_usd
    slippage = max(0.0, (best_rate - effective_rate) / best_rate * 10_000.0)
    return StablecoinConversionDepthLeg(
        source_currency="USD",
        target_currency=stablecoin,
        input_amount=amount_usd,
        output_amount=output,
        effective_rate=effective_rate,
        best_rate=best_rate,
        slippage_bps=slippage,
        levels_consumed=estimate.levels_consumed,
        book_symbol=book.symbol,
        book_observed_at=book.observed_at,
        request_latency_ms=book.request_latency_ms,
    )


def quote_stablecoin_conversion_depth(
    source_currency: str,
    target_currency: str,
    input_amount: float,
    books: list[OrderBookSnapshot],
    *,
    now: datetime | None = None,
    max_book_age_seconds: float = 15.0,
    max_book_skew_seconds: float = 5.0,
) -> StablecoinConversionDepthQuote:
    source = _validate_currency(source_currency)
    target = _validate_currency(target_currency)
    if source == target:
        raise ValueError("source and target currencies must differ")
    if input_amount <= 0:
        raise ValueError("input_amount must be positive")

    now = now or datetime.now(timezone.utc)
    needed: list[OrderBookSnapshot] = []
    if source in SUPPORTED_STABLECOINS:
        needed.append(_stablecoin_book(books, source))
    if target in SUPPORTED_STABLECOINS and target != source:
        needed.append(_stablecoin_book(books, target))
    if not needed:
        raise ValueError("at least one stablecoin/USD book is required")

    ages = [_book_age_seconds(book, now) for book in needed]
    worst_age = max(ages)
    if worst_age > max_book_age_seconds:
        raise ValueError(f"stablecoin conversion book stale: {worst_age:.6f}s > {max_book_age_seconds:.6f}s")
    skew = 0.0
    if len(needed) > 1:
        times = [book.observed_at for book in needed]
        skew = (max(times) - min(times)).total_seconds()
        if skew > max_book_skew_seconds:
            raise ValueError(f"stablecoin conversion book skew too high: {skew:.6f}s > {max_book_skew_seconds:.6f}s")

    legs: list[StablecoinConversionDepthLeg] = []
    running_amount = input_amount
    if source == "USD":
        leg = _usd_to_stablecoin(target, running_amount, _stablecoin_book(books, target))
        legs.append(leg)
        running_amount = leg.output_amount
    elif target == "USD":
        leg = _stablecoin_to_usd(source, running_amount, _stablecoin_book(books, source))
        legs.append(leg)
        running_amount = leg.output_amount
    else:
        first = _stablecoin_to_usd(source, running_amount, _stablecoin_book(books, source))
        legs.append(first)
        running_amount = first.output_amount
        second = _usd_to_stablecoin(target, running_amount, _stablecoin_book(books, target))
        legs.append(second)
        running_amount = second.output_amount

    effective_rate = running_amount / input_amount
    best_rate = 1.0
    for leg in legs:
        best_rate *= leg.best_rate
    total_slippage = max(0.0, (best_rate - effective_rate) / best_rate * 10_000.0)
    observed_at = min(book.observed_at for book in needed)
    return StablecoinConversionDepthQuote(
        source_currency=source,
        target_currency=target,
        input_amount=input_amount,
        output_amount=running_amount,
        effective_rate=effective_rate,
        total_slippage_bps=total_slippage,
        legs=legs,
        observed_at=observed_at,
        max_book_age_seconds=worst_age,
        book_skew_seconds=skew,
    )


__all__ = [
    "InsufficientDepthError",
    "StablecoinConversionDepthLeg",
    "StablecoinConversionDepthQuote",
    "quote_stablecoin_conversion_depth",
]
