from __future__ import annotations

from dataclasses import dataclass

from inefficiency_engine.models import OrderBookSnapshot, TradeSide


class InsufficientDepthError(RuntimeError):
    """Raised when a requested paper trade cannot be fully filled from observed depth."""


@dataclass(frozen=True)
class ExecutionEstimate:
    side: TradeSide
    requested_notional_usd: float
    filled_notional_usd: float
    base_quantity: float
    average_price: float
    best_price: float
    slippage_bps: float
    levels_consumed: int


def max_executable_notional(snapshot: OrderBookSnapshot, side: TradeSide) -> float:
    levels = snapshot.asks if side == TradeSide.BUY else snapshot.bids
    return sum(level.price * level.size for level in levels)


def estimate_market_order(
    snapshot: OrderBookSnapshot,
    side: TradeSide,
    notional_usd: float,
    *,
    require_full_fill: bool = True,
) -> ExecutionEstimate:
    """Estimate a taker fill against a point-in-time L2 snapshot.

    The estimate deliberately makes no latency or queue-position claim. It only
    answers whether the requested USD notional was visible in the snapshot and
    what VWAP/slippage would result if that visible depth were immediately
    consumable. Later shadow-fill logic can haircut this further.
    """
    if notional_usd <= 0:
        raise ValueError("notional_usd must be positive")

    levels = snapshot.asks if side == TradeSide.BUY else snapshot.bids
    if not levels:
        raise InsufficientDepthError("order book has no executable levels")

    ordered = sorted(levels, key=lambda x: x.price, reverse=side == TradeSide.SELL)
    best_price = ordered[0].price
    remaining = notional_usd
    filled_notional = 0.0
    base_quantity = 0.0
    levels_consumed = 0

    for level in ordered:
        if remaining <= 1e-9:
            break
        level_notional = level.price * level.size
        take_notional = min(remaining, level_notional)
        take_quantity = take_notional / level.price
        filled_notional += take_notional
        base_quantity += take_quantity
        remaining -= take_notional
        levels_consumed += 1

    if require_full_fill and remaining > max(1e-6, notional_usd * 1e-9):
        raise InsufficientDepthError(
            f"visible depth filled ${filled_notional:.2f} of requested ${notional_usd:.2f}"
        )
    if base_quantity <= 0:
        raise InsufficientDepthError("no quantity could be filled")

    average_price = filled_notional / base_quantity
    if side == TradeSide.BUY:
        slippage_bps = ((average_price / best_price) - 1.0) * 10_000
    else:
        slippage_bps = (1.0 - (average_price / best_price)) * 10_000

    return ExecutionEstimate(
        side=side,
        requested_notional_usd=notional_usd,
        filled_notional_usd=filled_notional,
        base_quantity=base_quantity,
        average_price=average_price,
        best_price=best_price,
        slippage_bps=max(0.0, slippage_bps),
        levels_consumed=levels_consumed,
    )
