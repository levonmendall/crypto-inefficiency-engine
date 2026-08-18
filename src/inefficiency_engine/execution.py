from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.costs import BorrowCostUnavailableError, UnknownVenueFeeError, economic_costs
from inefficiency_engine.models import (
    CapitalTierQualification,
    LegExecutionEstimate,
    Opportunity,
    OpportunityExecutability,
    OrderBookSnapshot,
    Side,
    TradeSide,
)


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


@dataclass(frozen=True)
class QuantityExecutionEstimate:
    side: TradeSide
    requested_base_quantity: float
    filled_base_quantity: float
    filled_notional_usd: float
    average_price: float
    best_price: float
    slippage_bps: float
    levels_consumed: int


def max_executable_notional(snapshot: OrderBookSnapshot, side: TradeSide) -> float:
    levels = snapshot.asks if side == TradeSide.BUY else snapshot.bids
    return sum(level.price * level.size for level in levels)


def max_executable_quantity(snapshot: OrderBookSnapshot, side: TradeSide) -> float:
    levels = snapshot.asks if side == TradeSide.BUY else snapshot.bids
    return sum(level.size for level in levels)


def _ordered_levels(snapshot: OrderBookSnapshot, side: TradeSide):
    levels = snapshot.asks if side == TradeSide.BUY else snapshot.bids
    if not levels:
        raise InsufficientDepthError("order book has no executable levels")
    return sorted(levels, key=lambda x: x.price, reverse=side == TradeSide.SELL)


def estimate_market_order_quantity(
    snapshot: OrderBookSnapshot,
    side: TradeSide,
    base_quantity: float,
    *,
    require_full_fill: bool = True,
) -> QuantityExecutionEstimate:
    """Estimate a taker fill for an exact base quantity against observed L2 depth."""
    if base_quantity <= 0:
        raise ValueError("base_quantity must be positive")

    ordered = _ordered_levels(snapshot, side)
    best_price = ordered[0].price
    remaining = base_quantity
    filled_quantity = 0.0
    filled_notional = 0.0
    levels_consumed = 0

    for level in ordered:
        if remaining <= 1e-12:
            break
        take_quantity = min(remaining, level.size)
        filled_quantity += take_quantity
        filled_notional += take_quantity * level.price
        remaining -= take_quantity
        levels_consumed += 1

    if require_full_fill and remaining > max(1e-12, base_quantity * 1e-9):
        raise InsufficientDepthError(
            f"visible depth filled {filled_quantity:.12g} of requested {base_quantity:.12g} base units"
        )
    if filled_quantity <= 0:
        raise InsufficientDepthError("no quantity could be filled")

    average_price = filled_notional / filled_quantity
    if side == TradeSide.BUY:
        slippage_bps = ((average_price / best_price) - 1.0) * 10_000
    else:
        slippage_bps = (1.0 - (average_price / best_price)) * 10_000

    return QuantityExecutionEstimate(
        side=side,
        requested_base_quantity=base_quantity,
        filled_base_quantity=filled_quantity,
        filled_notional_usd=filled_notional,
        average_price=average_price,
        best_price=best_price,
        slippage_bps=max(0.0, slippage_bps),
        levels_consumed=levels_consumed,
    )


def estimate_market_order(
    snapshot: OrderBookSnapshot,
    side: TradeSide,
    notional_usd: float,
    *,
    require_full_fill: bool = True,
) -> ExecutionEstimate:
    """Estimate a taker fill against a point-in-time L2 snapshot by USD notional."""
    if notional_usd <= 0:
        raise ValueError("notional_usd must be positive")

    ordered = _ordered_levels(snapshot, side)
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


def _trade_side(side: Side) -> TradeSide:
    return TradeSide.BUY if side == Side.LONG else TradeSide.SELL


def _book_key(snapshot: OrderBookSnapshot) -> tuple[str, str, str]:
    return (snapshot.venue, snapshot.asset, snapshot.market_kind.value)


def _leg_key(venue: str, asset: str, market_kind) -> tuple[str, str, str]:
    return (venue, asset, market_kind.value)


def _rejected_tier(opportunity: Opportunity, notional: float, reason: str) -> CapitalTierQualification:
    return CapitalTierQualification(
        opportunity_id=opportunity.id,
        notional_usd_per_leg=notional,
        executable=False,
        gross_edge_bps_per_hour=opportunity.gross_edge_bps_per_hour,
        static_modeled_cost_bps=opportunity.modeled_cost_bps,
        total_modeled_cost_bps=opportunity.modeled_cost_bps,
        net_edge_bps_per_hour=0.0,
        net_annualized_return=0.0,
        rejection_reason=reason,
    )


def qualify_opportunity(
    opportunity: Opportunity,
    books: list[OrderBookSnapshot],
    settings: Settings,
    *,
    notionals_usd: tuple[float, ...] | None = None,
    now: datetime | None = None,
) -> OpportunityExecutability:
    """Qualify a two-leg opportunity with explicit fees, capital, and hedge risk.

    Both legs must fill the same base quantity and retain an additional visible
    liquidity reserve. Entry and exit taker fees are venue-specific; financing,
    collateral opportunity cost, book age/hedge latency risk, and a hedge-recovery
    buffer are charged before the return hurdle is applied. Returns are measured
    on modeled capital required, not merely one leg's notional.
    """
    now = now or datetime.now(timezone.utc)
    notionals = notionals_usd or settings.capital_tiers_usd
    book_map = {_book_key(book): book for book in books}

    if len(opportunity.legs) != 2:
        reason = "opportunity must have exactly two legs"
        return OpportunityExecutability(
            opportunity_id=opportunity.id,
            strategy=opportunity.strategy,
            asset=opportunity.asset,
            observed_at=now,
            tiers=[_rejected_tier(opportunity, n, reason) for n in notionals],
        )

    leg_books: list[OrderBookSnapshot] = []
    missing: list[str] = []
    for leg in opportunity.legs:
        book = book_map.get(_leg_key(leg.venue, leg.asset, leg.market_kind))
        if book is None:
            missing.append(f"{leg.venue}:{leg.asset}:{leg.market_kind.value}")
        else:
            leg_books.append(book)

    if missing:
        reason = "missing order book(s): " + ", ".join(missing)
        return OpportunityExecutability(
            opportunity_id=opportunity.id,
            strategy=opportunity.strategy,
            asset=opportunity.asset,
            observed_at=now,
            tiers=[_rejected_tier(opportunity, n, reason) for n in notionals],
        )

    age_limit = settings.max_order_book_age_seconds
    book_ages = [max(0.0, (now - book.observed_at).total_seconds()) for book in leg_books]
    stale = [book for book, age in zip(leg_books, book_ages) if age > age_limit]
    skew = abs((leg_books[0].observed_at - leg_books[1].observed_at).total_seconds())
    if stale or skew > settings.max_order_book_skew_seconds:
        reason = "stale order book" if stale else f"order-book time skew {skew:.3f}s exceeds limit"
        return OpportunityExecutability(
            opportunity_id=opportunity.id,
            strategy=opportunity.strategy,
            asset=opportunity.asset,
            observed_at=now,
            tiers=[_rejected_tier(opportunity, n, reason) for n in notionals],
        )

    best_prices: list[float] = []
    side_pairs: list[tuple[TradeSide, OrderBookSnapshot]] = []
    for leg, book in zip(opportunity.legs, leg_books):
        side = _trade_side(leg.side)
        levels = book.asks if side == TradeSide.BUY else book.bids
        best_prices.append((min if side == TradeSide.BUY else max)(level.price for level in levels))
        side_pairs.append((side, book))
    conservative_reference = max(best_prices)
    reserve_ratio = max(1.0, settings.hedge_liquidity_reserve_ratio)
    max_shared_quantity = min(max_executable_quantity(book, side) for side, book in side_pairs)
    visible_depth_ceiling = conservative_reference * max_shared_quantity / reserve_ratio
    worst_book_age = max(book_ages, default=0.0)

    def evaluate(notional: float) -> CapitalTierQualification:
        target_quantity = notional / conservative_reference
        reserve_quantity = target_quantity * reserve_ratio
        for side, book in side_pairs:
            if max_executable_quantity(book, side) + 1e-12 < reserve_quantity:
                tier = _rejected_tier(
                    opportunity,
                    notional,
                    f"visible depth does not preserve {reserve_ratio:.2f}x hedge liquidity reserve",
                )
                tier.target_base_quantity = target_quantity
                return tier

        try:
            costs = economic_costs(opportunity, notional, settings, worst_book_age_seconds=worst_book_age)
        except (UnknownVenueFeeError, BorrowCostUnavailableError) as exc:
            tier = _rejected_tier(opportunity, notional, str(exc))
            tier.target_base_quantity = target_quantity
            return tier

        leg_estimates: list[LegExecutionEstimate] = []
        try:
            for leg, book in zip(opportunity.legs, leg_books):
                trade_side = _trade_side(leg.side)
                estimate = estimate_market_order_quantity(book, trade_side, target_quantity)
                leg_estimates.append(
                    LegExecutionEstimate(
                        venue=leg.venue,
                        asset=leg.asset,
                        market_kind=leg.market_kind,
                        trade_side=trade_side,
                        requested_base_quantity=target_quantity,
                        filled_base_quantity=estimate.filled_base_quantity,
                        filled_notional_usd=estimate.filled_notional_usd,
                        average_price=estimate.average_price,
                        best_price=estimate.best_price,
                        slippage_bps=estimate.slippage_bps,
                        levels_consumed=estimate.levels_consumed,
                    )
                )
        except InsufficientDepthError as exc:
            tier = _rejected_tier(opportunity, notional, str(exc))
            tier.target_base_quantity = target_quantity
            tier.leg_estimates = leg_estimates
            return tier

        entry_slippage = sum(item.slippage_bps for item in leg_estimates)
        assumed_exit_slippage = entry_slippage * settings.exit_slippage_multiplier
        total_cost_bps = costs.total_non_slippage_cost_bps + entry_slippage + assumed_exit_slippage
        total_gross_bps = opportunity.gross_edge_bps_per_hour * opportunity.holding_hours
        total_safety_bps = opportunity.safety_buffer_bps_per_hour * opportunity.holding_hours
        net_total_bps = total_gross_bps - total_cost_bps - total_safety_bps
        net_hourly_bps_on_leg_notional = net_total_bps / opportunity.holding_hours
        leg_notional_annualized = (net_hourly_bps_on_leg_notional / 10_000.0) * 24 * 365
        capital_adjusted_annualized = leg_notional_annualized / costs.capital_multiple if costs.capital_multiple > 0 else float("-inf")
        passes = net_hourly_bps_on_leg_notional > 0 and capital_adjusted_annualized >= settings.min_net_annualized_return

        return CapitalTierQualification(
            opportunity_id=opportunity.id,
            notional_usd_per_leg=notional,
            target_base_quantity=target_quantity,
            executable=True,
            passes_return_hurdle=passes,
            gross_edge_bps_per_hour=opportunity.gross_edge_bps_per_hour,
            static_modeled_cost_bps=opportunity.modeled_cost_bps,
            venue_roundtrip_fee_bps=costs.venue_roundtrip_fee_bps,
            financing_cost_bps=costs.financing_cost_bps,
            collateral_opportunity_cost_bps=costs.collateral_opportunity_cost_bps,
            latency_risk_bps=costs.latency_risk_bps,
            hedge_recovery_buffer_bps=costs.hedge_recovery_buffer_bps,
            capital_required_usd=costs.capital_required_usd,
            capital_multiple=costs.capital_multiple,
            observed_entry_slippage_bps=entry_slippage,
            assumed_exit_slippage_bps=assumed_exit_slippage,
            total_modeled_cost_bps=total_cost_bps,
            net_edge_bps_per_hour=net_hourly_bps_on_leg_notional,
            net_annualized_return=capital_adjusted_annualized,
            leg_notional_net_annualized_return=leg_notional_annualized,
            leg_estimates=leg_estimates,
            rejection_reason=None if passes else "capital-adjusted net return falls below hurdle after fees, risk, and slippage",
        )

    tiers = [evaluate(notional) for notional in notionals]
    max_tested_qualified = max(
        (tier.notional_usd_per_leg for tier in tiers if tier.executable and tier.passes_return_hurdle),
        default=0.0,
    )

    tolerance = max(0.01, settings.capacity_search_tolerance_usd)
    capacity = 0.0
    capacity_return: float | None = None
    if visible_depth_ceiling > 0:
        upper = visible_depth_ceiling
        upper_eval = evaluate(upper)
        if upper_eval.executable and upper_eval.passes_return_hurdle:
            capacity = upper
            capacity_return = upper_eval.net_annualized_return
        else:
            probe = min(tolerance, upper)
            probe_eval = evaluate(probe)
            if probe_eval.executable and probe_eval.passes_return_hurdle:
                low = probe
                high = upper
                best = probe_eval
                for _ in range(64):
                    if high - low <= tolerance:
                        break
                    mid = (low + high) / 2.0
                    candidate = evaluate(mid)
                    if candidate.executable and candidate.passes_return_hurdle:
                        low = mid
                        best = candidate
                    else:
                        high = mid
                capacity = low
                capacity_return = best.net_annualized_return

    return OpportunityExecutability(
        opportunity_id=opportunity.id,
        strategy=opportunity.strategy,
        asset=opportunity.asset,
        observed_at=now,
        tiers=tiers,
        max_qualified_notional_usd=max_tested_qualified,
        visible_depth_ceiling_usd=visible_depth_ceiling,
        estimated_capacity_notional_usd=capacity,
        capacity_frontier_net_annualized_return=capacity_return,
    )
