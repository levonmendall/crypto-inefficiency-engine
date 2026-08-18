from __future__ import annotations

from dataclasses import dataclass

from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind, Opportunity, OpportunityLeg, Side


class UnknownVenueFeeError(RuntimeError):
    """Raised when an execution leg has no explicit fee model."""


class BorrowCostUnavailableError(RuntimeError):
    """Raised when a short spot leg would require an unconfigured borrow cost."""


@dataclass(frozen=True)
class EconomicCostBreakdown:
    screening_cost_floor_bps: float
    venue_roundtrip_fee_bps: float
    transaction_cost_bps: float
    financing_cost_bps: float
    collateral_opportunity_cost_bps: float
    latency_risk_bps: float
    hedge_recovery_buffer_bps: float
    total_non_slippage_cost_bps: float
    capital_required_usd: float
    capital_multiple: float


def taker_fee_bps(leg: OpportunityLeg, settings: Settings) -> float:
    """Return the conservative taker fee assumption for one fill.

    V1 only has executable L2 support for Coinbase spot and Hyperliquid perps,
    so unknown venues fail closed rather than inheriting a generic fee guess.
    """
    if leg.venue == "Coinbase" and leg.market_kind == MarketKind.SPOT:
        return settings.coinbase_spot_taker_fee_bps
    if leg.venue == "HlPerp" and leg.market_kind == MarketKind.PERPETUAL:
        return settings.hyperliquid_perp_taker_fee_bps
    raise UnknownVenueFeeError(f"no explicit taker fee model for {leg.venue}:{leg.market_kind.value}")


def collateral_fraction(leg: OpportunityLeg, settings: Settings) -> float:
    if leg.market_kind == MarketKind.SPOT:
        return settings.spot_collateral_fraction
    if leg.market_kind in {MarketKind.PERPETUAL, MarketKind.FUTURE}:
        return settings.perp_collateral_fraction
    raise ValueError(f"unsupported market kind for collateral: {leg.market_kind.value}")


def _financing_cost_bps(leg: OpportunityLeg, settings: Settings, holding_hours: float) -> float:
    if leg.market_kind == MarketKind.SPOT and leg.side == Side.SHORT:
        if settings.spot_short_borrow_annual is None:
            raise BorrowCostUnavailableError(f"spot short borrow cost unavailable for {leg.venue}:{leg.asset}")
        return settings.spot_short_borrow_annual * (holding_hours / (24.0 * 365.0)) * 10_000.0
    return 0.0


def economic_costs(opportunity: Opportunity, notional_usd_per_leg: float, settings: Settings, *, worst_book_age_seconds: float) -> EconomicCostBreakdown:
    if notional_usd_per_leg <= 0:
        raise ValueError("notional_usd_per_leg must be positive")
    roundtrip_fee_bps = 0.0
    financing_bps = 0.0
    capital_required = 0.0
    for leg in opportunity.legs:
        roundtrip_fee_bps += taker_fee_bps(leg, settings) * 2.0
        financing_bps += _financing_cost_bps(leg, settings, opportunity.holding_hours)
        capital_required += notional_usd_per_leg * collateral_fraction(leg, settings)
    capital_multiple = capital_required / notional_usd_per_leg
    collateral_cost_bps = settings.collateral_opportunity_cost_annual * (opportunity.holding_hours / (24.0 * 365.0)) * 10_000.0 * capital_multiple
    latency_seconds = max(0.0, worst_book_age_seconds) + max(0.0, settings.expected_hedge_latency_ms) / 1000.0
    latency_risk_bps = latency_seconds * settings.latency_risk_bps_per_second
    transaction_cost_bps = max(opportunity.modeled_cost_bps, roundtrip_fee_bps)
    total = transaction_cost_bps + financing_bps + collateral_cost_bps + latency_risk_bps + settings.hedge_recovery_buffer_bps
    return EconomicCostBreakdown(screening_cost_floor_bps=opportunity.modeled_cost_bps, venue_roundtrip_fee_bps=roundtrip_fee_bps, transaction_cost_bps=transaction_cost_bps, financing_cost_bps=financing_bps, collateral_opportunity_cost_bps=collateral_cost_bps, latency_risk_bps=latency_risk_bps, hedge_recovery_buffer_bps=settings.hedge_recovery_buffer_bps, total_non_slippage_cost_bps=total, capital_required_usd=capital_required, capital_multiple=capital_multiple)
