from __future__ import annotations

from dataclasses import dataclass, field

from inefficiency_engine.config import Settings
from inefficiency_engine.models import EmpiricalLatencyModel, MarketKind, Opportunity, OpportunityLeg, Side


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
    latency_model_source: str
    latency_model_scope: str
    latency_scope_fallbacks: list[str] = field(default_factory=list)
    latency_reference_ms: float | None = None
    collector_latency_reference_ms: float | None = None
    execution_latency_empirical: bool = False
    latency_reference_horizon_seconds: float | None = None
    latency_reference_lower_horizon_seconds: float | None = None
    latency_interpolation_weight: float = 0.0
    latency_interpolation_mode: str = "fixed"
    latency_sample_count: int = 0
    latency_effective_sample_size: int = 0
    latency_confidence_gate_passed: bool = False
    empirical_pair_fill_probability: float | None = None
    empirical_pair_fill_ci_lower: float | None = None
    empirical_reserve_fill_probability: float | None = None
    empirical_capture_probability: float | None = None
    empirical_capture_ci_lower: float | None = None
    empirical_hedge_recovery_probability: float | None = None
    empirical_partial_fill_probability: float | None = None
    empirical_unhedged_fraction_p95: float | None = None
    empirical_hedge_recovery_loss_p95_bps: float | None = None
    fill_model_kind: str = "visible_l2_taker_reconstruction"
    queue_position_supported: bool = False
    hedge_recovery_buffer_bps: float = 0.0
    hedge_recovery_buffer_source: str = "fixed"
    total_non_slippage_cost_bps: float = 0.0
    capital_required_usd: float = 0.0
    capital_multiple: float = 0.0


def taker_fee_bps(leg: OpportunityLeg, settings: Settings) -> float:
    if leg.venue == "Coinbase" and leg.market_kind == MarketKind.SPOT:
        return settings.coinbase_spot_taker_fee_bps
    if leg.venue == "HlPerp" and leg.market_kind == MarketKind.PERPETUAL:
        return settings.hyperliquid_perp_taker_fee_bps
    if leg.venue == "Bybit" and leg.market_kind == MarketKind.SPOT:
        return settings.bybit_spot_taker_fee_bps
    if leg.venue == "Bybit" and leg.market_kind in {MarketKind.PERPETUAL, MarketKind.FUTURE}:
        return settings.bybit_derivatives_taker_fee_bps
    if leg.venue == "Kraken" and leg.market_kind == MarketKind.SPOT:
        return settings.kraken_spot_taker_fee_bps
    if leg.venue == "OKX" and leg.market_kind == MarketKind.SPOT:
        return settings.okx_spot_taker_fee_bps
    if leg.venue == "OKX" and leg.market_kind in {MarketKind.PERPETUAL, MarketKind.FUTURE}:
        return settings.okx_derivatives_taker_fee_bps
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


def economic_costs(
    opportunity: Opportunity,
    notional_usd_per_leg: float,
    settings: Settings,
    *,
    worst_book_age_seconds: float,
    latency_model: EmpiricalLatencyModel | None = None,
) -> EconomicCostBreakdown:
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
    collateral_cost_bps = (
        settings.collateral_opportunity_cost_annual
        * (opportunity.holding_hours / (24.0 * 365.0))
        * 10_000.0
        * capital_multiple
    )

    book_age_risk_bps = max(0.0, worst_book_age_seconds) * settings.latency_risk_bps_per_second
    fixed_execution_latency_ms = (
        max(0.0, settings.expected_order_ack_latency_ms)
        + max(0.0, settings.expected_hedge_latency_ms)
    )
    fixed_hedge_latency_risk_bps = fixed_execution_latency_ms / 1000.0 * settings.latency_risk_bps_per_second

    latency_model_source = "fixed"
    latency_model_scope = "fixed"
    latency_scope_fallbacks: list[str] = []
    latency_reference_ms: float | None = fixed_execution_latency_ms
    collector_latency_reference_ms: float | None = None
    execution_latency_empirical = False
    latency_reference_horizon_seconds = None
    latency_reference_lower_horizon_seconds = None
    latency_interpolation_weight = 0.0
    latency_interpolation_mode = "fixed"
    latency_sample_count = 0
    latency_effective_sample_size = 0
    latency_confidence_gate_passed = False
    pair_fill_probability = None
    pair_fill_ci_lower = None
    reserve_fill_probability = None
    capture_probability = None
    capture_ci_lower = None
    hedge_recovery_probability = None
    partial_fill_probability = None
    unhedged_fraction_p95 = None
    recovery_loss_p95 = None
    fill_model_kind = "visible_l2_taker_reconstruction"
    queue_position_supported = False
    hedge_latency_risk_bps = fixed_hedge_latency_risk_bps
    recovery_buffer_bps = max(0.0, settings.hedge_recovery_buffer_bps)
    recovery_buffer_source = "fixed"

    if latency_model is not None and latency_model.usable_for_qualification:
        latency_model_source = "empirical_shadow"
        latency_model_scope = latency_model.model_scope
        latency_scope_fallbacks = list(latency_model.scope_fallbacks)
        latency_reference_ms = latency_model.effective_decision_to_hedge_latency_ms or latency_model.reference_latency_ms
        collector_latency_reference_ms = latency_model.collector_latency_reference_ms
        execution_latency_empirical = latency_model.execution_latency_empirical
        latency_reference_horizon_seconds = latency_model.reference_upper_horizon_seconds
        latency_reference_lower_horizon_seconds = latency_model.reference_lower_horizon_seconds
        latency_interpolation_weight = latency_model.interpolation_weight
        latency_interpolation_mode = latency_model.interpolation_mode
        latency_sample_count = latency_model.cohort_sample_count
        latency_effective_sample_size = latency_model.effective_sample_size
        latency_confidence_gate_passed = latency_model.confidence_gate_passed
        pair_fill_probability = latency_model.pair_fill_probability
        pair_fill_ci_lower = latency_model.pair_fill_ci_lower
        reserve_fill_probability = latency_model.reserve_fill_probability
        capture_probability = latency_model.capture_probability
        capture_ci_lower = latency_model.capture_ci_lower
        hedge_recovery_probability = latency_model.hedge_recovery_probability
        partial_fill_probability = latency_model.partial_fill_probability
        unhedged_fraction_p95 = latency_model.unhedged_fraction_p95
        recovery_loss_p95 = latency_model.hedge_recovery_loss_p95_bps
        fill_model_kind = latency_model.fill_model_kind
        queue_position_supported = latency_model.queue_position_supported
        hedge_latency_risk_bps = max(0.0, latency_model.empirical_latency_risk_bps or 0.0)
        if recovery_loss_p95 is not None:
            recovery_buffer_bps = max(recovery_buffer_bps, max(0.0, recovery_loss_p95))
            recovery_buffer_source = "max_fixed_empirical"

    latency_risk_bps = book_age_risk_bps + hedge_latency_risk_bps
    transaction_cost_bps = max(opportunity.modeled_cost_bps, roundtrip_fee_bps)
    total = (
        transaction_cost_bps
        + financing_bps
        + collateral_cost_bps
        + latency_risk_bps
        + recovery_buffer_bps
    )
    return EconomicCostBreakdown(
        screening_cost_floor_bps=opportunity.modeled_cost_bps,
        venue_roundtrip_fee_bps=roundtrip_fee_bps,
        transaction_cost_bps=transaction_cost_bps,
        financing_cost_bps=financing_bps,
        collateral_opportunity_cost_bps=collateral_cost_bps,
        latency_risk_bps=latency_risk_bps,
        latency_model_source=latency_model_source,
        latency_model_scope=latency_model_scope,
        latency_scope_fallbacks=latency_scope_fallbacks,
        latency_reference_ms=latency_reference_ms,
        collector_latency_reference_ms=collector_latency_reference_ms,
        execution_latency_empirical=execution_latency_empirical,
        latency_reference_horizon_seconds=latency_reference_horizon_seconds,
        latency_reference_lower_horizon_seconds=latency_reference_lower_horizon_seconds,
        latency_interpolation_weight=latency_interpolation_weight,
        latency_interpolation_mode=latency_interpolation_mode,
        latency_sample_count=latency_sample_count,
        latency_effective_sample_size=latency_effective_sample_size,
        latency_confidence_gate_passed=latency_confidence_gate_passed,
        empirical_pair_fill_probability=pair_fill_probability,
        empirical_pair_fill_ci_lower=pair_fill_ci_lower,
        empirical_reserve_fill_probability=reserve_fill_probability,
        empirical_capture_probability=capture_probability,
        empirical_capture_ci_lower=capture_ci_lower,
        empirical_hedge_recovery_probability=hedge_recovery_probability,
        empirical_partial_fill_probability=partial_fill_probability,
        empirical_unhedged_fraction_p95=unhedged_fraction_p95,
        empirical_hedge_recovery_loss_p95_bps=recovery_loss_p95,
        fill_model_kind=fill_model_kind,
        queue_position_supported=queue_position_supported,
        hedge_recovery_buffer_bps=recovery_buffer_bps,
        hedge_recovery_buffer_source=recovery_buffer_source,
        total_non_slippage_cost_bps=total,
        capital_required_usd=capital_required,
        capital_multiple=capital_multiple,
    )
