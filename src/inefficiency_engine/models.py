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


class LegExecutionEstimate(BaseModel):
    venue: str
    asset: str
    market_kind: MarketKind
    trade_side: TradeSide
    requested_base_quantity: float = Field(gt=0)
    filled_base_quantity: float = Field(gt=0)
    filled_notional_usd: float = Field(gt=0)
    average_price: float = Field(gt=0)
    best_price: float = Field(gt=0)
    slippage_bps: float = Field(ge=0)
    levels_consumed: int = Field(gt=0)


class ShadowOutcome(str, Enum):
    SURVIVED = "survived"
    SIGNAL_DISAPPEARED = "signal_disappeared"
    EXECUTABILITY_FAILED = "executability_failed"


class ShadowFailureCause(str, Enum):
    SIGNAL_DISAPPEARED = "signal_disappeared"
    INSUFFICIENT_DEPTH = "insufficient_depth"
    SLIPPAGE_EXPANSION = "slippage_expansion"
    FEE_COST_HURDLE_FAILURE = "fee_cost_hurdle_failure"
    STALE_DATA_PROVIDER_FAILURE = "stale_data_provider_failure"
    HEDGE_LEG_DIVERGENCE = "hedge_leg_divergence"


class EmpiricalLatencyModel(BaseModel):
    model_version: str = "v0.8.1"
    model_scope: str = "global"
    scope_strategy: Strategy | None = None
    scope_venue_pair: str | None = None
    scope_asset: str | None = None
    scope_notional_usd_per_leg: float | None = Field(default=None, gt=0)
    scope_candidate_counts: dict[str, int] = Field(default_factory=dict)
    scope_fallbacks: list[str] = Field(default_factory=list)
    latency_quantile: float = 0.95
    scan_latency_sample_count: int = 0
    cohort_sample_count: int = 0
    reference_latency_ms: float | None = None
    reference_horizon_seconds: float | None = None
    scan_latency_p50_ms: float | None = None
    scan_latency_p90_ms: float | None = None
    scan_latency_p95_ms: float | None = None
    pair_fill_probability: float | None = None
    reserve_fill_probability: float | None = None
    capture_probability: float | None = None
    hedge_recovery_probability: float | None = None
    adverse_selection_p50_bps: float | None = None
    adverse_selection_p90_bps: float | None = None
    adverse_selection_p95_bps: float | None = None
    empirical_latency_risk_bps: float | None = None
    usable_for_qualification: bool = False
    reason: str | None = None
    paper_only: bool = True


class CapitalTierQualification(BaseModel):
    opportunity_id: str
    notional_usd_per_leg: float = Field(gt=0)
    target_base_quantity: float | None = Field(default=None, gt=0)
    executable: bool
    passes_return_hurdle: bool = False
    gross_edge_bps_per_hour: float
    static_modeled_cost_bps: float
    venue_roundtrip_fee_bps: float = 0.0
    financing_cost_bps: float = 0.0
    collateral_opportunity_cost_bps: float = 0.0
    latency_risk_bps: float = 0.0
    latency_model_source: Literal["fixed", "empirical_shadow"] = "fixed"
    latency_model_scope: str = "fixed"
    latency_scope_fallbacks: list[str] = Field(default_factory=list)
    latency_reference_ms: float | None = None
    latency_sample_count: int = 0
    empirical_pair_fill_probability: float | None = None
    empirical_reserve_fill_probability: float | None = None
    empirical_capture_probability: float | None = None
    empirical_hedge_recovery_probability: float | None = None
    hedge_recovery_buffer_bps: float = 0.0
    capital_required_usd: float = 0.0
    capital_multiple: float = 0.0
    observed_entry_slippage_bps: float = 0.0
    assumed_exit_slippage_bps: float = 0.0
    total_modeled_cost_bps: float
    net_edge_bps_per_hour: float
    net_annualized_return: float
    leg_notional_net_annualized_return: float = 0.0
    leg_estimates: list[LegExecutionEstimate] = Field(default_factory=list)
    rejection_reason: str | None = None


class OpportunityExecutability(BaseModel):
    opportunity_id: str
    strategy: Strategy
    asset: str
    observed_at: datetime
    tiers: list[CapitalTierQualification]
    max_qualified_notional_usd: float = 0.0
    visible_depth_ceiling_usd: float = 0.0
    estimated_capacity_notional_usd: float = 0.0
    capacity_frontier_net_annualized_return: float | None = None
    paper_only: bool = True


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


class ShadowLegAttribution(BaseModel):
    venue: str
    asset: str
    market_kind: MarketKind
    side: Side
    initial_best_price: float | None = None
    verification_best_price: float | None = None
    adverse_selection_bps: float | None = None
    initial_spread_bps: float | None = None
    verification_spread_bps: float | None = None
    initial_available_depth_usd: float | None = None
    verification_available_depth_usd: float | None = None
    initial_available_base_quantity: float | None = None
    verification_available_base_quantity: float | None = None
    initial_depth_multiple: float | None = None
    verification_depth_multiple: float | None = None
    initial_slippage_bps: float | None = None
    verification_slippage_bps: float | None = None


class ShadowObservation(BaseModel):
    shadow_id: str
    initial_scan_id: str
    verification_scan_id: str
    opportunity_signature: str
    opportunity_id: str
    strategy: Strategy
    asset: str
    notional_usd_per_leg: float = Field(gt=0)
    target_base_quantity: float | None = Field(default=None, gt=0)
    started_at: datetime
    verified_at: datetime
    delay_seconds: float = Field(ge=0)
    initial_scan_latency_ms: float | None = Field(default=None, ge=0)
    verification_scan_latency_ms: float | None = Field(default=None, ge=0)
    initial_net_annualized_return: float
    initial_capacity_notional_usd: float
    survived: bool
    pair_fillable: bool | None = None
    pair_fillable_with_reserve: bool | None = None
    hedge_recovery_required: bool | None = None
    verification_net_annualized_return: float | None = None
    outcome: ShadowOutcome
    reason: str | None = None
    venue_pair: str | None = None
    time_of_day_bucket: str | None = None
    initial_expected_return_bucket: str | None = None
    initial_gross_edge_bps_per_hour: float | None = None
    verification_gross_edge_bps_per_hour: float | None = None
    gross_edge_decay_bps_per_hour: float | None = None
    initial_total_modeled_cost_bps: float | None = None
    verification_total_modeled_cost_bps: float | None = None
    cost_expansion_bps: float | None = None
    initial_entry_slippage_bps: float | None = None
    verification_entry_slippage_bps: float | None = None
    slippage_expansion_bps: float | None = None
    verification_capacity_notional_usd: float | None = None
    capacity_deterioration_usd: float | None = None
    edge_decay_annualized: float | None = None
    hedge_leg_divergence_bps: float | None = None
    failure_cause: ShadowFailureCause | None = None
    leg_attribution: list[ShadowLegAttribution] = Field(default_factory=list)
    paper_only: bool = True


class ShadowCycle(BaseModel):
    cycle_id: str
    started_at: datetime
    completed_at: datetime
    delay_seconds: float = Field(ge=0)
    initial_scan_id: str
    verification_scan_id: str
    verification_scan_ids: list[str] = Field(default_factory=list)
    horizons_seconds: list[float] = Field(default_factory=list)
    observations: list[ShadowObservation] = Field(default_factory=list)
    paper_only: bool = True
