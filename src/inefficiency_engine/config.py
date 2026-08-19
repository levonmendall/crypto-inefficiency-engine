from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None else float(raw)


def _optional_float(name: str, default: float | None = None) -> float | None:
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None else int(raw)


def _float_tuple(name: str, default: tuple[float, ...]) -> tuple[float, ...]:
    raw = os.getenv(name)
    if raw is None:
        return default
    values = tuple(float(item.strip()) for item in raw.split(",") if item.strip())
    if not values or any(value <= 0 for value in values):
        raise ValueError(f"{name} must contain positive comma-separated numbers")
    return tuple(sorted(set(values)))


@dataclass(frozen=True)
class Settings:
    paper_only: bool = True
    scan_interval_seconds: float = 30.0
    default_holding_hours: float = 24.0
    spot_dislocation_holding_hours: float = 1.0
    min_net_annualized_return: float = 0.08
    safety_buffer_bps_per_hour: float = 0.02
    pair_roundtrip_cost_bps: float = 20.0
    max_quote_age_seconds: float = 120.0
    max_order_book_age_seconds: float = 15.0
    max_order_book_skew_seconds: float = 5.0
    exit_slippage_multiplier: float = 1.0
    capacity_search_tolerance_usd: float = 1.0
    coinbase_spot_taker_fee_bps: float = 60.0
    hyperliquid_perp_taker_fee_bps: float = 4.5
    bybit_spot_taker_fee_bps: float = 10.0
    bybit_derivatives_taker_fee_bps: float = 5.5
    kraken_spot_taker_fee_bps: float = 80.0
    okx_spot_taker_fee_bps: float = 10.0
    okx_derivatives_taker_fee_bps: float = 5.0
    spot_collateral_fraction: float = 1.0
    perp_collateral_fraction: float = 1.0
    collateral_opportunity_cost_annual: float = 0.04
    spot_short_borrow_annual: float | None = None
    expected_order_ack_latency_ms: float = 0.0
    expected_hedge_latency_ms: float = 750.0
    latency_risk_bps_per_second: float = 0.5
    empirical_latency_enabled: bool = True
    empirical_latency_min_samples: int = 100
    empirical_latency_min_scan_samples: int = 30
    empirical_latency_min_effective_samples: int = 30
    empirical_latency_quantile: float = 0.95
    empirical_probability_confidence_level: float = 0.95
    empirical_probability_max_ci_width: float = 0.25
    hedge_liquidity_reserve_ratio: float = 1.25
    hedge_recovery_buffer_bps: float = 2.0
    shadow_delay_seconds: float = 5.0
    shadow_horizons_seconds: tuple[float, ...] = (1.0, 5.0, 15.0, 30.0, 60.0)
    shadow_notional_usd: float = 1000.0
    shadow_max_candidates: int = 0
    shadow_slippage_expansion_bps: float = 1.0
    shadow_hedge_divergence_bps: float = 5.0
    shadow_cycle_interval_seconds: float = 30.0
    worker_error_backoff_seconds: float = 15.0
    worker_heartbeat_stale_seconds: float = 180.0
    capital_tiers_usd: tuple[float, ...] = (1000.0, 10000.0, 25000.0, 50000.0, 100000.0)
    stablecoin_depeg_risk_multiplier: float = 1.5
    stablecoin_conversion_risk_floor_bps: float = 2.0
    stablecoin_dislocation_min_edge_bps: float = 8.0
    dex_dislocation_min_edge_bps: float = 12.0
    dex_liquidity_risk_floor_bps: float = 8.0
    dex_route_evidence_notional_usd: float = 1000.0
    dex_route_frontier_notionals_usd: tuple[float, ...] = (1000.0, 5000.0, 10000.0, 25000.0)
    dex_route_frontier_max_deterioration_bps: float = 25.0
    dex_route_frontier_every_cycles: int = 10
    option_relative_value_min_iv_points: float = 8.0
    allocator_max_venue_fraction: float = 0.50
    allocator_max_asset_fraction: float = 0.50
    allocator_max_allocations: int = 10
    evidence_db_path: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        requested_paper_only = _bool("CIE_PAPER_ONLY", True)
        if not requested_paper_only:
            raise RuntimeError("V1 is paper-only; live execution cannot be enabled by configuration")
        return cls(
            paper_only=True,
            scan_interval_seconds=_float("CIE_SCAN_INTERVAL_SECONDS", 30.0),
            default_holding_hours=_float("CIE_DEFAULT_HOLDING_HOURS", 24.0),
            spot_dislocation_holding_hours=_float("CIE_SPOT_DISLOCATION_HOLDING_HOURS", 1.0),
            min_net_annualized_return=_float("CIE_MIN_NET_ANNUALIZED_RETURN", 0.08),
            safety_buffer_bps_per_hour=_float("CIE_SAFETY_BUFFER_BPS_PER_HOUR", 0.02),
            pair_roundtrip_cost_bps=_float("CIE_PAIR_ROUNDTRIP_COST_BPS", 20.0),
            max_quote_age_seconds=_float("CIE_MAX_QUOTE_AGE_SECONDS", 120.0),
            max_order_book_age_seconds=_float("CIE_MAX_ORDER_BOOK_AGE_SECONDS", 15.0),
            max_order_book_skew_seconds=_float("CIE_MAX_ORDER_BOOK_SKEW_SECONDS", 5.0),
            exit_slippage_multiplier=_float("CIE_EXIT_SLIPPAGE_MULTIPLIER", 1.0),
            capacity_search_tolerance_usd=_float("CIE_CAPACITY_SEARCH_TOLERANCE_USD", 1.0),
            coinbase_spot_taker_fee_bps=_float("CIE_COINBASE_SPOT_TAKER_FEE_BPS", 60.0),
            hyperliquid_perp_taker_fee_bps=_float("CIE_HYPERLIQUID_PERP_TAKER_FEE_BPS", 4.5),
            bybit_spot_taker_fee_bps=_float("CIE_BYBIT_SPOT_TAKER_FEE_BPS", 10.0),
            bybit_derivatives_taker_fee_bps=_float("CIE_BYBIT_DERIVATIVES_TAKER_FEE_BPS", 5.5),
            kraken_spot_taker_fee_bps=_float("CIE_KRAKEN_SPOT_TAKER_FEE_BPS", 80.0),
            okx_spot_taker_fee_bps=_float("CIE_OKX_SPOT_TAKER_FEE_BPS", 10.0),
            okx_derivatives_taker_fee_bps=_float("CIE_OKX_DERIVATIVES_TAKER_FEE_BPS", 5.0),
            spot_collateral_fraction=_float("CIE_SPOT_COLLATERAL_FRACTION", 1.0),
            perp_collateral_fraction=_float("CIE_PERP_COLLATERAL_FRACTION", 1.0),
            collateral_opportunity_cost_annual=_float("CIE_COLLATERAL_OPPORTUNITY_COST_ANNUAL", 0.04),
            spot_short_borrow_annual=_optional_float("CIE_SPOT_SHORT_BORROW_ANNUAL", None),
            expected_order_ack_latency_ms=_float("CIE_EXPECTED_ORDER_ACK_LATENCY_MS", 0.0),
            expected_hedge_latency_ms=_float("CIE_EXPECTED_HEDGE_LATENCY_MS", 750.0),
            latency_risk_bps_per_second=_float("CIE_LATENCY_RISK_BPS_PER_SECOND", 0.5),
            empirical_latency_enabled=_bool("CIE_EMPIRICAL_LATENCY_ENABLED", True),
            empirical_latency_min_samples=_int("CIE_EMPIRICAL_LATENCY_MIN_SAMPLES", 100),
            empirical_latency_min_scan_samples=_int("CIE_EMPIRICAL_LATENCY_MIN_SCAN_SAMPLES", 30),
            empirical_latency_min_effective_samples=_int("CIE_EMPIRICAL_LATENCY_MIN_EFFECTIVE_SAMPLES", 30),
            empirical_latency_quantile=_float("CIE_EMPIRICAL_LATENCY_QUANTILE", 0.95),
            empirical_probability_confidence_level=_float("CIE_EMPIRICAL_PROBABILITY_CONFIDENCE_LEVEL", 0.95),
            empirical_probability_max_ci_width=_float("CIE_EMPIRICAL_PROBABILITY_MAX_CI_WIDTH", 0.25),
            hedge_liquidity_reserve_ratio=_float("CIE_HEDGE_LIQUIDITY_RESERVE_RATIO", 1.25),
            hedge_recovery_buffer_bps=_float("CIE_HEDGE_RECOVERY_BUFFER_BPS", 2.0),
            shadow_delay_seconds=_float("CIE_SHADOW_DELAY_SECONDS", 5.0),
            shadow_horizons_seconds=_float_tuple("CIE_SHADOW_HORIZONS_SECONDS", (1.0, 5.0, 15.0, 30.0, 60.0)),
            shadow_notional_usd=_float("CIE_SHADOW_NOTIONAL_USD", 1000.0),
            shadow_max_candidates=_int("CIE_SHADOW_MAX_CANDIDATES", 0),
            shadow_slippage_expansion_bps=_float("CIE_SHADOW_SLIPPAGE_EXPANSION_BPS", 1.0),
            shadow_hedge_divergence_bps=_float("CIE_SHADOW_HEDGE_DIVERGENCE_BPS", 5.0),
            shadow_cycle_interval_seconds=_float("CIE_SHADOW_CYCLE_INTERVAL_SECONDS", 30.0),
            worker_error_backoff_seconds=_float("CIE_WORKER_ERROR_BACKOFF_SECONDS", 15.0),
            worker_heartbeat_stale_seconds=_float("CIE_WORKER_HEARTBEAT_STALE_SECONDS", 180.0),
            capital_tiers_usd=_float_tuple("CIE_CAPITAL_TIERS_USD", (1000.0, 10000.0, 25000.0, 50000.0, 100000.0)),
            stablecoin_depeg_risk_multiplier=_float("CIE_STABLECOIN_DEPEG_RISK_MULTIPLIER", 1.5),
            stablecoin_conversion_risk_floor_bps=_float("CIE_STABLECOIN_CONVERSION_RISK_FLOOR_BPS", 2.0),
            stablecoin_dislocation_min_edge_bps=_float("CIE_STABLECOIN_DISLOCATION_MIN_EDGE_BPS", 8.0),
            dex_dislocation_min_edge_bps=_float("CIE_DEX_DISLOCATION_MIN_EDGE_BPS", 12.0),
            dex_liquidity_risk_floor_bps=_float("CIE_DEX_LIQUIDITY_RISK_FLOOR_BPS", 8.0),
            dex_route_evidence_notional_usd=_float("CIE_DEX_ROUTE_EVIDENCE_NOTIONAL_USD", 1000.0),
            dex_route_frontier_notionals_usd=_float_tuple(
                "CIE_DEX_ROUTE_FRONTIER_NOTIONALS_USD", (1000.0, 5000.0, 10000.0, 25000.0)
            ),
            dex_route_frontier_max_deterioration_bps=_float(
                "CIE_DEX_ROUTE_FRONTIER_MAX_DETERIORATION_BPS", 25.0
            ),
            dex_route_frontier_every_cycles=max(1, _int("CIE_DEX_ROUTE_FRONTIER_EVERY_CYCLES", 10)),
            option_relative_value_min_iv_points=_float("CIE_OPTION_RELATIVE_VALUE_MIN_IV_POINTS", 8.0),
            allocator_max_venue_fraction=_float("CIE_ALLOCATOR_MAX_VENUE_FRACTION", 0.50),
            allocator_max_asset_fraction=_float("CIE_ALLOCATOR_MAX_ASSET_FRACTION", 0.50),
            allocator_max_allocations=_int("CIE_ALLOCATOR_MAX_ALLOCATIONS", 10),
            evidence_db_path=os.getenv("CIE_EVIDENCE_DB_PATH") or None,
        )
