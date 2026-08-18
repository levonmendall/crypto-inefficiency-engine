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
    min_net_annualized_return: float = 0.08
    safety_buffer_bps_per_hour: float = 0.02
    pair_roundtrip_cost_bps: float = 20.0
    max_quote_age_seconds: float = 120.0
    max_order_book_age_seconds: float = 15.0
    max_order_book_skew_seconds: float = 5.0
    exit_slippage_multiplier: float = 1.0
    capacity_search_tolerance_usd: float = 1.0
    capital_tiers_usd: tuple[float, ...] = (1000.0, 10000.0, 25000.0, 50000.0, 100000.0)
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
            min_net_annualized_return=_float("CIE_MIN_NET_ANNUALIZED_RETURN", 0.08),
            safety_buffer_bps_per_hour=_float("CIE_SAFETY_BUFFER_BPS_PER_HOUR", 0.02),
            pair_roundtrip_cost_bps=_float("CIE_PAIR_ROUNDTRIP_COST_BPS", 20.0),
            max_quote_age_seconds=_float("CIE_MAX_QUOTE_AGE_SECONDS", 120.0),
            max_order_book_age_seconds=_float("CIE_MAX_ORDER_BOOK_AGE_SECONDS", 15.0),
            max_order_book_skew_seconds=_float("CIE_MAX_ORDER_BOOK_SKEW_SECONDS", 5.0),
            exit_slippage_multiplier=_float("CIE_EXIT_SLIPPAGE_MULTIPLIER", 1.0),
            capacity_search_tolerance_usd=_float("CIE_CAPACITY_SEARCH_TOLERANCE_USD", 1.0),
            capital_tiers_usd=_float_tuple("CIE_CAPITAL_TIERS_USD", (1000.0, 10000.0, 25000.0, 50000.0, 100000.0)),
            evidence_db_path=os.getenv("CIE_EVIDENCE_DB_PATH") or None,
        )
