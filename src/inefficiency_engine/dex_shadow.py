from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from inefficiency_engine.dex_routes import DexRouteQuote


class DexRouteQuoteRecord(BaseModel):
    record_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cycle_id: str
    phase: Literal["initial", "verification"]
    horizon_seconds: float = Field(ge=0)
    route_signature: str
    observed_at: datetime
    quote: DexRouteQuote


class DexRouteShadowObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cycle_id: str
    route_signature: str
    asset: str
    direction: Literal["buy_asset", "sell_asset"]
    source_amount_raw: str
    quote_notional_usd_proxy: float = Field(gt=0)
    delay_seconds: float = Field(ge=0)
    started_at: datetime
    verified_at: datetime
    initial_record_id: str
    verification_record_id: str | None = None
    survived: bool
    initial_effective_asset_price: float = Field(gt=0)
    verification_effective_asset_price: float | None = Field(default=None, gt=0)
    price_deterioration_bps: float | None = None
    initial_gas_cost_usd: float | None = Field(default=None, ge=0)
    verification_gas_cost_usd: float | None = Field(default=None, ge=0)
    gas_cost_change_usd: float | None = None
    initial_request_latency_ms: float | None = Field(default=None, ge=0)
    verification_request_latency_ms: float | None = Field(default=None, ge=0)
    initial_block_number: int | None = Field(default=None, ge=0)
    verification_block_number: int | None = Field(default=None, ge=0)
    block_delta: int | None = None
    initial_route_exchanges: list[str] = Field(default_factory=list)
    verification_route_exchanges: list[str] = Field(default_factory=list)
    route_changed: bool | None = None
    failure_type: str | None = None
    capacity_claimed: bool = False
    transaction_built: bool = False
    paper_only: bool = True


class DexRouteShadowCycle(BaseModel):
    cycle_id: str
    started_at: datetime
    completed_at: datetime
    horizons_seconds: list[float] = Field(default_factory=list)
    initial_quote_count: int = 0
    observations: list[DexRouteShadowObservation] = Field(default_factory=list)
    paper_only: bool = True


def route_signature(quote: DexRouteQuote) -> str:
    return ":".join([
        quote.provider,
        str(quote.network_id),
        quote.asset.upper(),
        quote.direction,
        quote.source_token.lower(),
        quote.destination_token.lower(),
        quote.source_amount_raw,
    ])


def quote_notional_usd_proxy(quote: DexRouteQuote) -> float:
    return quote.source_amount if quote.direction == "buy_asset" else quote.destination_amount


def price_deterioration_bps(initial: DexRouteQuote, verification: DexRouteQuote) -> float:
    """Positive values are adverse to the original DEX trade direction."""
    if initial.direction != verification.direction:
        raise ValueError("route directions must match")
    base = initial.effective_asset_price
    if initial.direction == "sell_asset":
        return (base - verification.effective_asset_price) / base * 10_000.0
    return (verification.effective_asset_price - base) / base * 10_000.0


def build_shadow_observation(
    *,
    cycle_id: str,
    initial_record: DexRouteQuoteRecord,
    verification_record: DexRouteQuoteRecord | None,
    delay_seconds: float,
    verified_at: datetime,
    failure_type: str | None = None,
) -> DexRouteShadowObservation:
    initial = initial_record.quote
    verification = verification_record.quote if verification_record is not None else None
    survived = verification is not None
    route_changed = None
    block_delta = None
    deterioration = None
    gas_change = None
    if verification is not None:
        deterioration = price_deterioration_bps(initial, verification)
        route_changed = set(initial.route_exchanges) != set(verification.route_exchanges)
        if initial.block_number is not None and verification.block_number is not None:
            block_delta = verification.block_number - initial.block_number
        if initial.gas_cost_usd is not None and verification.gas_cost_usd is not None:
            gas_change = verification.gas_cost_usd - initial.gas_cost_usd
    return DexRouteShadowObservation(
        cycle_id=cycle_id,
        route_signature=initial_record.route_signature,
        asset=initial.asset,
        direction=initial.direction,
        source_amount_raw=initial.source_amount_raw,
        quote_notional_usd_proxy=quote_notional_usd_proxy(initial),
        delay_seconds=delay_seconds,
        started_at=initial.observed_at,
        verified_at=verified_at,
        initial_record_id=initial_record.record_id,
        verification_record_id=verification_record.record_id if verification_record else None,
        survived=survived,
        initial_effective_asset_price=initial.effective_asset_price,
        verification_effective_asset_price=verification.effective_asset_price if verification else None,
        price_deterioration_bps=deterioration,
        initial_gas_cost_usd=initial.gas_cost_usd,
        verification_gas_cost_usd=verification.gas_cost_usd if verification else None,
        gas_cost_change_usd=gas_change,
        initial_request_latency_ms=initial.request_latency_ms,
        verification_request_latency_ms=verification.request_latency_ms if verification else None,
        initial_block_number=initial.block_number,
        verification_block_number=verification.block_number if verification else None,
        block_delta=block_delta,
        initial_route_exchanges=list(initial.route_exchanges),
        verification_route_exchanges=list(verification.route_exchanges) if verification else [],
        route_changed=route_changed,
        failure_type=failure_type,
        capacity_claimed=False,
        transaction_built=False,
        paper_only=True,
    )


def summarize_route_cycles(cycles: list[DexRouteShadowCycle]) -> dict[str, object]:
    observations = [obs for cycle in cycles for obs in cycle.observations]
    by_horizon: dict[str, dict[str, object]] = {}
    for horizon in sorted({obs.delay_seconds for obs in observations}):
        rows = [obs for obs in observations if obs.delay_seconds == horizon]
        survived = [obs for obs in rows if obs.survived]
        adverse = [obs.price_deterioration_bps for obs in survived if obs.price_deterioration_bps is not None]
        by_horizon[str(horizon)] = {
            "observation_count": len(rows),
            "survived_count": len(survived),
            "survival_rate": len(survived) / len(rows) if rows else None,
            "mean_price_deterioration_bps": sum(adverse) / len(adverse) if adverse else None,
            "route_change_rate": (
                sum(bool(obs.route_changed) for obs in survived if obs.route_changed is not None)
                / sum(obs.route_changed is not None for obs in survived)
                if any(obs.route_changed is not None for obs in survived)
                else None
            ),
        }
    return {
        "cycle_count": len(cycles),
        "observation_count": len(observations),
        "by_horizon": by_horizon,
        "paper_only": True,
        "capacity_claimed": False,
    }
