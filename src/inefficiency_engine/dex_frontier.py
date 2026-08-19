from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.dex_shadow import price_deterioration_bps, quote_notional_usd_proxy


class DexRouteSizePoint(BaseModel):
    target_notional_usd: float = Field(gt=0)
    quoted: bool
    quote_notional_usd_proxy: float | None = Field(default=None, gt=0)
    effective_asset_price: float | None = Field(default=None, gt=0)
    price_deterioration_bps: float | None = None
    gas_cost_usd: float | None = Field(default=None, ge=0)
    gas_cost_bps: float | None = Field(default=None, ge=0)
    request_latency_ms: float | None = Field(default=None, ge=0)
    block_number: int | None = Field(default=None, ge=0)
    route_exchanges: list[str] = Field(default_factory=list)
    route_changed_from_baseline: bool | None = None
    within_deterioration_limit: bool = False
    contiguous_acceptable: bool = False
    failure_type: str | None = None
    quote: DexRouteQuote | None = None


class DexRouteSizeFrontier(BaseModel):
    frontier_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    provider: str = "Velora"
    network_id: int = 1
    chain_id: str = "ethereum"
    asset: str
    direction: Literal["buy_asset", "sell_asset"]
    reference_price: float = Field(gt=0)
    requested_notionals_usd: list[float] = Field(default_factory=list)
    deterioration_limit_bps: float = Field(ge=0)
    points: list[DexRouteSizePoint] = Field(default_factory=list)
    largest_successful_tier_usd: float | None = Field(default=None, gt=0)
    largest_contiguous_acceptable_tier_usd: float | None = Field(default=None, gt=0)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def build_size_frontier(
    *,
    asset: str,
    direction: Literal["buy_asset", "sell_asset"],
    reference_price: float,
    quote_results: list[tuple[float, DexRouteQuote | None, str | None]],
    deterioration_limit_bps: float,
) -> DexRouteSizeFrontier:
    ordered = sorted(quote_results, key=lambda row: row[0])
    baseline_quote = ordered[0][1] if ordered and ordered[0][1] is not None else None
    points: list[DexRouteSizePoint] = []
    contiguous = baseline_quote is not None
    largest_successful = None
    largest_acceptable = None
    for target, quote, failure_type in ordered:
        if quote is None:
            contiguous = False
            points.append(DexRouteSizePoint(
                target_notional_usd=target,
                quoted=False,
                failure_type=failure_type or "QuoteUnavailable",
                within_deterioration_limit=False,
                contiguous_acceptable=False,
            ))
            continue
        largest_successful = target
        proxy = quote_notional_usd_proxy(quote)
        deterioration = 0.0 if baseline_quote is None else price_deterioration_bps(baseline_quote, quote)
        gas_bps = quote.gas_cost_usd / proxy * 10_000.0 if quote.gas_cost_usd is not None and proxy > 0 else None
        within = baseline_quote is not None and deterioration <= deterioration_limit_bps
        contiguous = bool(contiguous and within)
        if contiguous:
            largest_acceptable = target
        points.append(DexRouteSizePoint(
            target_notional_usd=target,
            quoted=True,
            quote_notional_usd_proxy=proxy,
            effective_asset_price=quote.effective_asset_price,
            price_deterioration_bps=deterioration,
            gas_cost_usd=quote.gas_cost_usd,
            gas_cost_bps=gas_bps,
            request_latency_ms=quote.request_latency_ms,
            block_number=quote.block_number,
            route_exchanges=list(quote.route_exchanges),
            route_changed_from_baseline=(
                set(quote.route_exchanges) != set(baseline_quote.route_exchanges)
                if baseline_quote is not None else None
            ),
            within_deterioration_limit=within,
            contiguous_acceptable=contiguous,
            quote=quote,
        ))
    observed = max(
        (point.quote.observed_at for point in points if point.quote is not None),
        default=datetime.now(timezone.utc),
    )
    return DexRouteSizeFrontier(
        asset=asset.upper(),
        direction=direction,
        reference_price=reference_price,
        requested_notionals_usd=[row[0] for row in ordered],
        deterioration_limit_bps=deterioration_limit_bps,
        points=points,
        largest_successful_tier_usd=largest_successful,
        largest_contiguous_acceptable_tier_usd=largest_acceptable,
        observed_at=observed,
        capacity_claimed=False,
        executable_eligible=False,
        paper_only=True,
    )


def summarize_size_frontiers(frontiers: list[DexRouteSizeFrontier]) -> dict[str, object]:
    latest: dict[str, DexRouteSizeFrontier] = {}
    for frontier in sorted(frontiers, key=lambda item: item.observed_at):
        latest[f"{frontier.asset}:{frontier.direction}"] = frontier
    return {
        "frontier_count": len(frontiers),
        "latest": {
            key: {
                "observed_at": item.observed_at.isoformat(),
                "largest_successful_tier_usd": item.largest_successful_tier_usd,
                "largest_contiguous_acceptable_tier_usd": item.largest_contiguous_acceptable_tier_usd,
                "deterioration_limit_bps": item.deterioration_limit_bps,
            }
            for key, item in latest.items()
        },
        "capacity_claimed": False,
        "paper_only": True,
    }
