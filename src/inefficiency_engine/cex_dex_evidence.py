from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.config import Settings
from inefficiency_engine.conversion_depth import StablecoinConversionDepthQuote, quote_stablecoin_conversion_depth
from inefficiency_engine.costs import taker_fee_bps
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, DexRouteSizePoint
from inefficiency_engine.models import MarketKind, MarketQuote, OpportunityLeg, Side
from inefficiency_engine.universal import StablecoinConversionModel
from inefficiency_engine.universal_models import StablecoinConversionEdge


class CexDexCompositeEvidence(BaseModel):
    evidence_id: str
    frontier_id: str
    asset: str
    route_direction: str
    target_notional_usd: float = Field(gt=0)
    route_contiguous_acceptable: bool
    cex_venue: str
    cex_symbol: str
    cex_quote_currency: str
    cex_reference_price: float = Field(gt=0)
    route_quote_currency: str
    route_effective_asset_price: float = Field(gt=0)
    route_quote_notional_usd_proxy: float = Field(gt=0)
    conversion_depth_quote: StablecoinConversionDepthQuote | None = None
    conversion_risk_haircut_bps: float = Field(ge=0)
    cex_taker_fee_bps: float = Field(ge=0)
    gas_cost_bps: float = Field(ge=0)
    gross_edge_after_conversion_depth_bps: float
    net_research_edge_bps: float
    observed_at: datetime
    evidence_complete: bool
    blocked_reason: str
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def _fresh_conversion_path_risk(
    model: StablecoinConversionModel,
    source: str,
    target: str,
    *,
    reference_time: datetime,
    max_age_seconds: float,
) -> float | None:
    source, target = source.upper(), target.upper()
    if source == target:
        return 0.0
    resolved = model.best_path(source, target)
    if resolved is None:
        return None
    _, _, path = resolved
    if any(abs((reference_time - edge.observed_at).total_seconds()) > max_age_seconds for edge in path):
        return None
    return sum(edge.risk_haircut_bps for edge in path)


def _cex_fee_bps(quote: MarketQuote, settings: Settings, side: Side) -> float:
    leg = OpportunityLeg(
        venue=quote.venue,
        asset=quote.asset,
        market_kind=MarketKind.SPOT,
        side=side,
        symbol=quote.symbol,
        quote_currency=quote.quote_currency,
        contract_key=quote.contract_key,
        reference_price=quote.mid,
    )
    return taker_fee_bps(leg, settings)


def _point_route_notional(point: DexRouteSizePoint) -> float:
    if point.quote is None:
        raise ValueError("quoted DEX route point required")
    return point.quote.source_amount if point.quote.direction == "buy_asset" else point.quote.destination_amount


def build_cex_dex_composite_evidence(
    frontier: DexRouteSizeFrontier,
    point: DexRouteSizePoint,
    cex_quote: MarketQuote,
    conversion_books,
    conversion_model: StablecoinConversionModel,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> CexDexCompositeEvidence | None:
    if point.quote is None or not point.quoted:
        return None
    route = point.quote
    if cex_quote.market_kind != MarketKind.SPOT or cex_quote.asset.upper() != route.asset.upper():
        return None
    cex_currency = (cex_quote.quote_currency or "").upper()
    route_currency = route.quote_currency.upper()
    if not cex_currency:
        return None
    now = now or datetime.now(timezone.utc)
    route_age = max(0.0, (now - route.observed_at).total_seconds())
    cex_age = max(0.0, (now - cex_quote.observed_at).total_seconds())
    if max(route_age, cex_age) > settings.max_quote_age_seconds:
        return None

    conversion_quote: StablecoinConversionDepthQuote | None = None
    if route.direction == "sell_asset":
        asset_quantity = route.source_amount
        cex_reference = cex_quote.ask or cex_quote.mid
        cex_cost = asset_quantity * cex_reference
        if route_currency == cex_currency:
            normalized_route_proceeds = route.destination_amount
            conversion_risk = 0.0
        else:
            conversion_risk = _fresh_conversion_path_risk(
                conversion_model,
                route_currency,
                cex_currency,
                reference_time=max(route.observed_at, cex_quote.observed_at),
                max_age_seconds=settings.max_quote_age_seconds,
            )
            if conversion_risk is None:
                return None
            conversion_quote = quote_stablecoin_conversion_depth(
                route_currency,
                cex_currency,
                route.destination_amount,
                conversion_books,
                now=now,
                max_book_age_seconds=settings.max_order_book_age_seconds,
                max_book_skew_seconds=settings.max_order_book_skew_seconds,
            )
            normalized_route_proceeds = conversion_quote.output_amount
        gross_edge_bps = (normalized_route_proceeds / cex_cost - 1.0) * 10_000.0
        cex_fee = _cex_fee_bps(cex_quote, settings, Side.LONG)
    else:
        asset_quantity = route.destination_amount
        cex_reference = cex_quote.bid or cex_quote.mid
        cex_proceeds = asset_quantity * cex_reference
        if route_currency == cex_currency:
            normalized_cex_proceeds = cex_proceeds
            conversion_risk = 0.0
        else:
            conversion_risk = _fresh_conversion_path_risk(
                conversion_model,
                cex_currency,
                route_currency,
                reference_time=max(route.observed_at, cex_quote.observed_at),
                max_age_seconds=settings.max_quote_age_seconds,
            )
            if conversion_risk is None:
                return None
            conversion_quote = quote_stablecoin_conversion_depth(
                cex_currency,
                route_currency,
                cex_proceeds,
                conversion_books,
                now=now,
                max_book_age_seconds=settings.max_order_book_age_seconds,
                max_book_skew_seconds=settings.max_order_book_skew_seconds,
            )
            normalized_cex_proceeds = conversion_quote.output_amount
        gross_edge_bps = (normalized_cex_proceeds / route.source_amount - 1.0) * 10_000.0
        cex_fee = _cex_fee_bps(cex_quote, settings, Side.SHORT)

    route_notional = _point_route_notional(point)
    gas_bps = (
        route.gas_cost_usd / route_notional * 10_000.0
        if route.gas_cost_usd is not None and route_notional > 0
        else 0.0
    )
    net = gross_edge_bps - cex_fee - gas_bps - conversion_risk
    observed_times = [route.observed_at, cex_quote.observed_at]
    if conversion_quote is not None:
        observed_times.extend(leg.book_observed_at for leg in conversion_quote.legs)
    observed_at = min(observed_times)
    raw = (
        f"composite:{frontier.frontier_id}:{point.target_notional_usd}:{cex_quote.venue}:"
        f"{cex_quote.symbol}:{route.direction}:{observed_at.isoformat()}"
    )
    return CexDexCompositeEvidence(
        evidence_id=hashlib.sha256(raw.encode()).hexdigest()[:24],
        frontier_id=frontier.frontier_id,
        asset=route.asset,
        route_direction=route.direction,
        target_notional_usd=point.target_notional_usd,
        route_contiguous_acceptable=point.contiguous_acceptable,
        cex_venue=cex_quote.venue,
        cex_symbol=cex_quote.symbol,
        cex_quote_currency=cex_currency,
        cex_reference_price=cex_reference,
        route_quote_currency=route_currency,
        route_effective_asset_price=route.effective_asset_price,
        route_quote_notional_usd_proxy=route_notional,
        conversion_depth_quote=conversion_quote,
        conversion_risk_haircut_bps=conversion_risk,
        cex_taker_fee_bps=cex_fee,
        gas_cost_bps=gas_bps,
        gross_edge_after_conversion_depth_bps=gross_edge_bps,
        net_research_edge_bps=net,
        observed_at=observed_at,
        evidence_complete=True,
        blocked_reason=(
            "same-notional route and conversion depth are observed, but cross-venue inventory/settlement, "
            "atomic hedge recovery and statistical confidence gates are not yet qualified"
        ),
        capacity_claimed=False,
        executable_eligible=False,
        paper_only=True,
    )
