from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.universal_models import UniversalCandidate, UniversalFamily


class DexRouteQuote(BaseModel):
    provider: str
    network_id: int
    chain_id: str
    asset: str
    quote_currency: str
    direction: Literal["buy_asset", "sell_asset"]
    source_token: str
    destination_token: str
    source_decimals: int = Field(ge=0, le=36)
    destination_decimals: int = Field(ge=0, le=36)
    source_amount_raw: str
    destination_amount_raw: str
    source_amount: float = Field(gt=0)
    destination_amount: float = Field(gt=0)
    effective_asset_price: float = Field(gt=0)
    block_number: int | None = Field(default=None, ge=0)
    route_exchanges: list[str] = Field(default_factory=list)
    gas_cost_usd: float | None = Field(default=None, ge=0)
    request_latency_ms: float | None = Field(default=None, ge=0)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str
    amount_specific: bool = True
    transaction_built: bool = False
    executable_eligible: bool = False

    @model_validator(mode="after")
    def validate_direction(self):
        if self.direction == "buy_asset" and self.source_amount <= 0:
            raise ValueError("buy quote requires positive quote-currency source amount")
        if self.direction == "sell_asset" and self.destination_amount <= 0:
            raise ValueError("sell quote requires positive quote-currency destination amount")
        return self


def _cex_spot_by_asset(market_quotes: list[MarketQuote]) -> dict[str, list[MarketQuote]]:
    grouped: dict[str, list[MarketQuote]] = {}
    for quote in market_quotes:
        if quote.market_kind != MarketKind.SPOT or quote.asset.upper() not in {"BTC", "ETH"}:
            continue
        grouped.setdefault(quote.asset.upper(), []).append(quote)
    return grouped


def detect_route_quoted_cex_dex(
    market_quotes: list[MarketQuote],
    route_quotes: list[DexRouteQuote],
    *,
    minimum_edge_bps: float = 12.0,
    conversion_risk_floor_bps: float = 2.0,
) -> list[UniversalCandidate]:
    """Compare amount-specific DEX routes with observable CEX spot prices.

    This improves price evidence but intentionally does not grant executability:
    cross-venue inventory/settlement, stablecoin conversion, quote survival and
    atomic hedge recovery are not yet qualified.
    """
    grouped = _cex_spot_by_asset(market_quotes)
    results: list[UniversalCandidate] = []
    for route in route_quotes:
        for cex in grouped.get(route.asset.upper(), []):
            if route.direction == "sell_asset":
                cex_reference = cex.ask or cex.mid
                raw_edge = route.effective_asset_price / cex_reference - 1.0
                economic_direction = "buy_cex_sell_dex"
            else:
                cex_reference = cex.bid or cex.mid
                raw_edge = cex_reference / route.effective_asset_price - 1.0
                economic_direction = "buy_dex_sell_cex"
            gross_bps = raw_edge * 10_000.0
            if gross_bps < minimum_edge_bps:
                continue
            quote_notional = route.source_amount if route.direction == "buy_asset" else route.destination_amount
            gas_bps = (
                route.gas_cost_usd / quote_notional * 10_000.0
                if route.gas_cost_usd is not None and quote_notional > 0
                else 0.0
            )
            risk_bps = max(0.0, conversion_risk_floor_bps)
            raw = (
                f"route-cex-dex:{route.provider}:{route.asset}:{route.direction}:"
                f"{cex.venue}:{route.block_number}:{route.observed_at.isoformat()}"
            )
            results.append(UniversalCandidate(
                candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20],
                family=UniversalFamily.CEX_DEX,
                asset=route.asset.upper(),
                gross_edge_bps=gross_bps,
                modeled_cost_bps=gas_bps,
                risk_haircut_bps=risk_bps,
                net_edge_bps=gross_bps - gas_bps - risk_bps,
                capacity_usd=None,
                observed_at=min(cex.observed_at, route.observed_at),
                expires_at=min(cex.observed_at, route.observed_at) + timedelta(seconds=30),
                executable_eligible=False,
                blocked_reason=(
                    "amount-specific DEX route quote is available, but cross-venue inventory/settlement, "
                    "USD/stablecoin conversion and quote-survival evidence are not yet qualified"
                ),
                evidence={
                    "price_evidence": "amount_specific_route_quote",
                    "provider": route.provider,
                    "network_id": route.network_id,
                    "block_number": route.block_number,
                    "route_exchanges": route.route_exchanges,
                    "route_direction": route.direction,
                    "economic_direction": economic_direction,
                    "source_amount": route.source_amount,
                    "destination_amount": route.destination_amount,
                    "quote_notional_usd_proxy": quote_notional,
                    "capacity_claimed": False,
                    "dex_effective_asset_price": route.effective_asset_price,
                    "cex_venue": cex.venue,
                    "cex_reference_price": cex_reference,
                    "gas_cost_usd": route.gas_cost_usd,
                    "request_latency_ms": route.request_latency_ms,
                    "transaction_built": False,
                },
            ))
    return sorted(results, key=lambda item: item.net_edge_bps, reverse=True)
