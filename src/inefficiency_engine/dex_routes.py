from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Literal, Protocol

from pydantic import BaseModel, Field, model_validator

from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.universal_models import StablecoinConversionEdge, UniversalCandidate, UniversalFamily


class ConversionModelProtocol(Protocol):
    def best_path(
        self, source: str, target: str
    ) -> tuple[float, float, list[StablecoinConversionEdge]] | None: ...


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


def _conversion_path(
    model: ConversionModelProtocol | None,
    source: str,
    target: str,
) -> tuple[float, list[StablecoinConversionEdge]] | None:
    source, target = source.upper(), target.upper()
    if source == target:
        return 1.0, []
    if model is None:
        return None
    resolved = model.best_path(source, target)
    if resolved is None:
        return None
    rate, _, path = resolved
    return rate, path


def _path_is_fresh(
    path: list[StablecoinConversionEdge],
    *,
    reference_time: datetime,
    max_age_seconds: float,
) -> bool:
    if max_age_seconds < 0:
        return False
    return all(abs((reference_time - edge.observed_at).total_seconds()) <= max_age_seconds for edge in path)


def _path_evidence(path: list[StablecoinConversionEdge]) -> list[dict[str, object]]:
    return [
        {
            "source_currency": edge.source_currency,
            "target_currency": edge.target_currency,
            "venue": edge.venue,
            "rate": edge.rate,
            "spread_bps": edge.spread_bps,
            "depeg_bps": edge.depeg_bps,
            "risk_haircut_bps": edge.risk_haircut_bps,
            "observed_at": edge.observed_at.isoformat(),
            "source": edge.source,
        }
        for edge in path
    ]


def detect_route_quoted_cex_dex(
    market_quotes: list[MarketQuote],
    route_quotes: list[DexRouteQuote],
    *,
    conversion_model: ConversionModelProtocol | None = None,
    minimum_edge_bps: float = 12.0,
    conversion_max_age_seconds: float = 120.0,
) -> list[UniversalCandidate]:
    """Compare amount-specific DEX routes with CEX spot after explicit quote-currency conversion.

    Cross-currency comparisons fail closed unless a fresh observed conversion path
    exists. Conversion bid/ask is embedded in the path rate; only path risk/depeg
    haircuts are charged separately to avoid double-counting spread.
    """
    grouped = _cex_spot_by_asset(market_quotes)
    results: list[UniversalCandidate] = []
    for route in route_quotes:
        dex_currency = route.quote_currency.upper()
        for cex in grouped.get(route.asset.upper(), []):
            cex_currency = (cex.quote_currency or "").upper()
            if not cex_currency:
                continue
            reference_time = max(route.observed_at, cex.observed_at)
            if route.direction == "sell_asset":
                resolved = _conversion_path(conversion_model, dex_currency, cex_currency)
                if resolved is None:
                    continue
                conversion_rate, conversion_path = resolved
                if not _path_is_fresh(
                    conversion_path,
                    reference_time=reference_time,
                    max_age_seconds=conversion_max_age_seconds,
                ):
                    continue
                cex_reference = cex.ask or cex.mid
                converted_dex_price = route.effective_asset_price * conversion_rate
                raw_edge = converted_dex_price / cex_reference - 1.0
                economic_direction = "buy_cex_sell_dex"
                conversion_source, conversion_target = dex_currency, cex_currency
            else:
                resolved = _conversion_path(conversion_model, cex_currency, dex_currency)
                if resolved is None:
                    continue
                conversion_rate, conversion_path = resolved
                if not _path_is_fresh(
                    conversion_path,
                    reference_time=reference_time,
                    max_age_seconds=conversion_max_age_seconds,
                ):
                    continue
                cex_reference = cex.bid or cex.mid
                converted_cex_proceeds = cex_reference * conversion_rate
                raw_edge = converted_cex_proceeds / route.effective_asset_price - 1.0
                economic_direction = "buy_dex_sell_cex"
                conversion_source, conversion_target = cex_currency, dex_currency

            gross_bps = raw_edge * 10_000.0
            if gross_bps < minimum_edge_bps:
                continue
            quote_notional = route.source_amount if route.direction == "buy_asset" else route.destination_amount
            gas_bps = (
                route.gas_cost_usd / quote_notional * 10_000.0
                if route.gas_cost_usd is not None and quote_notional > 0
                else 0.0
            )
            conversion_risk_bps = sum(edge.risk_haircut_bps for edge in conversion_path)
            conversion_spread_bps = sum(
                max(0.0, edge.total_conversion_cost_bps - edge.risk_haircut_bps)
                for edge in conversion_path
            )
            raw = (
                f"route-cex-dex:{route.provider}:{route.asset}:{route.direction}:"
                f"{cex.venue}:{cex_currency}:{route.block_number}:{route.observed_at.isoformat()}"
            )
            evidence_times = [cex.observed_at, route.observed_at, *[edge.observed_at for edge in conversion_path]]
            earliest_evidence = min(evidence_times)
            results.append(UniversalCandidate(
                candidate_id=hashlib.sha256(raw.encode()).hexdigest()[:20],
                family=UniversalFamily.CEX_DEX,
                asset=route.asset.upper(),
                gross_edge_bps=gross_bps,
                modeled_cost_bps=gas_bps,
                risk_haircut_bps=conversion_risk_bps,
                net_edge_bps=gross_bps - gas_bps - conversion_risk_bps,
                capacity_usd=None,
                observed_at=earliest_evidence,
                expires_at=earliest_evidence + timedelta(seconds=30),
                executable_eligible=False,
                blocked_reason=(
                    "amount-specific DEX route and quote-currency conversion are observed, but conversion depth, "
                    "cross-venue inventory/settlement, quote survival and atomic hedge recovery are not yet qualified"
                ),
                evidence={
                    "price_evidence": "amount_specific_route_quote_with_observed_conversion",
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
                    "dex_quote_currency": dex_currency,
                    "cex_quote_currency": cex_currency,
                    "conversion_source_currency": conversion_source,
                    "conversion_target_currency": conversion_target,
                    "conversion_rate": conversion_rate,
                    "conversion_market_spread_embedded_in_rate": True,
                    "conversion_spread_bps_reference": conversion_spread_bps,
                    "conversion_risk_haircut_bps": conversion_risk_bps,
                    "conversion_path": _path_evidence(conversion_path),
                    "dex_effective_asset_price": route.effective_asset_price,
                    "cex_venue": cex.venue,
                    "cex_reference_price": cex_reference,
                    "gas_cost_usd": route.gas_cost_usd,
                    "request_latency_ms": route.request_latency_ms,
                    "transaction_built": False,
                },
            ))
    return sorted(results, key=lambda item: item.net_edge_bps, reverse=True)
