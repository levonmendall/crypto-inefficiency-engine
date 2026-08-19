from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Literal

import httpx

from inefficiency_engine.dex_routes import DexRouteQuote


VELORA_BASE_URL = "https://api.paraswap.io"
ETHEREUM_NETWORK_ID = 1
USDC_ADDRESS = "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
NATIVE_ETH_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
WBTC_ADDRESS = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"


@dataclass(frozen=True)
class TokenSpec:
    symbol: str
    address: str
    decimals: int


TOKENS = {
    "USDC": TokenSpec("USDC", USDC_ADDRESS, 6),
    "ETH": TokenSpec("ETH", NATIVE_ETH_ADDRESS, 18),
    "BTC": TokenSpec("BTC", WBTC_ADDRESS, 8),
}


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _route_exchanges(price_route: dict[str, Any]) -> list[str]:
    exchanges: set[str] = set()
    for route in price_route.get("bestRoute") or []:
        if not isinstance(route, dict):
            continue
        for swap in route.get("swaps") or []:
            if not isinstance(swap, dict):
                continue
            for venue in swap.get("swapExchanges") or []:
                if not isinstance(venue, dict):
                    continue
                name = venue.get("exchange")
                if name:
                    exchanges.add(str(name))
    return sorted(exchanges)


def parse_velora_price_route(
    payload: Any,
    *,
    asset: str,
    direction: Literal["buy_asset", "sell_asset"],
    request_latency_ms: float | None = None,
    observed_at: datetime | None = None,
) -> DexRouteQuote:
    if not isinstance(payload, dict) or not isinstance(payload.get("priceRoute"), dict):
        raise ValueError("Velora /prices response must contain priceRoute")
    route = payload["priceRoute"]
    src_raw = str(route.get("srcAmount") or "")
    dst_raw = str(route.get("destAmount") or "")
    if not src_raw.isdigit() or not dst_raw.isdigit():
        raise ValueError("Velora priceRoute requires integer srcAmount/destAmount")
    src_decimals = int(route["srcDecimals"])
    dst_decimals = int(route["destDecimals"])
    src_amount = int(src_raw) / (10 ** src_decimals)
    dst_amount = int(dst_raw) / (10 ** dst_decimals)
    if src_amount <= 0 or dst_amount <= 0:
        raise ValueError("Velora priceRoute amounts must be positive")
    effective_price = src_amount / dst_amount if direction == "buy_asset" else dst_amount / src_amount
    return DexRouteQuote(
        provider="Velora",
        network_id=int(route.get("network") or ETHEREUM_NETWORK_ID),
        chain_id="ethereum",
        asset=asset.upper(),
        quote_currency="USDC",
        direction=direction,
        source_token=str(route.get("srcToken") or ""),
        destination_token=str(route.get("destToken") or ""),
        source_decimals=src_decimals,
        destination_decimals=dst_decimals,
        source_amount_raw=src_raw,
        destination_amount_raw=dst_raw,
        source_amount=src_amount,
        destination_amount=dst_amount,
        effective_asset_price=effective_price,
        block_number=int(route["blockNumber"]) if route.get("blockNumber") is not None else None,
        route_exchanges=_route_exchanges(route),
        gas_cost_usd=_float_or_none(route.get("gasCostUSD")),
        request_latency_ms=request_latency_ms,
        observed_at=observed_at or datetime.now(timezone.utc),
        source="velora-market:prices:v6.2",
        amount_specific=True,
        transaction_built=False,
        executable_eligible=False,
    )


class VeloraPriceRouteAdapter:
    """Quote-only Velora Market API adapter.

    It calls `/prices` only. It does not call transaction-building endpoints and
    contains no signer, wallet, allowance, approval or transaction-submission path.
    RFQ liquidity is excluded so the evidence reflects routed DEX/AMM liquidity.
    """

    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client

    async def _get(self, params: dict[str, object]) -> tuple[Any, float]:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=12.0, headers={"Cache-Control": "no-cache"})
        started = perf_counter()
        try:
            response = await client.get(f"{VELORA_BASE_URL}/prices", params=params)
            latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
            response.raise_for_status()
            return response.json(), latency_ms
        finally:
            if owns:
                await client.aclose()

    async def quote_raw(
        self,
        asset: str,
        direction: Literal["buy_asset", "sell_asset"],
        *,
        source_amount_raw: str,
    ) -> DexRouteQuote:
        asset = asset.upper()
        if asset not in {"BTC", "ETH"}:
            raise ValueError(f"Velora route evidence does not support asset {asset}")
        if not source_amount_raw.isdigit() or int(source_amount_raw) <= 0:
            raise ValueError("source_amount_raw must be a positive integer string")
        asset_spec = TOKENS[asset]
        usdc = TOKENS["USDC"]
        src, dest = (usdc, asset_spec) if direction == "buy_asset" else (asset_spec, usdc)
        payload, latency_ms = await self._get({
            "srcToken": src.address,
            "srcDecimals": src.decimals,
            "destToken": dest.address,
            "destDecimals": dest.decimals,
            "amount": source_amount_raw,
            "side": "SELL",
            "network": ETHEREUM_NETWORK_ID,
            "version": "6.2",
            "excludeRFQ": "true",
        })
        return parse_velora_price_route(
            payload,
            asset=asset,
            direction=direction,
            request_latency_ms=latency_ms,
        )

    async def quote(
        self,
        asset: str,
        direction: Literal["buy_asset", "sell_asset"],
        *,
        notional_usd: float,
        reference_price: float,
    ) -> DexRouteQuote:
        asset = asset.upper()
        if notional_usd <= 0 or reference_price <= 0:
            raise ValueError("notional_usd and reference_price must be positive")
        asset_spec = TOKENS.get(asset)
        if asset_spec is None:
            raise ValueError(f"Velora route evidence does not support asset {asset}")
        usdc = TOKENS["USDC"]
        src = usdc if direction == "buy_asset" else asset_spec
        human_amount = notional_usd if direction == "buy_asset" else notional_usd / reference_price
        raw_amount = str(max(1, int(human_amount * (10 ** src.decimals))))
        return await self.quote_raw(asset, direction, source_amount_raw=raw_amount)

    async def requote(self, initial: DexRouteQuote) -> DexRouteQuote:
        """Re-quote the exact same source amount for survival measurement."""
        return await self.quote_raw(
            initial.asset,
            initial.direction,
            source_amount_raw=initial.source_amount_raw,
        )

    async def quotes_for_market(
        self,
        reference_prices: dict[str, float],
        *,
        notional_usd: float = 1000.0,
    ) -> list[DexRouteQuote]:
        quotes: list[DexRouteQuote] = []
        for asset in ("BTC", "ETH"):
            reference = reference_prices.get(asset)
            if reference is None or reference <= 0:
                continue
            for direction in ("buy_asset", "sell_asset"):
                try:
                    quotes.append(await self.quote(
                        asset,
                        direction,
                        notional_usd=notional_usd,
                        reference_price=reference,
                    ))
                except httpx.HTTPStatusError:
                    continue
        return quotes
