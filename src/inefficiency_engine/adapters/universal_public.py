from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import httpx

from inefficiency_engine.universal_models import ChainToken, DexPoolSnapshot, OptionQuote, StablecoinConversionObservation

COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
DEXSCREENER_BASE_URL = "https://api.dexscreener.com"
DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"
DEFAULT_DEX_TOKENS = (
    ("ethereum", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "ETH"),
    ("ethereum", "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC"),
)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class CoinbaseStablecoinAdapter:
    def __init__(self, symbols: tuple[str, ...] = ("USDT-USD", "USDC-USD"), client: httpx.AsyncClient | None = None):
        self.symbols = symbols
        self._client = client

    async def _get(self, path: str) -> Any:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0, headers={"Cache-Control": "no-cache"})
        try:
            response = await client.get(f"{COINBASE_BASE_URL}{path}")
            response.raise_for_status()
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def observations(self) -> list[StablecoinConversionObservation]:
        rows: list[StablecoinConversionObservation] = []
        for symbol in self.symbols:
            try:
                data = await self._get(f"/products/{symbol}/ticker")
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            base, quote = symbol.split("-", 1)
            bid, ask = float(data["bid"]), float(data["ask"])
            rows.append(StablecoinConversionObservation(
                venue="Coinbase", base_currency=base, quote_currency=quote, symbol=symbol,
                bid=bid, ask=ask, mid=(bid+ask)/2.0, observed_at=datetime.now(timezone.utc),
                source="coinbase-exchange:stablecoin-ticker",
            ))
        return rows


def parse_dexscreener_pairs(payload: Any, *, observed_at: datetime | None = None) -> list[DexPoolSnapshot]:
    observed_at = observed_at or datetime.now(timezone.utc)
    raw_pairs = payload.get("pairs") or [] if isinstance(payload, dict) else payload if isinstance(payload, list) else None
    if raw_pairs is None:
        raise ValueError("DexScreener payload must be an object or list")
    pools: list[DexPoolSnapshot] = []
    for row in raw_pairs:
        if not isinstance(row, dict):
            continue
        chain_id, dex_id, pair_address = str(row.get("chainId") or ""), str(row.get("dexId") or ""), str(row.get("pairAddress") or "")
        base = row.get("baseToken") if isinstance(row.get("baseToken"), dict) else {}
        quote = row.get("quoteToken") if isinstance(row.get("quoteToken"), dict) else {}
        if not chain_id or not dex_id or not pair_address or not base.get("address") or not quote.get("address"):
            continue
        liquidity = row.get("liquidity") if isinstance(row.get("liquidity"), dict) else {}
        volume = row.get("volume") if isinstance(row.get("volume"), dict) else {}
        base_symbol, quote_symbol = str(base.get("symbol") or "").upper(), str(quote.get("symbol") or "").upper()
        canonical = lambda s: {"WETH":"ETH", "WBTC":"BTC"}.get(s, s or None)
        base_token = ChainToken(token_id=f"token:{chain_id}:{str(base['address']).lower()}", chain_id=chain_id,
                                address=str(base["address"]), symbol=base_symbol,
                                name=str(base.get("name")) if base.get("name") else None, canonical_asset=canonical(base_symbol))
        quote_token = ChainToken(token_id=f"token:{chain_id}:{str(quote['address']).lower()}", chain_id=chain_id,
                                 address=str(quote["address"]), symbol=quote_symbol,
                                 name=str(quote.get("name")) if quote.get("name") else None, canonical_asset=canonical(quote_symbol))
        pools.append(DexPoolSnapshot(
            chain_id=chain_id, dex_id=dex_id, pair_address=pair_address, base_token=base_token, quote_token=quote_token,
            price_native=_float_or_none(row.get("priceNative")), price_usd=_float_or_none(row.get("priceUsd")),
            liquidity_usd=_float_or_none(liquidity.get("usd")), reported_base_liquidity=_float_or_none(liquidity.get("base")),
            reported_quote_liquidity=_float_or_none(liquidity.get("quote")), volume_24h_usd=_float_or_none(volume.get("h24")),
            observed_at=observed_at, source="dexscreener:token-pairs", depth_model="reported_liquidity_proxy",
            executable_depth_supported=False,
        ))
    return pools


class DexScreenerAdapter:
    def __init__(self, token_queries: tuple[tuple[str,str,str], ...] = DEFAULT_DEX_TOKENS,
                 client: httpx.AsyncClient | None = None):
        self.token_queries = token_queries
        self._client = client

    async def _get(self, path: str) -> Any:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.get(f"{DEXSCREENER_BASE_URL}{path}")
            response.raise_for_status()
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def pools(self) -> list[DexPoolSnapshot]:
        pools: dict[tuple[str,str], DexPoolSnapshot] = {}
        for chain_id, token_address, _ in self.token_queries:
            try:
                payload = await self._get(f"/token-pairs/v1/{chain_id}/{token_address}")
            except httpx.HTTPStatusError:
                continue
            for pool in parse_dexscreener_pairs(payload):
                pools[(pool.chain_id, pool.pair_address.lower())] = pool
        return sorted(pools.values(), key=lambda item: (item.liquidity_usd or 0.0, item.volume_24h_usd or 0.0), reverse=True)


_OPTION_NAME = re.compile(r"^(?P<asset>[A-Z0-9]+)-(?P<expiry>[0-9]{1,2}[A-Z]{3}[0-9]{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$")

def _parse_deribit_expiry(value: str) -> datetime:
    return datetime.strptime(value, "%d%b%y").replace(hour=8, tzinfo=timezone.utc)


def parse_deribit_option_summaries(payload: Any, *, observed_at: datetime | None = None) -> list[OptionQuote]:
    observed_at = observed_at or datetime.now(timezone.utc)
    if not isinstance(payload, dict) or not isinstance(payload.get("result"), list):
        raise ValueError("Deribit option summary response must contain result list")
    rows: list[OptionQuote] = []
    for item in payload["result"]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("instrument_name") or "")
        match = _OPTION_NAME.match(name)
        if not match:
            continue
        rows.append(OptionQuote(
            venue="Deribit", asset=match.group("asset"), instrument_name=name,
            option_type="call" if match.group("type") == "C" else "put", strike=float(match.group("strike")),
            expires_at=_parse_deribit_expiry(match.group("expiry")), bid_price=_float_or_none(item.get("bid_price")),
            ask_price=_float_or_none(item.get("ask_price")), mark_price=_float_or_none(item.get("mark_price")),
            mark_iv=_float_or_none(item.get("mark_iv")), underlying_price=_float_or_none(item.get("underlying_price")),
            open_interest=_float_or_none(item.get("open_interest")), observed_at=observed_at,
            source="deribit:public:get_book_summary_by_currency",
        ))
    return rows


class DeribitOptionsAdapter:
    def __init__(self, assets: tuple[str, ...] = ("BTC", "ETH"), client: httpx.AsyncClient | None = None):
        self.assets = assets
        self._client = client

    async def _get(self, params: dict[str, object]) -> Any:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.get(f"{DERIBIT_BASE_URL}/public/get_book_summary_by_currency", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns:
                await client.aclose()

    async def option_quotes(self) -> list[OptionQuote]:
        rows: list[OptionQuote] = []
        for asset in self.assets:
            try:
                payload = await self._get({"currency": asset, "kind": "option"})
            except httpx.HTTPStatusError:
                continue
            rows.extend(parse_deribit_option_summaries(payload))
        return rows
