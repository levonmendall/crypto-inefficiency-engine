from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import httpx

from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_REQUEST_ATTEMPTS = 3
HYPERLIQUID_RETRY_BACKOFF_SECONDS = 0.25
HYPERLIQUID_MAX_RETRY_AFTER_SECONDS = 1.0
HYPERLIQUID_TRANSIENT_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


def _utc_from_ms(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)


def parse_predicted_fundings(payload: Any, observed_at: datetime | None = None) -> list[FundingQuote]:
    observed_at = observed_at or datetime.now(timezone.utc)
    quotes: list[FundingQuote] = []
    if not isinstance(payload, list):
        raise ValueError("predictedFundings response must be a list")
    for row in payload:
        if not isinstance(row, list) or len(row) != 2:
            continue
        asset, venues = row
        if not isinstance(asset, str) or not isinstance(venues, list):
            continue
        for venue_row in venues:
            if not isinstance(venue_row, list) or len(venue_row) != 2:
                continue
            venue, details = venue_row
            if not isinstance(venue, str) or not isinstance(details, dict):
                continue
            rate = details.get("fundingRate")
            interval = details.get("fundingIntervalHours")
            if rate is None or interval is None:
                continue
            quotes.append(
                FundingQuote(
                    venue=venue,
                    asset=asset.upper(),
                    rate=float(rate),
                    interval_hours=float(interval),
                    symbol=asset.upper(),
                    quote_currency="USD",
                    contract_key="continuous",
                    next_funding_time=_utc_from_ms(details.get("nextFundingTime")),
                    observed_at=observed_at,
                    source="hyperliquid:predictedFundings",
                )
            )
    return quotes


def parse_meta_and_asset_contexts(payload: Any, observed_at: datetime | None = None) -> list[MarketQuote]:
    observed_at = observed_at or datetime.now(timezone.utc)
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("metaAndAssetCtxs response must be [meta, contexts]")
    meta, contexts = payload
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    if not isinstance(contexts, list):
        raise ValueError("asset contexts must be a list")
    quotes: list[MarketQuote] = []
    for instrument, context in zip(universe, contexts):
        if not isinstance(instrument, dict) or not isinstance(context, dict):
            continue
        asset = instrument.get("name")
        price = context.get("midPx") or context.get("markPx")
        if not asset or not price:
            continue
        quotes.append(
            MarketQuote(
                venue="HlPerp",
                asset=str(asset).upper(),
                market_kind=MarketKind.PERPETUAL,
                symbol=str(asset).upper(),
                quote_currency="USD",
                contract_key="continuous",
                mid=float(price),
                observed_at=observed_at,
                source="hyperliquid:metaAndAssetCtxs",
            )
        )
    return quotes


def parse_l2_book(payload: Any) -> OrderBookSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("l2Book response must be an object")
    coin = payload.get("coin")
    levels = payload.get("levels")
    if not isinstance(coin, str) or not isinstance(levels, list) or len(levels) != 2:
        raise ValueError("l2Book response must contain coin and [bids, asks]")

    def parse_side(rows: Any) -> list[OrderBookLevel]:
        if not isinstance(rows, list):
            raise ValueError("l2Book side must be a list")
        parsed: list[OrderBookLevel] = []
        for row in rows:
            if not isinstance(row, dict) or row.get("px") is None or row.get("sz") is None:
                continue
            parsed.append(OrderBookLevel(price=float(row["px"]), size=float(row["sz"])))
        return parsed

    observed_at = _utc_from_ms(payload.get("time")) or datetime.now(timezone.utc)
    return OrderBookSnapshot(
        venue="HlPerp",
        asset=coin.upper(),
        market_kind=MarketKind.PERPETUAL,
        symbol=coin.upper(),
        quote_currency="USD",
        contract_key="continuous",
        bids=parse_side(levels[0]),
        asks=parse_side(levels[1]),
        observed_at=observed_at,
        source="hyperliquid:l2Book",
    )


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        raw = response.headers.get("Retry-After")
        if raw:
            try:
                return min(
                    HYPERLIQUID_MAX_RETRY_AFTER_SECONDS,
                    max(0.0, float(raw)),
                )
            except ValueError:
                pass
    return min(
        HYPERLIQUID_MAX_RETRY_AFTER_SECONDS,
        HYPERLIQUID_RETRY_BACKOFF_SECONDS * max(1, attempt),
    )


class HyperliquidAdapter:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client

    async def _post_payload(self, payload: dict[str, Any]) -> Any:
        """POST one documented info request with bounded transport-only retries.

        Hyperliquid's info API is rate-limited per IP and can transiently return 429
        or 5xx responses. Retrying only transient HTTP/transport failures preserves the
        exact request type and response semantics while preventing one short provider
        rejection from marking funding, market data, and visible L2 failed together.
        Permanent 4xx responses remain fail-closed immediately.
        """

        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=10.0,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-cache",
                "User-Agent": "crypto-inefficiency-engine/hyperliquid-resilient",
            },
        )
        try:
            for attempt in range(1, HYPERLIQUID_REQUEST_ATTEMPTS + 1):
                response: httpx.Response | None = None
                try:
                    response = await client.post(INFO_URL, json=payload)
                    response.raise_for_status()
                    return response.json()
                except httpx.HTTPStatusError as exc:
                    status = int(exc.response.status_code)
                    if (
                        status not in HYPERLIQUID_TRANSIENT_STATUS_CODES
                        or attempt >= HYPERLIQUID_REQUEST_ATTEMPTS
                    ):
                        raise
                    await asyncio.sleep(_retry_delay(exc.response, attempt))
                except httpx.TransportError:
                    if attempt >= HYPERLIQUID_REQUEST_ATTEMPTS:
                        raise
                    await asyncio.sleep(_retry_delay(response, attempt))
            raise RuntimeError("Hyperliquid retry loop exhausted unexpectedly")
        finally:
            if owns_client:
                await client.aclose()

    async def _post(self, request_type: str) -> Any:
        return await self._post_payload({"type": request_type})

    async def funding_quotes(self) -> list[FundingQuote]:
        payload = await self._post("predictedFundings")
        return parse_predicted_fundings(payload)

    async def market_quotes(self) -> list[MarketQuote]:
        payload = await self._post("metaAndAssetCtxs")
        return parse_meta_and_asset_contexts(payload)

    async def order_book(self, asset: str) -> OrderBookSnapshot:
        started = perf_counter()
        payload = await self._post_payload({"type": "l2Book", "coin": asset.upper()})
        latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
        book = parse_l2_book(payload)
        book.request_latency_ms = latency_ms
        return book
