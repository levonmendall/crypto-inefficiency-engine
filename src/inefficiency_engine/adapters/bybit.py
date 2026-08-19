from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import httpx

from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot


BASE_URL = "https://api.bybit.com"


def _utc_from_ms(value: str | int | float | None) -> datetime | None:
    if value in (None, "", "0", 0):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)


def _checked_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise ValueError("Bybit response did not return retCode=0")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Bybit response must contain result object")
    return result


def _result_list(payload: Any) -> tuple[list[dict[str, Any]], datetime]:
    result = _checked_payload(payload)
    if not isinstance(result.get("list"), list):
        raise ValueError("Bybit response must contain result.list")
    observed_at = _utc_from_ms(payload.get("time")) or datetime.now(timezone.utc)
    return [item for item in result["list"] if isinstance(item, dict)], observed_at


@dataclass(frozen=True)
class BybitInstrumentSpec:
    symbol: str
    asset: str
    quote_currency: str
    market_kind: MarketKind
    contract_key: str
    expires_at: datetime | None = None
    funding_interval_hours: float | None = None


def parse_instruments_info(payload: Any) -> list[BybitInstrumentSpec]:
    rows, _ = _result_list(payload)
    specs: list[BybitInstrumentSpec] = []
    for row in rows:
        if row.get("status") != "Trading":
            continue
        symbol = str(row.get("symbol") or "")
        asset = str(row.get("baseCoin") or "").upper()
        quote = str(row.get("quoteCoin") or "").upper()
        if not symbol or not asset or not quote:
            continue
        contract_type = str(row.get("contractType") or "")
        if not contract_type:
            specs.append(
                BybitInstrumentSpec(
                    symbol=symbol,
                    asset=asset,
                    quote_currency=quote,
                    market_kind=MarketKind.SPOT,
                    contract_key="spot",
                )
            )
            continue
        if contract_type == "LinearPerpetual":
            interval_minutes = row.get("fundingInterval")
            specs.append(
                BybitInstrumentSpec(
                    symbol=symbol,
                    asset=asset,
                    quote_currency=quote,
                    market_kind=MarketKind.PERPETUAL,
                    contract_key="continuous",
                    funding_interval_hours=(float(interval_minutes) / 60.0 if interval_minutes else None),
                )
            )
        elif contract_type == "LinearFutures":
            expires_at = _utc_from_ms(row.get("deliveryTime"))
            if expires_at is None:
                continue
            specs.append(
                BybitInstrumentSpec(
                    symbol=symbol,
                    asset=asset,
                    quote_currency=quote,
                    market_kind=MarketKind.FUTURE,
                    contract_key=f"expiry-{expires_at.strftime('%Y%m%dT%H%M%SZ')}",
                    expires_at=expires_at,
                )
            )
    return specs


def parse_ticker(payload: Any, spec: BybitInstrumentSpec) -> tuple[MarketQuote | None, FundingQuote | None]:
    rows, observed_at = _result_list(payload)
    row = next((item for item in rows if str(item.get("symbol")) == spec.symbol), None)
    if row is None:
        return None, None
    bid_raw = row.get("bid1Price")
    ask_raw = row.get("ask1Price")
    last_raw = row.get("lastPrice") or row.get("markPrice")
    bid = float(bid_raw) if bid_raw not in (None, "", "0") else None
    ask = float(ask_raw) if ask_raw not in (None, "", "0") else None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    elif last_raw not in (None, "", "0"):
        mid = float(last_raw)
    else:
        return None, None
    quote = MarketQuote(
        venue="Bybit",
        asset=spec.asset,
        market_kind=spec.market_kind,
        symbol=spec.symbol,
        quote_currency=spec.quote_currency,
        contract_key=spec.contract_key,
        expires_at=spec.expires_at,
        bid=bid,
        ask=ask,
        mid=mid,
        observed_at=observed_at,
        source=f"bybit-v5:ticker:{'spot' if spec.market_kind == MarketKind.SPOT else 'linear'}",
    )
    funding: FundingQuote | None = None
    funding_rate = row.get("fundingRate")
    if spec.market_kind == MarketKind.PERPETUAL and funding_rate not in (None, ""):
        interval = row.get("fundingIntervalHour") or spec.funding_interval_hours
        if interval:
            funding = FundingQuote(
                venue="Bybit",
                asset=spec.asset,
                rate=float(funding_rate),
                interval_hours=float(interval),
                symbol=spec.symbol,
                quote_currency=spec.quote_currency,
                contract_key=spec.contract_key,
                next_funding_time=_utc_from_ms(row.get("nextFundingTime")),
                observed_at=observed_at,
                source="bybit-v5:ticker:funding",
            )
    return quote, funding


def parse_orderbook(payload: Any, spec: BybitInstrumentSpec) -> OrderBookSnapshot:
    result = _checked_payload(payload)

    def side(name: str) -> list[OrderBookLevel]:
        raw = result.get(name)
        if not isinstance(raw, list):
            raise ValueError("Bybit orderbook side must be a list")
        levels: list[OrderBookLevel] = []
        for level in raw:
            if isinstance(level, list) and len(level) >= 2:
                levels.append(OrderBookLevel(price=float(level[0]), size=float(level[1])))
        return levels

    observed_at = _utc_from_ms(result.get("ts")) or _utc_from_ms(payload.get("time")) or datetime.now(timezone.utc)
    return OrderBookSnapshot(
        venue="Bybit",
        asset=spec.asset,
        market_kind=spec.market_kind,
        symbol=spec.symbol,
        quote_currency=spec.quote_currency,
        contract_key=spec.contract_key,
        expires_at=spec.expires_at,
        bids=side("b"),
        asks=side("a"),
        observed_at=observed_at,
        source="bybit-v5:orderbook",
    )


class BybitPublicAdapter:
    """Public Bybit V5 market-data adapter; no credentials or trading calls."""

    def __init__(
        self,
        assets: tuple[str, ...] = ("BTC", "ETH", "SOL"),
        quote_currency: str = "USDT",
        max_futures_per_asset: int = 2,
        client: httpx.AsyncClient | None = None,
    ):
        self.assets = tuple(asset.upper() for asset in assets)
        self.quote_currency = quote_currency.upper()
        self.max_futures_per_asset = max(0, max_futures_per_asset)
        self._client = client

    async def _get(self, path: str, *, params: dict[str, object]) -> Any:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=10.0, headers={"Cache-Control": "no-cache"})
        try:
            response = await client.get(f"{BASE_URL}{path}", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    async def instrument_specs(self) -> list[BybitInstrumentSpec]:
        async def for_asset(asset: str) -> list[BybitInstrumentSpec]:
            spot_symbol = f"{asset}{self.quote_currency}"
            spot_payload, linear_payload = await asyncio.gather(
                self._get("/v5/market/instruments-info", params={"category": "spot", "symbol": spot_symbol}),
                self._get("/v5/market/instruments-info", params={"category": "linear", "baseCoin": asset, "limit": 1000}),
            )
            specs = [*parse_instruments_info(spot_payload), *parse_instruments_info(linear_payload)]
            specs = [spec for spec in specs if spec.asset == asset and spec.quote_currency == self.quote_currency]
            perpetuals = [spec for spec in specs if spec.market_kind == MarketKind.PERPETUAL]
            spots = [spec for spec in specs if spec.market_kind == MarketKind.SPOT]
            now = datetime.now(timezone.utc)
            futures = sorted(
                [spec for spec in specs if spec.market_kind == MarketKind.FUTURE and spec.expires_at and spec.expires_at > now],
                key=lambda item: item.expires_at or now,
            )[: self.max_futures_per_asset]
            return [*spots[:1], *perpetuals[:1], *futures]

        grouped = await asyncio.gather(*(for_asset(asset) for asset in self.assets))
        return [spec for group in grouped for spec in group]

    async def market_snapshot(self) -> tuple[list[MarketQuote], list[FundingQuote]]:
        specs = await self.instrument_specs()
        payloads = await asyncio.gather(*(
            self._get(
                "/v5/market/tickers",
                params={"category": "spot" if spec.market_kind == MarketKind.SPOT else "linear", "symbol": spec.symbol},
            )
            for spec in specs
        ))
        market_quotes: list[MarketQuote] = []
        funding_quotes: list[FundingQuote] = []
        for spec, payload in zip(specs, payloads):
            quote, funding = parse_ticker(payload, spec)
            if quote is not None:
                market_quotes.append(quote)
            if funding is not None:
                funding_quotes.append(funding)
        return market_quotes, funding_quotes

    async def order_book(
        self,
        *,
        asset: str,
        market_kind: MarketKind,
        symbol: str,
        quote_currency: str | None = None,
        contract_key: str | None = None,
        expires_at: datetime | None = None,
    ) -> OrderBookSnapshot:
        spec = BybitInstrumentSpec(
            symbol=symbol,
            asset=asset.upper(),
            quote_currency=(quote_currency or self.quote_currency).upper(),
            market_kind=market_kind,
            contract_key=contract_key or ("spot" if market_kind == MarketKind.SPOT else "continuous"),
            expires_at=expires_at,
        )
        category = "spot" if market_kind == MarketKind.SPOT else "linear"
        started = perf_counter()
        payload = await self._get("/v5/market/orderbook", params={"category": category, "symbol": symbol, "limit": 200})
        latency_ms = max(0.0, (perf_counter() - started) * 1000.0)
        book = parse_orderbook(payload, spec)
        book.request_latency_ms = latency_ms
        return book
