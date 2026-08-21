from __future__ import annotations

import hashlib
import os
import statistics
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx
from sqlalchemy import Column, Index, MetaData, String, Table, Text, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


COINBASE_EXCHANGE_URL = "https://api.exchange.coinbase.com"
OKX_URL = "https://www.okx.com"
BYBIT_URL = "https://api.bybit.com"
COINGECKO_PUBLIC_API_URL = "https://api.coingecko.com/api/v3"
COINGECKO_PRO_API_URL = "https://pro-api.coingecko.com/api/v3"
HISTORICAL_BACKFILL_DAYS = 365
HISTORICAL_CANDLE_SECONDS = 21600
HISTORICAL_REQUEST_CHUNK_DAYS = 60
HISTORICAL_COINGECKO_CHUNK_DAYS = 89
HISTORICAL_OKX_MAX_PAGES = 24
HISTORICAL_REPLAY_STEP_HOURS = 72.0
HISTORICAL_REPLAY_MIN_SAMPLES = 24
HISTORICAL_REPLAY_MIN_HIT_RATE = 0.55
PROBATIONARY_FORWARD_MIN_SAMPLES = 8
PROBATIONARY_FORWARD_MIN_HIT_RATE = 0.55
PROBATIONARY_MAX_CANDIDATE_FRACTION = 0.02
PROBATIONARY_MAX_TOTAL_FRACTION = 0.05
PROBATIONARY_FORWARD_MEAN_HAIRCUT = 0.50
PROBATIONARY_HISTORICAL_MEAN_HAIRCUT = 0.35
HISTORICAL_PROVIDER_VENUES = frozenset({"Coinbase", "OKX", "Bybit", "CoinGecko"})
_PROVIDER_PRIORITY = {"Coinbase": 4, "OKX": 3, "Bybit": 2, "CoinGecko": 1}


def _utc_day_start(value: datetime) -> datetime:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    aware = aware.astimezone(timezone.utc)
    return aware.replace(hour=0, minute=0, second=0, microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _quote_id(quote: MarketQuote) -> str:
    raw = "|".join(
        [
            quote.venue,
            quote.asset.upper(),
            quote.market_kind.value,
            quote.symbol,
            quote.observed_at.astimezone(timezone.utc).isoformat(),
        ]
    )
    return hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class HistoricalBackfillReport:
    requested_assets: tuple[str, ...]
    fetched_assets: tuple[str, ...]
    stored_quote_count: int
    total_quote_count: int
    earliest_observed_at: datetime | None
    latest_observed_at: datetime | None
    errors: tuple[str, ...] = ()
    provider_diagnostics: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class CycleReplaySummary:
    strategy_id: str
    asset: str
    direction: str
    sample_count: int
    positive_count: int
    hit_rate: float | None
    mean_realized_net_return: float | None
    regime_count: int
    regime_means: dict[str, float]
    qualified_for_probationary_support: bool


@dataclass(frozen=True)
class ProbationaryPolicyDecision:
    eligible: bool
    blockers: tuple[str, ...]


def probationary_policy(
    qualification,
    health,
    replay: CycleReplaySummary | None,
    settings,
) -> ProbationaryPolicyDecision:
    """Strict pre-qualification paper gate; never changes full promotion criteria."""
    blockers: list[str] = []
    if qualification.statistically_qualified:
        blockers.append("already fully qualified")
    if qualification.sample_count < PROBATIONARY_FORWARD_MIN_SAMPLES:
        blockers.append("insufficient genuine forward outcomes for probationary paper")
    if (
        qualification.mean_realized_net_return is None
        or qualification.mean_realized_net_return <= qualification.required_mean_lower_bound
    ):
        blockers.append("forward mean return is below probationary hurdle")
    if qualification.hit_rate is None or qualification.hit_rate < PROBATIONARY_FORWARD_MIN_HIT_RATE:
        blockers.append("forward hit rate is below probationary hurdle")
    if qualification.regime_count < 1:
        blockers.append("no forward regime coverage")
    if not health.healthy_for_paper_allocation or health.capital_multiplier <= 0:
        blockers.append("adaptive alpha health is not paper-healthy")
    if replay is None or not replay.qualified_for_probationary_support:
        blockers.append("historical walk-forward support is not qualified")
    return ProbationaryPolicyDecision(eligible=not blockers, blockers=tuple(blockers))


class CycleHistoricalResearch:
    """Separated historical evidence for warm-up and walk-forward support.

    Historical quotes are stored outside EvidenceStore.market_quotes so they cannot
    be mistaken for live point-in-time scans or genuine forward alpha outcomes.
    Historical replay can support probationary paper sizing only; it never
    contributes to AlphaQualification.sample_count.

    Backfill uses a provider cascade rather than treating a single exchange as a
    universal source. Every provider retains truthful source/venue lineage, while
    coverage and replay select one best continuous provider per asset so multiple
    fallbacks can never inflate the historical-completeness gate.
    """

    def __init__(
        self,
        store: EvidenceStore,
        *,
        client: httpx.AsyncClient | None = None,
        backfill_days: int = HISTORICAL_BACKFILL_DAYS,
    ):
        self.store = store
        self._client = client
        self.backfill_days = max(260, int(backfill_days))
        self.metadata = MetaData()
        self.quotes = Table(
            "cycle_historical_quotes",
            self.metadata,
            Column("quote_id", String(64), primary_key=True),
            Column("venue", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("market_kind", Text, nullable=False),
            Column("symbol", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("source", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_cycle_history_asset_observed", self.quotes.c.asset, self.quotes.c.observed_at)
        self.metadata.create_all(self.store.engine)
        self._replay_cache_key: tuple[int, str] | None = None
        self._replay_cache: dict[tuple[str, str], CycleReplaySummary] = {}

    async def _get(self, path: str, *, params: dict[str, object]) -> object:
        """Coinbase request retained as the primary historical source."""
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/cycle-history",
                "Cache-Control": "no-cache",
            },
        )
        try:
            response = await client.get(f"{COINBASE_EXCHANGE_URL}{path}", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    async def _get_okx(self, *, params: dict[str, object]) -> object:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/cycle-history",
                "Cache-Control": "no-cache",
            },
        )
        try:
            response = await client.get(f"{OKX_URL}/api/v5/market/history-candles", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    async def _get_bybit(self, *, params: dict[str, object]) -> object:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/cycle-history",
                "Cache-Control": "no-cache",
            },
        )
        try:
            response = await client.get(f"{BYBIT_URL}/v5/market/kline", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    @staticmethod
    def _coingecko_client_config() -> tuple[str, dict[str, str]]:
        pro_key = os.getenv("CIE_COINGECKO_API_KEY", "").strip()
        demo_key = os.getenv("CIE_COINGECKO_DEMO_API_KEY", "").strip()
        if pro_key:
            return COINGECKO_PRO_API_URL, {"x-cg-pro-api-key": pro_key}
        if demo_key:
            return COINGECKO_PUBLIC_API_URL, {"x-cg-demo-api-key": demo_key}
        return COINGECKO_PUBLIC_API_URL, {}

    async def _get_coingecko(
        self,
        *,
        coin_id: str,
        params: dict[str, object],
    ) -> object:
        base_url, auth_headers = self._coingecko_client_config()
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": "crypto-inefficiency-engine/cycle-history",
                "Cache-Control": "no-cache",
                **auth_headers,
            },
        )
        try:
            response = await client.get(
                f"{base_url}/coins/{coin_id}/market_chart/range",
                params=params,
                headers=auth_headers or None,
            )
            response.raise_for_status()
            return response.json()
        finally:
            if owns_client:
                await client.aclose()

    @staticmethod
    def _parse_candles(payload: object, *, asset: str) -> list[MarketQuote]:
        if not isinstance(payload, list):
            raise ValueError("Coinbase candle response must be a list")
        rows: list[MarketQuote] = []
        symbol = f"{asset.upper()}-USD"
        for item in payload:
            if not isinstance(item, list) or len(item) < 5:
                continue
            try:
                observed_at = datetime.fromtimestamp(float(item[0]), tz=timezone.utc)
                close = float(item[4])
            except (TypeError, ValueError, OverflowError):
                continue
            if close <= 0:
                continue
            rows.append(
                MarketQuote(
                    venue="Coinbase",
                    asset=asset.upper(),
                    market_kind=MarketKind.SPOT,
                    symbol=symbol,
                    quote_currency="USD",
                    contract_key="spot",
                    mid=close,
                    observed_at=observed_at,
                    source="coinbase-exchange:candles:6h:historical-backfill",
                )
            )
        rows.sort(key=lambda item: item.observed_at)
        return rows

    @staticmethod
    def _parse_okx_candles(payload: object, *, asset: str) -> list[MarketQuote]:
        if not isinstance(payload, dict) or str(payload.get("code")) != "0":
            raise ValueError("OKX history-candles response did not return code=0")
        raw_rows = payload.get("data")
        if not isinstance(raw_rows, list):
            raise ValueError("OKX history-candles response must contain data")
        rows: list[MarketQuote] = []
        symbol = f"{asset.upper()}-USDT"
        for item in raw_rows:
            if not isinstance(item, list) or len(item) < 5:
                continue
            if len(item) >= 9 and str(item[8]) == "0":
                continue
            try:
                observed_at = datetime.fromtimestamp(float(item[0]) / 1000.0, tz=timezone.utc)
                close = float(item[4])
            except (TypeError, ValueError, OverflowError):
                continue
            if close <= 0:
                continue
            rows.append(
                MarketQuote(
                    venue="OKX",
                    asset=asset.upper(),
                    market_kind=MarketKind.SPOT,
                    symbol=symbol,
                    quote_currency="USDT",
                    contract_key="spot",
                    mid=close,
                    observed_at=observed_at,
                    source="okx-v5:market:history-candles:spot:6h-utc:historical-backfill",
                )
            )
        rows.sort(key=lambda item: item.observed_at)
        return rows

    @staticmethod
    def _parse_bybit_candles(payload: object, *, asset: str) -> list[MarketQuote]:
        if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
            raise ValueError("Bybit kline response did not return retCode=0")
        result = payload.get("result")
        raw_rows = result.get("list") if isinstance(result, dict) else None
        if not isinstance(raw_rows, list):
            raise ValueError("Bybit kline response must contain result.list")
        rows: list[MarketQuote] = []
        symbol = f"{asset.upper()}USDT"
        for item in raw_rows:
            if not isinstance(item, list) or len(item) < 5:
                continue
            try:
                observed_at = datetime.fromtimestamp(float(item[0]) / 1000.0, tz=timezone.utc)
                close = float(item[4])
            except (TypeError, ValueError, OverflowError):
                continue
            if close <= 0:
                continue
            rows.append(
                MarketQuote(
                    venue="Bybit",
                    asset=asset.upper(),
                    market_kind=MarketKind.SPOT,
                    symbol=symbol,
                    quote_currency="USDT",
                    contract_key="spot",
                    mid=close,
                    observed_at=observed_at,
                    source="bybit-v5:kline:spot:6h:historical-backfill",
                )
            )
        rows.sort(key=lambda item: item.observed_at)
        return rows

    @staticmethod
    def _parse_coingecko_prices(
        payload: object,
        *,
        asset: str,
        coin_id: str,
    ) -> list[MarketQuote]:
        if not isinstance(payload, dict):
            raise ValueError("CoinGecko market-chart response must be an object")
        raw_prices = payload.get("prices")
        if not isinstance(raw_prices, list):
            raise ValueError("CoinGecko market-chart response must contain prices")

        # CoinGecko supplies hourly observations for sub-90-day range requests.
        # Collapse them deterministically to one UTC-aligned six-hour reference
        # observation so this source is comparable with exchange 6h candles.
        buckets: dict[int, tuple[float, float]] = {}
        for item in raw_prices:
            if not isinstance(item, list) or len(item) < 2:
                continue
            try:
                raw_ms = float(item[0])
                price = float(item[1])
            except (TypeError, ValueError, OverflowError):
                continue
            if raw_ms <= 0 or price <= 0:
                continue
            raw_seconds = raw_ms / 1000.0
            bucket = int(raw_seconds // HISTORICAL_CANDLE_SECONDS) * HISTORICAL_CANDLE_SECONDS
            current = buckets.get(bucket)
            if current is None or raw_ms > current[0]:
                buckets[bucket] = (raw_ms, price)

        rows = [
            MarketQuote(
                venue="CoinGecko",
                asset=asset.upper(),
                market_kind=MarketKind.SPOT,
                symbol=coin_id,
                quote_currency="USD",
                contract_key="spot-reference",
                mid=price,
                observed_at=datetime.fromtimestamp(bucket, tz=timezone.utc),
                source=(
                    f"coingecko:coins/{coin_id}/market_chart/range:hourly-downsampled-6h:"
                    "historical-backfill"
                ),
            )
            for bucket, (_raw_ms, price) in buckets.items()
        ]
        rows.sort(key=lambda item: item.observed_at)
        return rows

    def record_quotes(self, quotes: Iterable[MarketQuote]) -> int:
        rows = []
        for quote in quotes:
            if quote.market_kind != MarketKind.SPOT or quote.venue not in HISTORICAL_PROVIDER_VENUES:
                continue
            rows.append(
                {
                    "quote_id": _quote_id(quote),
                    "venue": quote.venue,
                    "asset": quote.asset.upper(),
                    "market_kind": quote.market_kind.value,
                    "symbol": quote.symbol,
                    "observed_at": _iso(quote.observed_at),
                    "source": quote.source,
                    "payload_json": quote.model_dump_json(),
                }
            )
        if not rows:
            return 0
        backend = self.store.engine.url.get_backend_name()
        if backend == "postgresql":
            statement = pg_insert(self.quotes).values(rows).on_conflict_do_nothing(
                index_elements=[self.quotes.c.quote_id]
            )
        elif backend == "sqlite":
            statement = sqlite_insert(self.quotes).values(rows).on_conflict_do_nothing(
                index_elements=[self.quotes.c.quote_id]
            )
        else:
            with self.store.engine.connect() as db:
                existing = set(db.execute(select(self.quotes.c.quote_id)).scalars())
            rows = [row for row in rows if str(row["quote_id"]) not in existing]
            if not rows:
                return 0
            statement = insert(self.quotes).values(rows)
        with self.store.engine.begin() as db:
            result = db.execute(statement)
        self._replay_cache_key = None
        return max(0, int(result.rowcount or 0))

    def _provider_coverage_rows(
        self,
        assets: Iterable[str] | None = None,
    ) -> list[tuple[str, str, int, datetime, datetime]]:
        query = select(
            self.quotes.c.asset,
            self.quotes.c.venue,
            func.count(self.quotes.c.quote_id),
            func.min(self.quotes.c.observed_at),
            func.max(self.quotes.c.observed_at),
        ).group_by(self.quotes.c.asset, self.quotes.c.venue)
        allowed = tuple(sorted({str(asset).upper() for asset in (assets or ()) if str(asset).strip()}))
        if allowed:
            query = query.where(self.quotes.c.asset.in_(allowed))
        with self.store.engine.connect() as db:
            raw_rows = list(db.execute(query).all())
        rows: list[tuple[str, str, int, datetime, datetime]] = []
        for asset, venue, count, earliest, latest in raw_rows:
            if not earliest or not latest:
                continue
            rows.append(
                (
                    str(asset).upper(),
                    str(venue),
                    int(count or 0),
                    datetime.fromisoformat(str(earliest)),
                    datetime.fromisoformat(str(latest)),
                )
            )
        return rows

    def _preferred_venue_by_asset(
        self,
        assets: Iterable[str] | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, str]:
        rows = self._provider_coverage_rows(assets)
        expected = (
            max(1, int((end - start).total_seconds() // HISTORICAL_CANDLE_SECONDS))
            if start is not None and end is not None and end > start
            else None
        )
        best: dict[str, tuple[tuple[float, ...], str]] = {}
        for asset, venue, count, earliest, latest in rows:
            if start is not None and end is not None:
                complete = bool(
                    expected is not None
                    and count >= int(expected * 0.90)
                    and earliest <= start + timedelta(days=3)
                    and latest >= end - timedelta(days=3)
                )
                covered_start = max(earliest, start)
                covered_end = min(latest, end)
                span = max(0.0, (covered_end - covered_start).total_seconds())
                score = (
                    1.0 if complete else 0.0,
                    span,
                    float(count),
                    float(_PROVIDER_PRIORITY.get(venue, 0)),
                )
            else:
                span = max(0.0, (latest - earliest).total_seconds())
                score = (
                    span,
                    float(count),
                    float(_PROVIDER_PRIORITY.get(venue, 0)),
                )
            current = best.get(asset)
            if current is None or score > current[0]:
                best[asset] = (score, venue)
        return {asset: venue for asset, (_score, venue) in best.items()}

    def preferred_venue(
        self,
        asset: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> str | None:
        return self._preferred_venue_by_asset((asset,), start=start, end=end).get(asset.upper())

    def _coverage(
        self,
        asset: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[int, datetime | None, datetime | None]:
        preferred = self.preferred_venue(asset, start=start, end=end)
        if preferred is None:
            return 0, None, None
        for row_asset, venue, count, earliest, latest in self._provider_coverage_rows((asset,)):
            if row_asset == asset.upper() and venue == preferred:
                return count, earliest, latest
        return 0, None, None

    @staticmethod
    def _sufficient_coverage(
        count: int,
        earliest: datetime | None,
        latest: datetime | None,
        *,
        expected_points: int,
        start: datetime,
        end: datetime,
    ) -> bool:
        return bool(
            count >= int(expected_points * 0.90)
            and earliest is not None
            and latest is not None
            and earliest <= start + timedelta(days=3)
            and latest >= end - timedelta(days=3)
        )

    async def _fetch_coinbase_history(
        self,
        *,
        asset: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[MarketQuote], str | None]:
        rows: list[MarketQuote] = []
        cursor = start
        while cursor < end:
            chunk_end = min(end, cursor + timedelta(days=HISTORICAL_REQUEST_CHUNK_DAYS))
            try:
                payload = await self._get(
                    f"/products/{asset}-USD/candles",
                    params={
                        "granularity": HISTORICAL_CANDLE_SECONDS,
                        "start": _iso(cursor),
                        "end": _iso(chunk_end),
                    },
                )
                rows.extend(self._parse_candles(payload, asset=asset))
            except httpx.HTTPStatusError as exc:
                label = "NotListed" if exc.response.status_code == 404 else f"HTTP{exc.response.status_code}"
                return rows, label
            except Exception as exc:
                return rows, type(exc).__name__
            cursor = chunk_end
        return rows, None

    async def _fetch_okx_history(
        self,
        *,
        asset: str,
        start: datetime,
        end: datetime,
    ) -> list[MarketQuote]:
        start_ms = int(start.timestamp() * 1000)
        cursor_ms = int(end.timestamp() * 1000)
        rows: dict[str, MarketQuote] = {}
        for _page in range(HISTORICAL_OKX_MAX_PAGES):
            if cursor_ms <= start_ms:
                break
            payload = await self._get_okx(
                params={
                    "instId": f"{asset.upper()}-USDT",
                    "bar": "6Hutc",
                    "after": str(cursor_ms),
                    "limit": "100",
                }
            )
            page = [
                quote
                for quote in self._parse_okx_candles(payload, asset=asset)
                if start <= quote.observed_at < end
            ]
            if not page:
                break
            for quote in page:
                rows[_iso(quote.observed_at)] = quote
            oldest_ms = int(min(quote.observed_at for quote in page).timestamp() * 1000)
            if oldest_ms >= cursor_ms:
                break
            cursor_ms = oldest_ms
            if cursor_ms <= start_ms:
                break
        return sorted(rows.values(), key=lambda item: item.observed_at)

    async def _fetch_bybit_history(
        self,
        *,
        asset: str,
        start: datetime,
        end: datetime,
    ) -> list[MarketQuote]:
        rows: list[MarketQuote] = []
        cursor = start
        while cursor < end:
            chunk_end = min(end, cursor + timedelta(days=HISTORICAL_REQUEST_CHUNK_DAYS))
            payload = await self._get_bybit(
                params={
                    "category": "spot",
                    "symbol": f"{asset.upper()}USDT",
                    "interval": "360",
                    "start": int(cursor.timestamp() * 1000),
                    "end": int(chunk_end.timestamp() * 1000),
                    "limit": 1000,
                }
            )
            rows.extend(self._parse_bybit_candles(payload, asset=asset))
            cursor = chunk_end
        return rows

    def _coingecko_asset_id(self, asset: str) -> str | None:
        try:
            from inefficiency_engine.volume_universe import read_latest_volume_universe

            snapshot = read_latest_volume_universe(self.store)
        except Exception:
            return None
        rows = snapshot.get("assets") if isinstance(snapshot, dict) else None
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("asset") or "").upper() != asset.upper():
                continue
            coin_id = str(row.get("source_asset_id") or "").strip()
            return coin_id or None
        return None

    async def _fetch_coingecko_history(
        self,
        *,
        asset: str,
        start: datetime,
        end: datetime,
    ) -> list[MarketQuote]:
        coin_id = self._coingecko_asset_id(asset)
        if not coin_id:
            raise LookupError("CoinGecko source_asset_id is unavailable")
        rows: dict[str, MarketQuote] = {}
        cursor = start
        while cursor < end:
            chunk_end = min(end, cursor + timedelta(days=HISTORICAL_COINGECKO_CHUNK_DAYS))
            payload = await self._get_coingecko(
                coin_id=coin_id,
                params={
                    "vs_currency": "usd",
                    "from": int(cursor.timestamp()),
                    "to": int(chunk_end.timestamp()),
                    "precision": "full",
                },
            )
            for quote in self._parse_coingecko_prices(payload, asset=asset, coin_id=coin_id):
                if start <= quote.observed_at < end:
                    rows[_iso(quote.observed_at)] = quote
            cursor = chunk_end
        return sorted(rows.values(), key=lambda item: item.observed_at)

    @staticmethod
    def _provider_error_label(provider: str, exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            return f"{provider}:HTTP{exc.response.status_code}"
        if isinstance(exc, LookupError):
            return f"{provider}:MissingAssetId"
        return f"{provider}:{type(exc).__name__}"

    async def ensure_backfilled(
        self,
        assets: Iterable[str],
        *,
        now: datetime | None = None,
    ) -> HistoricalBackfillReport:
        now = now or datetime.now(timezone.utc)
        end = _utc_day_start(now)
        start = end - timedelta(days=self.backfill_days)
        requested = tuple(sorted({str(asset).upper() for asset in assets if str(asset).strip()}))
        fetched: set[str] = set()
        errors: list[str] = []
        provider_diagnostics: dict[str, tuple[str, ...]] = {}
        stored = 0
        expected_points = self.backfill_days * (86400 // HISTORICAL_CANDLE_SECONDS)

        for asset in requested:
            count, earliest, latest = self._coverage(asset, start=start, end=end)
            if self._sufficient_coverage(
                count,
                earliest,
                latest,
                expected_points=expected_points,
                start=start,
                end=end,
            ):
                continue

            attempts: list[str] = []

            coinbase_rows, coinbase_error = await self._fetch_coinbase_history(
                asset=asset,
                start=start,
                end=end,
            )
            if coinbase_rows:
                stored += self.record_quotes(coinbase_rows)
                fetched.add(asset)
                attempts.append(f"Coinbase:rows={len(coinbase_rows)}")
            if coinbase_error:
                attempts.append(f"Coinbase:{coinbase_error}")

            count, earliest, latest = self._coverage(asset, start=start, end=end)
            complete = self._sufficient_coverage(
                count,
                earliest,
                latest,
                expected_points=expected_points,
                start=start,
                end=end,
            )

            providers = (
                ("OKX", self._fetch_okx_history),
                ("Bybit", self._fetch_bybit_history),
                ("CoinGecko", self._fetch_coingecko_history),
            )
            for provider, fetcher in providers:
                if complete:
                    break
                try:
                    provider_rows = await fetcher(asset=asset, start=start, end=end)
                    attempts.append(f"{provider}:rows={len(provider_rows)}")
                    if provider_rows:
                        stored += self.record_quotes(provider_rows)
                        fetched.add(asset)
                except Exception as exc:
                    attempts.append(self._provider_error_label(provider, exc))
                    continue

                count, earliest, latest = self._coverage(asset, start=start, end=end)
                complete = self._sufficient_coverage(
                    count,
                    earliest,
                    latest,
                    expected_points=expected_points,
                    start=start,
                    end=end,
                )

            provider_diagnostics[asset] = tuple(attempts)
            count, earliest, latest = self._coverage(asset, start=start, end=end)
            if self._sufficient_coverage(
                count,
                earliest,
                latest,
                expected_points=expected_points,
                start=start,
                end=end,
            ):
                continue

            blocked = any(":HTTP403" in item for item in attempts)
            if count > 0 and earliest is not None and latest is not None:
                if earliest > start + timedelta(days=3) and latest >= end - timedelta(days=3):
                    reason = "InsufficientAssetAge"
                else:
                    reason = "PartialProviderHistory"
            elif blocked:
                reason = "ProviderBlocked"
            elif attempts and all(
                ("NotListed" in item or "rows=0" in item or "MissingAssetId" in item)
                for item in attempts
            ):
                reason = "NotListed"
            else:
                reason = "ProviderUnavailable"
            errors.append(f"{asset}:{reason}")

        with self.store.engine.connect() as db:
            observed = list(
                db.execute(select(self.quotes.c.observed_at).order_by(self.quotes.c.observed_at)).scalars()
            )
        return HistoricalBackfillReport(
            requested_assets=requested,
            fetched_assets=tuple(sorted(fetched)),
            stored_quote_count=stored,
            total_quote_count=len(observed),
            earliest_observed_at=datetime.fromisoformat(observed[0]) if observed else None,
            latest_observed_at=datetime.fromisoformat(observed[-1]) if observed else None,
            errors=tuple(errors),
            provider_diagnostics=provider_diagnostics,
        )

    def history(
        self,
        *,
        start: datetime,
        end: datetime,
        assets: Iterable[str] | None = None,
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        allowed = {str(item).upper() for item in assets} if assets is not None else None
        preferred = self._preferred_venue_by_asset(allowed, start=start, end=end)
        query = (
            select(self.quotes.c.payload_json)
            .where(self.quotes.c.observed_at >= _iso(start))
            .where(self.quotes.c.observed_at <= _iso(end))
            .order_by(self.quotes.c.observed_at)
        )
        if allowed:
            query = query.where(self.quotes.c.asset.in_(tuple(sorted(allowed))))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            asset = quote.asset.upper()
            if allowed is not None and asset not in allowed:
                continue
            if preferred.get(asset) not in (None, quote.venue):
                continue
            key = (quote.venue, asset, quote.market_kind)
            grouped.setdefault(key, []).append(quote)
        return grouped

    @staticmethod
    def _exit_quote(series: list[MarketQuote], due_at: datetime) -> MarketQuote | None:
        timestamps = [item.observed_at for item in series]
        index = bisect_left(timestamps, due_at)
        if index >= len(series):
            return None
        quote = series[index]
        if quote.observed_at - due_at > timedelta(hours=12):
            return None
        return quote

    def _filter_preferred_quotes(
        self,
        quotes: list[MarketQuote],
        *,
        start: datetime,
        end: datetime,
    ) -> list[MarketQuote]:
        preferred = self._preferred_venue_by_asset(
            {quote.asset.upper() for quote in quotes},
            start=start,
            end=end,
        )
        return [
            quote
            for quote in quotes
            if preferred.get(quote.asset.upper()) in (None, quote.venue)
        ]

    @staticmethod
    def _historical_spot_taker_fee_bps(settings, venue: str) -> float:
        if venue == "Coinbase":
            return settings.coinbase_spot_taker_fee_bps
        if venue == "OKX":
            return settings.okx_spot_taker_fee_bps
        if venue == "Bybit":
            return settings.bybit_spot_taker_fee_bps
        if venue == "Kraken":
            return settings.kraken_spot_taker_fee_bps
        # CoinGecko is reference pricing, not an execution venue. Charge the most
        # conservative configured spot taker fee rather than granting free execution.
        return max(
            settings.coinbase_spot_taker_fee_bps,
            settings.okx_spot_taker_fee_bps,
            settings.bybit_spot_taker_fee_bps,
            settings.kraken_spot_taker_fee_bps,
        )

    def replay_summaries(
        self,
        strategy,
        settings,
        *,
        total_capital_usd: float,
        now: datetime | None = None,
    ) -> dict[tuple[str, str], CycleReplaySummary]:
        now = now or datetime.now(timezone.utc)
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(select(self.quotes.c.payload_json).order_by(self.quotes.c.observed_at)).scalars()
            )
        if not payloads:
            return {}
        quotes = [MarketQuote.model_validate_json(payload) for payload in payloads]
        target_end = _utc_day_start(now)
        target_start = target_end - timedelta(days=self.backfill_days)
        quotes = self._filter_preferred_quotes(quotes, start=target_start, end=target_end)
        if not quotes:
            return {}
        latest = max(item.observed_at for item in quotes)
        preferred_key = ",".join(
            f"{asset}:{venue}"
            for asset, venue in sorted(
                self._preferred_venue_by_asset(
                    {quote.asset.upper() for quote in quotes},
                    start=target_start,
                    end=target_end,
                ).items()
            )
        )
        cache_key = (len(quotes), f"{latest.isoformat()}|{preferred_key}")
        if self._replay_cache_key == cache_key:
            return dict(self._replay_cache)

        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
        for quote in quotes:
            grouped.setdefault((quote.venue, quote.asset.upper(), quote.market_kind), []).append(quote)
        for series in grouped.values():
            series.sort(key=lambda item: item.observed_at)

        earliest = min(item.observed_at for item in quotes)
        evaluation = earliest + timedelta(hours=strategy.required_history_hours(settings))
        replay_end = min(latest, now) - timedelta(hours=strategy.horizon_hours)
        outcomes: dict[tuple[str, str], list[tuple[float, str]]] = {}

        while evaluation <= replay_end:
            current: list[MarketQuote] = []
            point_history: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
            for key, series in grouped.items():
                timestamps = [item.observed_at for item in series]
                index = bisect_right(timestamps, evaluation)
                if index <= 0:
                    continue
                visible = series[:index]
                latest_visible = visible[-1]
                if evaluation - latest_visible.observed_at > timedelta(hours=12):
                    continue
                current.append(latest_visible)
                point_history[key] = visible
            if current:
                snapshot = ScanSnapshot(
                    scan_id=f"historical-replay-{int(evaluation.timestamp())}",
                    started_at=evaluation,
                    completed_at=evaluation,
                    providers=[],
                    funding_quotes=[],
                    market_quotes=current,
                    opportunities=[],
                    order_books=[],
                    executability=[],
                    analysis_config={"historical_walk_forward": True},
                )
                candidates = strategy.discover(
                    snapshot,
                    point_history,
                    settings,
                    total_capital_usd=total_capital_usd,
                )
                for candidate in candidates:
                    series = grouped.get(
                        (candidate.venue, candidate.asset.upper(), candidate.market_kind), []
                    )
                    exit_quote = self._exit_quote(
                        series,
                        candidate.observed_at + timedelta(hours=candidate.horizon_hours),
                    )
                    if exit_quote is None or candidate.entry_reference_price <= 0:
                        continue
                    raw = exit_quote.mid / candidate.entry_reference_price - 1.0
                    directional = raw if candidate.direction == "long" else -raw
                    fee_floor = 0.0
                    if candidate.market_kind == MarketKind.SPOT:
                        taker_fee_bps = self._historical_spot_taker_fee_bps(
                            settings,
                            candidate.venue,
                        )
                        fee_floor = (
                            2.0 * taker_fee_bps + settings.alpha_execution_risk_floor_bps
                        ) / 10_000.0
                    cost = max(candidate.estimated_cost_return, fee_floor)
                    outcomes.setdefault((candidate.asset.upper(), candidate.direction), []).append(
                        (directional - cost, candidate.regime)
                    )
            evaluation += timedelta(hours=HISTORICAL_REPLAY_STEP_HOURS)

        summaries: dict[tuple[str, str], CycleReplaySummary] = {}
        for key, rows in outcomes.items():
            values = [value for value, _ in rows]
            positive = sum(value > 0 for value in values)
            regime_values: dict[str, list[float]] = {}
            for value, regime in rows:
                regime_values.setdefault(regime, []).append(value)
            regime_means = {
                regime: statistics.fmean(regime_rows)
                for regime, regime_rows in regime_values.items()
                if regime_rows
            }
            mean = statistics.fmean(values) if values else None
            hit = positive / len(values) if values else None
            qualified = (
                len(values) >= HISTORICAL_REPLAY_MIN_SAMPLES
                and mean is not None
                and mean > settings.alpha_min_forward_mean_return
                and hit is not None
                and hit >= HISTORICAL_REPLAY_MIN_HIT_RATE
                and len(regime_means) >= settings.alpha_min_regimes
                and all(
                    value > settings.alpha_min_regime_mean_return
                    for value in regime_means.values()
                )
            )
            summaries[key] = CycleReplaySummary(
                strategy_id=strategy.strategy_id,
                asset=key[0],
                direction=key[1],
                sample_count=len(values),
                positive_count=positive,
                hit_rate=hit,
                mean_realized_net_return=mean,
                regime_count=len(regime_means),
                regime_means=regime_means,
                qualified_for_probationary_support=qualified,
            )

        self._replay_cache_key = cache_key
        self._replay_cache = summaries
        return dict(summaries)