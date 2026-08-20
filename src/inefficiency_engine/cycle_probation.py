from __future__ import annotations

import hashlib
import statistics
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx
from sqlalchemy import Column, Index, MetaData, String, Table, Text, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


COINBASE_EXCHANGE_URL = "https://api.exchange.coinbase.com"
HISTORICAL_BACKFILL_DAYS = 365
HISTORICAL_CANDLE_SECONDS = 21600
HISTORICAL_REQUEST_CHUNK_DAYS = 60
HISTORICAL_REPLAY_STEP_HOURS = 72.0
HISTORICAL_REPLAY_MIN_SAMPLES = 24
HISTORICAL_REPLAY_MIN_HIT_RATE = 0.55
PROBATIONARY_FORWARD_MIN_SAMPLES = 8
PROBATIONARY_FORWARD_MIN_HIT_RATE = 0.55
PROBATIONARY_MAX_CANDIDATE_FRACTION = 0.02
PROBATIONARY_MAX_TOTAL_FRACTION = 0.05
PROBATIONARY_FORWARD_MEAN_HAIRCUT = 0.50
PROBATIONARY_HISTORICAL_MEAN_HAIRCUT = 0.35


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

    def record_quotes(self, quotes: Iterable[MarketQuote]) -> int:
        rows = []
        for quote in quotes:
            if quote.market_kind != MarketKind.SPOT or quote.venue != "Coinbase":
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

    def _coverage(self, asset: str) -> tuple[int, datetime | None, datetime | None]:
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.quotes.c.observed_at)
                    .where(self.quotes.c.asset == asset.upper())
                    .order_by(self.quotes.c.observed_at)
                ).scalars()
            )
        if not payloads:
            return 0, None, None
        return len(payloads), datetime.fromisoformat(payloads[0]), datetime.fromisoformat(payloads[-1])

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
        fetched: list[str] = []
        errors: list[str] = []
        stored = 0
        expected_points = self.backfill_days * (86400 // HISTORICAL_CANDLE_SECONDS)

        for asset in requested:
            count, earliest, latest = self._coverage(asset)
            sufficiently_covered = (
                count >= int(expected_points * 0.90)
                and earliest is not None
                and latest is not None
                and earliest <= start + timedelta(days=3)
                and latest >= end - timedelta(days=3)
            )
            if sufficiently_covered:
                continue

            asset_rows: list[MarketQuote] = []
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
                    asset_rows.extend(self._parse_candles(payload, asset=asset))
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        errors.append(f"{asset}:NotListed")
                        break
                    errors.append(f"{asset}:HTTP{exc.response.status_code}")
                    break
                except Exception as exc:
                    errors.append(f"{asset}:{type(exc).__name__}")
                    break
                cursor = chunk_end

            if asset_rows:
                stored += self.record_quotes(asset_rows)
                fetched.append(asset)

        with self.store.engine.connect() as db:
            observed = list(
                db.execute(select(self.quotes.c.observed_at).order_by(self.quotes.c.observed_at)).scalars()
            )
        return HistoricalBackfillReport(
            requested_assets=requested,
            fetched_assets=tuple(fetched),
            stored_quote_count=stored,
            total_quote_count=len(observed),
            earliest_observed_at=datetime.fromisoformat(observed[0]) if observed else None,
            latest_observed_at=datetime.fromisoformat(observed[-1]) if observed else None,
            errors=tuple(errors),
        )

    def history(
        self,
        *,
        start: datetime,
        end: datetime,
        assets: Iterable[str] | None = None,
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        query = (
            select(self.quotes.c.payload_json)
            .where(self.quotes.c.observed_at >= _iso(start))
            .where(self.quotes.c.observed_at <= _iso(end))
            .order_by(self.quotes.c.observed_at)
        )
        allowed = {item.upper() for item in assets} if assets is not None else None
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
        for payload in payloads:
            quote = MarketQuote.model_validate_json(payload)
            if allowed is not None and quote.asset.upper() not in allowed:
                continue
            key = (quote.venue, quote.asset.upper(), quote.market_kind)
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
        latest = max(item.observed_at for item in quotes)
        cache_key = (len(quotes), latest.isoformat())
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
                    if candidate.venue == "Coinbase" and candidate.market_kind == MarketKind.SPOT:
                        fee_floor = (
                            2.0 * settings.coinbase_spot_taker_fee_bps
                            + settings.alpha_execution_risk_floor_bps
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
