from __future__ import annotations

import statistics
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import func, select

from inefficiency_engine.cycle_probation import (
    HISTORICAL_REPLAY_MIN_HIT_RATE,
    HISTORICAL_REPLAY_MIN_SAMPLES,
    HISTORICAL_REPLAY_STEP_HOURS,
    CycleHistoricalResearch,
    CycleReplaySummary,
    _iso,
)
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote


class BatchedCycleHistoricalResearch(CycleHistoricalResearch):
    """Cycle history projection that never materializes the full 40-asset archive.

    Backfill remains append-only and unchanged. Coverage, history reads and historical
    walk-forward replay are filtered in SQL to the current small asset batch so the
    history subprocess has a peak working set tied to batch size rather than universe
    size.
    """

    def __init__(self, *args, active_assets: Iterable[str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_assets = tuple(
            sorted({str(asset).upper() for asset in (active_assets or ()) if str(asset).strip()})
        )

    def set_active_assets(self, assets: Iterable[str]) -> None:
        self.active_assets = tuple(
            sorted({str(asset).upper() for asset in assets if str(asset).strip()})
        )
        self._replay_cache_key = None
        self._replay_cache = {}

    def _coverage(self, asset: str) -> tuple[int, datetime | None, datetime | None]:
        with self.store.engine.connect() as db:
            count, earliest, latest = db.execute(
                select(
                    func.count(self.quotes.c.quote_id),
                    func.min(self.quotes.c.observed_at),
                    func.max(self.quotes.c.observed_at),
                ).where(self.quotes.c.asset == asset.upper())
            ).one()
        return (
            int(count or 0),
            datetime.fromisoformat(str(earliest)) if earliest else None,
            datetime.fromisoformat(str(latest)) if latest else None,
        )

    def history(
        self,
        *,
        start: datetime,
        end: datetime,
        assets: Iterable[str] | None = None,
    ) -> dict[tuple[str, str, MarketKind], list[MarketQuote]]:
        allowed = tuple(
            sorted({str(item).upper() for item in (assets or self.active_assets) if str(item).strip()})
        )
        query = (
            select(self.quotes.c.payload_json)
            .where(self.quotes.c.observed_at >= _iso(start))
            .where(self.quotes.c.observed_at <= _iso(end))
            .order_by(self.quotes.c.observed_at)
        )
        if allowed:
            query = query.where(self.quotes.c.asset.in_(allowed))
        with self.store.engine.connect() as db:
            payloads = db.execution_options(stream_results=True).execute(query).scalars()
            grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
            for payload in payloads:
                quote = MarketQuote.model_validate_json(payload)
                key = (quote.venue, quote.asset.upper(), quote.market_kind)
                grouped.setdefault(key, []).append(quote)
        return grouped

    def replay_summaries(
        self,
        strategy,
        settings,
        *,
        total_capital_usd: float,
        now: datetime | None = None,
    ) -> dict[tuple[str, str], CycleReplaySummary]:
        now = now or datetime.now(timezone.utc)
        allowed = self.active_assets
        query = select(self.quotes.c.payload_json).order_by(self.quotes.c.observed_at)
        if allowed:
            query = query.where(self.quotes.c.asset.in_(allowed))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())
        if not payloads:
            return {}

        quotes = [MarketQuote.model_validate_json(payload) for payload in payloads]
        latest = max(item.observed_at for item in quotes)
        cache_key = (len(quotes), f"{latest.isoformat()}|{','.join(allowed)}")
        if self._replay_cache_key == cache_key:
            return dict(self._replay_cache)

        grouped: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
        timestamps: dict[tuple[str, str, MarketKind], list[datetime]] = {}
        for quote in quotes:
            grouped.setdefault((quote.venue, quote.asset.upper(), quote.market_kind), []).append(quote)
        for key, series in grouped.items():
            series.sort(key=lambda item: item.observed_at)
            timestamps[key] = [item.observed_at for item in series]

        earliest = min(item.observed_at for item in quotes)
        evaluation = earliest + timedelta(hours=strategy.required_history_hours(settings))
        replay_end = min(latest, now) - timedelta(hours=strategy.horizon_hours)
        outcomes: dict[tuple[str, str], list[tuple[float, str]]] = {}

        while evaluation <= replay_end:
            current: list[MarketQuote] = []
            point_history: dict[tuple[str, str, MarketKind], list[MarketQuote]] = {}
            for key, series in grouped.items():
                index = bisect_right(timestamps[key], evaluation)
                if index <= 0:
                    continue
                latest_visible = series[index - 1]
                if evaluation - latest_visible.observed_at > timedelta(hours=12):
                    continue
                current.append(latest_visible)
                point_history[key] = series[:index]
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
                    analysis_config={"historical_walk_forward": True, "batched": True},
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
                        if candidate.venue == "Coinbase":
                            taker_fee_bps = settings.coinbase_spot_taker_fee_bps
                        elif candidate.venue == "Bybit":
                            taker_fee_bps = settings.bybit_spot_taker_fee_bps
                        else:
                            taker_fee_bps = 0.0
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
