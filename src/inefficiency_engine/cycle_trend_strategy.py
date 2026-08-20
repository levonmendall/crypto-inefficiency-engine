from __future__ import annotations

import math
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaDirection, AlphaStrategyManifest
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import ScanSnapshot
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote


_HALVINGS = (
    datetime(2012, 11, 28, tzinfo=timezone.utc),
    datetime(2016, 7, 9, tzinfo=timezone.utc),
    datetime(2020, 5, 11, tzinfo=timezone.utc),
    datetime(2024, 4, 20, tzinfo=timezone.utc),
)


class CycleAwareMultiHorizonTrendStrategy:
    """Slow directional alpha with a bounded halving-cycle prior, never a trigger."""

    strategy_id = "cycle_aware_multi_horizon_trend_v1"
    family = "directional_cycle_trend"
    lookbacks = (168.0, 720.0, 2160.0, 4320.0)  # 7/30/90/180d
    horizon_hours = 72.0
    history_refresh_hours = 6.0
    history_rows_per_day = 8

    manifest = AlphaStrategyManifest(
        strategy_id=strategy_id,
        family=family,
        description=(
            "7/30/90/180-day trend with BTC/breadth confirmation and a "
            "Bitcoin-halving-cycle prior capped at ten percent."
        ),
        predictive=True,
        horizons_hours=[horizon_hours],
    )

    @classmethod
    def required_history_hours(cls, _settings) -> float:
        return max(cls.lookbacks)

    @classmethod
    def history_refresh(cls, _settings) -> float:
        return cls.history_refresh_hours

    @classmethod
    def rows_per_day(cls, _settings) -> int:
        return cls.history_rows_per_day

    @staticmethod
    def _cycle_context(at: datetime) -> tuple[str, float, float, float]:
        cycle_days = statistics.median(
            (b - a).total_seconds() / 86400.0
            for a, b in zip(_HALVINGS, _HALVINGS[1:])
        )
        last = max((item for item in _HALVINGS if item <= at), default=None)
        if last is None:
            return "unknown", 0.0, 0.0, cycle_days
        raw = max(0.0, (at - last).total_seconds() / 86400.0 / cycle_days)
        progress = raw if raw <= 1.0 else raw % 1.0
        if progress < 0.10:
            return "early_post_halving", progress, 0.35, cycle_days
        if progress < 0.55:
            return "expansion", progress, 1.0, cycle_days
        if progress < 0.72:
            return "late_cycle", progress, 0.15, cycle_days
        if progress < 0.90:
            return "contraction_risk", progress, -0.75, cycle_days
        return "pre_halving", progress, 0.25, cycle_days

    @staticmethod
    def _reference(
        series: list[MarketQuote], cutoff: datetime, lookback_hours: float
    ) -> float | None:
        if not series:
            return None
        nearest = min(series, key=lambda item: abs((item.observed_at - cutoff).total_seconds()))
        tolerance = min(72.0, max(18.0, lookback_hours * 0.03)) * 3600.0
        if abs((nearest.observed_at - cutoff).total_seconds()) > tolerance:
            return None
        return nearest.mid if nearest.mid > 0 else None

    @classmethod
    def _trend(
        cls, series: list[MarketQuote], current: MarketQuote, lookbacks: tuple[float, ...] | None = None
    ) -> tuple[dict[float, float], float, float, float]:
        horizons = lookbacks or cls.lookbacks
        ordered = sorted(
            (q for q in series if q.observed_at <= current.observed_at and q.mid > 0),
            key=lambda q: q.observed_at,
        )
        trailing: dict[float, float] = {}
        drifts: list[float] = []
        signs: list[int] = []
        for hours in horizons:
            ref = cls._reference(
                ordered, current.observed_at - timedelta(hours=hours), hours
            )
            if ref is None:
                continue
            value = current.mid / ref - 1.0
            trailing[hours] = value
            drifts.append(math.log(current.mid / ref) / max(1.0, hours / 24.0))
            signs.append(1 if value > 0 else -1 if value < 0 else 0)
        if not drifts:
            return trailing, 0.0, 0.0, 0.0
        drift = statistics.median(drifts)
        direction = 1 if drift > 0 else -1 if drift < 0 else 0
        agreement = (
            sum(sign == direction for sign in signs) / len(signs)
            if direction and signs
            else 0.0
        )
        return trailing, drift, agreement, sum(signs) / len(signs)

    @staticmethod
    def _regime(
        series: list[MarketQuote], current: MarketQuote
    ) -> Literal["low_vol", "normal", "high_vol"]:
        by_day: dict[str, MarketQuote] = {}
        for item in [*series, current]:
            if item.mid > 0 and item.observed_at <= current.observed_at:
                by_day[item.observed_at.date().isoformat()] = item
        daily = sorted(by_day.values(), key=lambda q: q.observed_at)[-31:]
        returns = [
            math.log(b.mid / a.mid)
            for a, b in zip(daily, daily[1:])
            if a.mid > 0 and b.mid > 0
        ]
        if len(returns) < 5:
            return "normal"
        vol = statistics.pstdev(returns)
        return "low_vol" if vol < 0.025 else "high_vol" if vol > 0.060 else "normal"

    @staticmethod
    def _funding_cost(
        funding: list[FundingQuote],
        quote: MarketQuote,
        direction: AlphaDirection,
        horizon_hours: float,
    ) -> float:
        if quote.market_kind != MarketKind.PERPETUAL:
            return 0.0
        matches = [
            item
            for item in funding
            if item.venue == quote.venue
            and item.asset.upper() == quote.asset.upper()
            and item.observed_at <= quote.observed_at
            and (item.symbol is None or item.symbol == quote.symbol)
        ]
        if not matches:
            return math.inf
        latest = max(matches, key=lambda item: item.observed_at)
        hourly_cost = latest.hourly_rate if direction == "long" else -latest.hourly_rate
        return max(0.0, hourly_cost * horizon_hours)  # favorable funding is ignored

    @staticmethod
    def _quotes(
        quotes: list[MarketQuote], direction: AlphaDirection
    ) -> list[MarketQuote]:
        if direction == "long":
            priorities = {"OKX": 0, "Bybit": 1, "Coinbase": 2, "Kraken": 3}
            rows = [q for q in quotes if q.market_kind == MarketKind.SPOT]
        else:
            priorities = {"HlPerp": 0, "OKX": 1, "Bybit": 2}
            rows = [q for q in quotes if q.market_kind == MarketKind.PERPETUAL]
        return sorted(rows, key=lambda q: (priorities.get(q.venue, 99), q.venue))

    @classmethod
    def _btc_score(
        cls,
        current: dict[str, list[MarketQuote]],
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
    ) -> float:
        for direction in ("long", "short"):
            for quote in cls._quotes(current.get("BTC", []), direction):  # type: ignore[arg-type]
                _, _, _, score = cls._trend(
                    history.get((quote.venue, "BTC", quote.market_kind), []), quote
                )
                if score:
                    return score
        return 0.0

    @classmethod
    def _breadth(
        cls,
        current: dict[str, list[MarketQuote]],
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
    ) -> float:
        signs: list[int] = []
        for asset, quotes in current.items():
            chosen = cls._quotes(quotes, "long") or cls._quotes(quotes, "short")
            if not chosen:
                continue
            quote = chosen[0]
            values, _, _, _ = cls._trend(
                history.get((quote.venue, asset, quote.market_kind), []),
                quote,
                (720.0,),
            )
            value = values.get(720.0)
            if value is not None:
                signs.append(1 if value > 0 else -1 if value < 0 else 0)
        return sum(signs) / len(signs) if signs else 0.0

    def discover(
        self,
        snapshot: ScanSnapshot,
        history: dict[tuple[str, str, MarketKind], list[MarketQuote]],
        settings: Settings,
        *,
        total_capital_usd: float,
    ) -> list[AlphaCandidate]:
        current: dict[str, list[MarketQuote]] = defaultdict(list)
        for quote in snapshot.market_quotes:
            if quote.market_kind in {MarketKind.SPOT, MarketKind.PERPETUAL}:
                current[quote.asset.upper()].append(quote)

        btc_score = self._btc_score(current, history)
        breadth = self._breadth(current, history)
        phase, progress, cycle_prior, cycle_days = self._cycle_context(snapshot.completed_at)
        scored: list[tuple[float, AlphaCandidate]] = []

        for asset, quotes in current.items():
            best: list[tuple[float, AlphaCandidate]] = []
            for provisional in ("long", "short"):
                for quote in self._quotes(quotes, provisional):  # type: ignore[arg-type]
                    key = (quote.venue, asset, quote.market_kind)
                    returns, drift, agreement, trend_score = self._trend(
                        history.get(key, []), quote
                    )
                    if len(returns) < 3 or agreement < 0.66 or abs(drift) < 0.00035:
                        continue
                    direction: AlphaDirection = "long" if drift > 0 else "short"
                    if direction != provisional:
                        continue
                    direction_sign = 1.0 if direction == "long" else -1.0
                    if direction_sign * btc_score < -0.60:
                        continue
                    if direction_sign * breadth < -0.70:
                        continue

                    carry = self._funding_cost(
                        snapshot.funding_quotes, quote, direction, self.horizon_hours
                    )
                    if not math.isfinite(carry):
                        continue

                    base = math.expm1(
                        max(0.0, direction_sign * drift) * self.horizon_hours / 24.0
                    ) * 0.25
                    market_multiplier = max(
                        0.80,
                        min(
                            1.20,
                            1.0
                            + 0.10 * direction_sign * btc_score
                            + 0.05 * direction_sign * breadth,
                        ),
                    )
                    cycle_multiplier = max(
                        0.90,
                        min(1.10, 1.0 + 0.10 * cycle_prior * direction_sign),
                    )
                    gross = min(0.03, base * market_multiplier * cycle_multiplier)
                    cost = max(0.0, settings.alpha_research_cost_floor_bps) / 10_000.0 + carry
                    net = gross - cost
                    if net <= 0:
                        continue

                    notional = min(
                        max(
                            settings.alpha_min_notional_usd,
                            total_capital_usd * settings.alpha_candidate_capital_fraction,
                        ),
                        total_capital_usd,
                    )
                    capital_multiple = (
                        settings.spot_collateral_fraction
                        if quote.market_kind == MarketKind.SPOT
                        else settings.perp_collateral_fraction
                    )
                    confidence = min(
                        1.0,
                        0.55 * agreement
                        + 0.25 * min(1.0, abs(drift) / 0.0014)
                        + 0.10 * max(0.0, direction_sign * btc_score)
                        + 0.10 * max(0.0, direction_sign * breadth),
                    )
                    candidate = AlphaCandidate(
                        candidate_id=(
                            f"alpha:{self.strategy_id}:{asset}:{quote.venue}:"
                            f"{quote.market_kind.value}:{uuid.uuid4().hex[:12]}"
                        ),
                        strategy_id=self.strategy_id,
                        family=self.family,
                        asset=asset,
                        direction=direction,
                        venue=quote.venue,
                        market_kind=quote.market_kind,
                        symbol=quote.symbol,
                        observed_at=quote.observed_at,
                        horizon_hours=self.horizon_hours,
                        lookback_hours=max(returns),
                        entry_reference_price=quote.mid,
                        expected_gross_return=gross,
                        estimated_cost_return=cost,
                        expected_net_return=net,
                        expected_profit_usd=notional * net,
                        notional_usd=notional,
                        capital_required_usd=max(
                            1.0, notional * max(0.01, capital_multiple)
                        ),
                        confidence_score=confidence,
                        regime=self._regime(history.get(key, []), quote),
                        conflict_keys=[
                            f"alpha-instrument:{quote.venue}:{quote.symbol}",
                            f"directional-trend:{asset}",
                        ],
                        features={
                            "trend_horizons_available": len(returns),
                            "trend_agreement": agreement,
                            "trend_signed_score": trend_score,
                            "robust_daily_log_drift": drift,
                            "btc_trend_score": btc_score,
                            "market_breadth_score": breadth,
                            "cycle_phase": phase,
                            "cycle_progress": progress,
                            "cycle_prior_score": cycle_prior,
                            "cycle_prior_weight": 0.10,
                            "nominal_cycle_days": cycle_days,
                            "holding_carry_cost_return": carry,
                            "halving_cycle_is_prior_not_trigger": True,
                        },
                    )
                    best.append((net, candidate))
                    break
            if best:
                best.sort(key=lambda row: (row[0], row[1].confidence_score), reverse=True)
                scored.append(best[0])

        scored.sort(key=lambda row: (row[0], row[1].confidence_score), reverse=True)
        return [candidate for _, candidate in scored[:6]]
