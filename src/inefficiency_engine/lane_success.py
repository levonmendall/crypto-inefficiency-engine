from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select
from sqlalchemy.exc import IntegrityError

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketQuote


LANE_SUCCESS_EVENT_VERSION = 1
MIN_CALIBRATION_SAMPLES = 5
MIN_REGIME_SAMPLES = 5
MIN_CORRELATION_SAMPLES = 8
RECENT_HEALTH_WINDOW = 8
MAX_TRAILING_LOSSES = 4
HIGH_CORRELATION_THRESHOLD = 0.80


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:48]


def _json(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _number(value: object | None) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _strategy_key(strategy: str, *, opportunity_id: str | None = None) -> str:
    if opportunity_id in {
        "yield",
        "liquidity_provision",
        "volatility",
        "liquidation_distress",
        "capital_location_settlement",
    }:
        return f"mechanism:{opportunity_id}"
    if strategy.startswith("mechanism:"):
        parts = strategy.split(":")
        return ":".join(parts[:2]) if len(parts) >= 2 else strategy
    return strategy


class LaneSuccessProfile(BaseModel):
    strategy: str
    regime: str | None = None
    sample_count: int = Field(ge=0)
    recent_sample_count: int = Field(ge=0)
    mean_realized_return: float | None = None
    recent_mean_realized_return: float | None = None
    median_capture_ratio: float | None = None
    recent_hit_rate: float | None = None
    trailing_loss_streak: int = Field(ge=0)
    calibration_multiplier: float = Field(ge=0, le=1)
    regime_multiplier: float = Field(ge=0, le=1)
    health_multiplier: float = Field(ge=0, le=1)
    combined_multiplier: float = Field(ge=0, le=1)
    state: str
    blockers: list[str] = Field(default_factory=list)
    paper_only: bool = True


class CandidateSuccessDecision(BaseModel):
    candidate_id: str
    strategy: str
    regime: str
    accepted: bool
    raw_expected_return: float
    adjusted_expected_return: float
    calibration_multiplier: float
    regime_multiplier: float
    health_multiplier: float
    combined_multiplier: float
    capital_velocity_score: float
    risk_factors: list[str] = Field(default_factory=list)
    reason: str | None = None
    diagnostic_only: bool = False
    allocation_authority: bool = False
    live_execution_authority: bool = False
    paper_only: bool = True


class LaneSuccessLedger:
    """Append-only realized forecast/outcome observations for every allocatable lane."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.events = Table(
            "lane_success_events",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_id", String(64), nullable=False, unique=True),
            Column("event_type", String(24), nullable=False),
            Column("strategy", Text, nullable=False),
            Column("asset", Text, nullable=False),
            Column("regime", String(32), nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_lane_success_strategy", self.events.c.strategy, self.events.c.id)
        Index("ix_lane_success_regime", self.events.c.strategy, self.events.c.regime, self.events.c.id)
        metadata.create_all(store.engine)

    def _record(
        self,
        *,
        event_id: str,
        event_type: str,
        strategy: str,
        asset: str,
        regime: str,
        observed_at: datetime,
        payload: dict[str, object],
    ) -> None:
        try:
            with self.store.engine.begin() as db:
                db.execute(
                    insert(self.events),
                    {
                        "event_id": event_id,
                        "event_type": event_type,
                        "strategy": strategy,
                        "asset": asset.upper(),
                        "regime": regime,
                        "observed_at": observed_at.isoformat(),
                        "payload_json": _json(
                            {
                                "version": LANE_SUCCESS_EVENT_VERSION,
                                **payload,
                                "paper_only": True,
                                "live_execution_authority": False,
                            }
                        ),
                    },
                )
        except IntegrityError:
            # Support checks can reconstruct the same forecast repeatedly. Stable
            # event IDs make this read plane idempotent without mutating history.
            return

    def record_forecast(
        self,
        *,
        forecast_key: str,
        strategy: str,
        asset: str,
        regime: str,
        observed_at: datetime,
        predicted_return: float,
        predicted_profit_usd: float,
        capital_usd: float,
        holding_hours: float | None,
        venues: list[str],
        candidate_id: str,
    ) -> None:
        self._record(
            event_id=_stable("forecast", forecast_key),
            event_type="forecast",
            strategy=strategy,
            asset=asset,
            regime=regime,
            observed_at=observed_at,
            payload={
                "forecast_key": forecast_key,
                "candidate_id": candidate_id,
                "predicted_return": predicted_return,
                "predicted_profit_usd": predicted_profit_usd,
                "capital_usd": capital_usd,
                "holding_hours": holding_hours,
                "venues": venues,
            },
        )

    def record_outcome(
        self,
        *,
        outcome_key: str,
        strategy: str,
        asset: str,
        regime: str,
        observed_at: datetime,
        predicted_return: float,
        realized_return: float,
        predicted_profit_usd: float,
        realized_profit_usd: float,
        capital_usd: float,
        holding_hours: float | None,
        venues: list[str],
        candidate_id: str,
        settlement_method: str,
        failure_attribution: list[str],
        settlement_detail: dict[str, object] | None = None,
    ) -> None:
        capture = realized_return / predicted_return if predicted_return > 1e-12 else None
        self._record(
            event_id=_stable("outcome", outcome_key),
            event_type="outcome",
            strategy=strategy,
            asset=asset,
            regime=regime,
            observed_at=observed_at,
            payload={
                "outcome_key": outcome_key,
                "candidate_id": candidate_id,
                "predicted_return": predicted_return,
                "realized_return": realized_return,
                "predicted_profit_usd": predicted_profit_usd,
                "realized_profit_usd": realized_profit_usd,
                "capital_usd": capital_usd,
                "holding_hours": holding_hours,
                "venues": venues,
                "capture_ratio": capture,
                "profitable": realized_return > 0,
                "settlement_method": settlement_method,
                "failure_attribution": failure_attribution,
                "settlement_detail": settlement_detail or {},
            },
        )

    def outcomes(
        self,
        *,
        strategy: str | None = None,
        regime: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        query = (
            select(
                self.events.c.strategy,
                self.events.c.asset,
                self.events.c.regime,
                self.events.c.observed_at,
                self.events.c.payload_json,
            )
            .where(self.events.c.event_type == "outcome")
            .order_by(self.events.c.id.desc())
            .limit(max(1, min(5000, int(limit))))
        )
        if strategy is not None:
            query = query.where(self.events.c.strategy == strategy)
        if regime is not None:
            query = query.where(self.events.c.regime == regime)
        with self.store.engine.connect() as db:
            raw_rows = list(db.execute(query))
        rows: list[dict[str, Any]] = []
        for strategy_value, asset, regime_value, observed_at, raw in raw_rows:
            try:
                payload = json.loads(str(raw))
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            payload.update(
                {
                    "strategy": str(strategy_value),
                    "asset": str(asset),
                    "regime": str(regime_value),
                    "observed_at": str(observed_at),
                }
            )
            rows.append(payload)
        rows.reverse()
        return rows


class LaneSuccessController:
    """Subtractive calibration, edge decay, regime and cross-lane risk controller."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        self.ledger = LaneSuccessLedger(store)

    def market_regime(self, *, now: datetime | None = None) -> str:
        current = now or _now()
        table = self.store.market_quotes
        try:
            with self.store.engine.connect() as db:
                payloads = list(
                    db.execute(
                        select(table.c.payload_json)
                        .where(table.c.asset == "BTC")
                        .order_by(table.c.id.desc())
                        .limit(240)
                    ).scalars()
                )
        except Exception:
            return "unknown"
        quotes: list[MarketQuote] = []
        for raw in reversed(payloads):
            try:
                quote = MarketQuote.model_validate_json(raw)
            except Exception:
                continue
            if quote.mid > 0 and quote.observed_at <= current:
                quotes.append(quote)
        buckets: dict[str, list[float]] = defaultdict(list)
        for quote in quotes:
            buckets[quote.observed_at.strftime("%Y-%m-%dT%H:%M")].append(quote.mid)
        series = [statistics.median(values) for _, values in sorted(buckets.items()) if values]
        if len(series) < 8:
            return "unknown"
        returns = [
            math.log(cur / prev)
            for prev, cur in zip(series, series[1:])
            if prev > 0 and cur > 0
        ]
        if len(returns) < 4:
            return "unknown"
        vol = statistics.pstdev(returns)
        trend = series[-1] / series[max(0, len(series) - min(24, len(series)))] - 1.0
        if vol > 0.010:
            return "stress_high_vol"
        if vol > 0.005:
            return "high_vol"
        if abs(trend) > 0.025:
            return "trend_up" if trend > 0 else "trend_down"
        if vol < 0.0015:
            return "low_vol_range"
        return "normal"

    @staticmethod
    def risk_factors(candidate) -> list[str]:
        strategy = _strategy_key(
            str(candidate.strategy),
            opportunity_id=str(candidate.opportunity_id) if candidate.opportunity_id else None,
        )
        factors = {f"asset:{str(candidate.asset).upper()}"}
        factors.update(f"venue:{venue}" for venue in list(candidate.venues or []))
        exposure = str(getattr(candidate, "exposure_kind", "market_neutral"))
        if exposure != "market_neutral":
            factors.add("crypto_beta")
            factors.add(f"direction:{exposure}")
        lower = strategy.lower()
        if "carry" in lower or "funding" in lower:
            factors.add("funding")
        if "volatility" in lower or "option" in lower:
            factors.add("volatility")
        if "liquidity" in lower or "microstructure" in lower or "maker" in lower:
            factors.add("liquidity")
        if "yield" in lower or "onchain" in lower or "fundamental" in lower:
            factors.add("protocol_chain")
        if "liquidation" in lower or "distress" in lower:
            factors.update({"liquidity", "deleveraging"})
        if "cex_dex" in lower or "capital_location" in lower:
            factors.add("settlement_location")
        return sorted(factors)

    @staticmethod
    def _capture_values(rows: list[dict[str, Any]]) -> list[float]:
        values: list[float] = []
        for row in rows:
            value = _number(row.get("capture_ratio"))
            if value is not None:
                values.append(value)
        return values

    @staticmethod
    def _realized_values(rows: list[dict[str, Any]]) -> list[float]:
        return [
            value
            for row in rows
            if (value := _number(row.get("realized_return"))) is not None
        ]

    @staticmethod
    def _loss_streak(values: list[float]) -> int:
        streak = 0
        for value in reversed(values):
            if value >= 0:
                break
            streak += 1
        return streak

    def profile(self, strategy: str, *, regime: str | None = None) -> LaneSuccessProfile:
        key = _strategy_key(strategy)
        all_rows = self.ledger.outcomes(strategy=key, limit=500)
        regime_rows = [row for row in all_rows if row.get("regime") == regime] if regime else []
        values = self._realized_values(all_rows)
        recent_values = values[-RECENT_HEALTH_WINDOW:]
        captures = self._capture_values(all_rows)
        recent_hit = (
            sum(value > 0 for value in recent_values) / len(recent_values)
            if recent_values
            else None
        )
        calibration = 1.0
        if len(captures) >= MIN_CALIBRATION_SAMPLES:
            calibration = _clamp(statistics.median(captures), 0.0, 1.0)

        regime_multiplier = 1.0
        regime_values = self._realized_values(regime_rows)
        regime_captures = self._capture_values(regime_rows)
        if regime and len(regime_values) >= MIN_REGIME_SAMPLES:
            if statistics.fmean(regime_values) <= 0:
                regime_multiplier = 0.0
            elif regime_captures:
                regime_multiplier = _clamp(statistics.median(regime_captures), 0.0, 1.0)

        health = 1.0
        blockers: list[str] = []
        state = "learning"
        if len(recent_values) >= MIN_CALIBRATION_SAMPLES:
            long_mean = statistics.fmean(values)
            recent_mean = statistics.fmean(recent_values)
            streak = self._loss_streak(values)
            if recent_mean <= 0:
                health = 0.0
                blockers.append("recent realized mean return is non-positive")
            if streak >= MAX_TRAILING_LOSSES:
                health = 0.0
                blockers.append("trailing loss streak exceeds lane-success limit")
            if captures and statistics.median(captures[-RECENT_HEALTH_WINDOW:]) < 0.25:
                health = 0.0
                blockers.append("recent realized-to-predicted capture has collapsed")
            if health > 0 and long_mean > 1e-12:
                decay = _clamp(recent_mean / long_mean, 0.25, 1.0)
                health = min(health, decay)
                if decay < 0.75:
                    blockers.append("recent edge has weakened versus long-run realized evidence")
            state = "healthy" if health >= 0.80 else "weakening" if health >= 0.40 else "probation" if health > 0 else "suspended"

        combined = min(calibration, regime_multiplier, health)
        if regime_multiplier == 0:
            blockers.append(f"current regime {regime!r} has non-qualifying realized evidence")
            state = "suspended"
        return LaneSuccessProfile(
            strategy=key,
            regime=regime,
            sample_count=len(values),
            recent_sample_count=len(recent_values),
            mean_realized_return=statistics.fmean(values) if values else None,
            recent_mean_realized_return=statistics.fmean(recent_values) if recent_values else None,
            median_capture_ratio=statistics.median(captures) if captures else None,
            recent_hit_rate=recent_hit,
            trailing_loss_streak=self._loss_streak(values),
            calibration_multiplier=calibration,
            regime_multiplier=regime_multiplier,
            health_multiplier=health,
            combined_multiplier=combined,
            state=state,
            blockers=blockers,
        )

    def adjust_return(
        self,
        *,
        strategy: str,
        raw_expected_return: float,
        regime: str,
    ) -> tuple[float, LaneSuccessProfile]:
        profile = self.profile(strategy, regime=regime)
        adjusted = max(0.0, float(raw_expected_return)) * profile.combined_multiplier
        return min(max(0.0, float(raw_expected_return)), adjusted), profile

    def adjust_candidate(self, candidate, *, regime: str) -> tuple[object | None, CandidateSuccessDecision]:
        raw_return = max(0.0, float(candidate.expected_return_on_reserved_capital))
        strategy = _strategy_key(
            str(candidate.strategy),
            opportunity_id=str(candidate.opportunity_id) if candidate.opportunity_id else None,
        )
        adjusted_return, profile = self.adjust_return(
            strategy=strategy,
            raw_expected_return=raw_return,
            regime=regime,
        )
        capital = max(1e-9, float(candidate.capital_required_usd))
        holding = max(0.25, float(candidate.modeled_holding_hours or 1.0))
        raw_profit = max(0.0, float(candidate.expected_profit_usd_per_deployment))
        adjusted_profit = min(raw_profit, capital * adjusted_return)
        velocity = adjusted_profit / capital / holding
        accepted = adjusted_return > 0 and profile.combined_multiplier > 0
        reason = None if accepted else "; ".join(profile.blockers) or "lane-success calibration revoked current paper eligibility"
        decision = CandidateSuccessDecision(
            candidate_id=str(candidate.candidate_id),
            strategy=strategy,
            regime=regime,
            accepted=accepted,
            raw_expected_return=raw_return,
            adjusted_expected_return=adjusted_return,
            calibration_multiplier=profile.calibration_multiplier,
            regime_multiplier=profile.regime_multiplier,
            health_multiplier=profile.health_multiplier,
            combined_multiplier=profile.combined_multiplier,
            capital_velocity_score=velocity,
            risk_factors=self.risk_factors(candidate),
            reason=reason,
        )
        if not accepted:
            return None, decision
        updated = candidate.model_copy(
            update={
                "expected_return_on_reserved_capital": adjusted_return,
                "expected_profit_usd_per_deployment": adjusted_profit,
                "source_return_metric": f"{candidate.source_return_metric}|lane_success_calibrated",
                "source_return_value": min(float(candidate.source_return_value), adjusted_return)
                if float(candidate.source_return_value) >= 0
                else float(candidate.source_return_value),
            }
        )
        return updated, decision

    def _strategy_series(self, strategy: str) -> dict[int, float]:
        rows = self.ledger.outcomes(strategy=_strategy_key(strategy), limit=1000)
        buckets: dict[int, list[float]] = defaultdict(list)
        for row in rows:
            try:
                observed = datetime.fromisoformat(str(row["observed_at"]).replace("Z", "+00:00"))
            except (KeyError, TypeError, ValueError):
                continue
            value = _number(row.get("realized_return"))
            if value is None:
                continue
            bucket = int(observed.timestamp() // (6 * 3600))
            buckets[bucket].append(value)
        return {key: statistics.fmean(values) for key, values in buckets.items() if values}

    def correlation(self, strategy_a: str, strategy_b: str) -> tuple[float | None, int]:
        a = self._strategy_series(strategy_a)
        b = self._strategy_series(strategy_b)
        common = sorted(set(a).intersection(b))
        if len(common) < MIN_CORRELATION_SAMPLES:
            return None, len(common)
        xs = [a[key] for key in common]
        ys = [b[key] for key in common]
        mean_x = statistics.fmean(xs)
        mean_y = statistics.fmean(ys)
        dx = [value - mean_x for value in xs]
        dy = [value - mean_y for value in ys]
        denom = math.sqrt(sum(value * value for value in dx) * sum(value * value for value in dy))
        if denom <= 1e-15:
            return None, len(common)
        return sum(x * y for x, y in zip(dx, dy)) / denom, len(common)

    def adjust_and_diversify(
        self,
        candidates: Iterable[object],
        *,
        total_capital_usd: float,
        regime: str | None = None,
    ) -> tuple[list[object], list[dict[str, object]], list[dict[str, object]]]:
        current_regime = regime or self.market_regime()
        adjusted: list[tuple[float, object, CandidateSuccessDecision]] = []
        skipped: list[dict[str, object]] = []
        diagnostics: list[dict[str, object]] = []
        for candidate in candidates:
            updated, decision = self.adjust_candidate(candidate, regime=current_regime)
            diagnostics.append(decision.model_dump(mode="json"))
            if updated is None:
                skipped.append(
                    {
                        "candidate_id": decision.candidate_id,
                        "family": getattr(candidate, "family", None),
                        "reason": decision.reason,
                        "lane_success_state": "suspended",
                        "lane_success_regime": current_regime,
                    }
                )
                continue
            adjusted.append((decision.capital_velocity_score, updated, decision))
        adjusted.sort(
            key=lambda row: (
                row[0],
                float(row[1].expected_return_on_reserved_capital),
                float(row[1].expected_profit_usd_per_deployment),
            ),
            reverse=True,
        )

        selected: list[object] = []
        selected_decisions: list[CandidateSuccessDecision] = []
        for _, candidate, decision in adjusted:
            rejected = None
            factors = set(decision.risk_factors)
            for incumbent, incumbent_decision in zip(selected, selected_decisions):
                overlap = factors.intersection(incumbent_decision.risk_factors)
                if not overlap:
                    continue
                corr, samples = self.correlation(decision.strategy, incumbent_decision.strategy)
                if corr is not None and corr >= HIGH_CORRELATION_THRESHOLD:
                    rejected = {
                        "candidate_id": decision.candidate_id,
                        "family": getattr(candidate, "family", None),
                        "reason": "empirically correlated hidden-risk exposure to a higher capital-velocity candidate",
                        "correlated_with": str(getattr(incumbent, "candidate_id", "")),
                        "correlation": corr,
                        "overlap_samples": samples,
                        "shared_risk_factors": sorted(overlap),
                    }
                    break
            if rejected is not None:
                skipped.append(rejected)
                continue
            selected.append(candidate)
            selected_decisions.append(decision)

        return selected, skipped, diagnostics

    @staticmethod
    def failure_attribution(
        *,
        predicted_return: float,
        realized_return: float,
        settlement_method: str,
        detail: dict[str, object] | None = None,
    ) -> list[str]:
        detail = detail or {}
        categories: list[str] = []
        capture = realized_return / predicted_return if predicted_return > 1e-12 else None
        if predicted_return > 0 and (capture is None or capture < 0.50):
            categories.append("forecast_error")
        latency = _number(detail.get("observation_latency_seconds"))
        if latency is not None and latency > 1.0:
            categories.extend(["source_latency", "timing_decay"])
        if detail.get("bid_crossed_without_fill") or detail.get("ask_crossed_without_fill"):
            categories.append("queue_non_fill")
        if (_number(detail.get("adverse_selection_penalty")) or 0.0) > 0:
            categories.append("adverse_selection")
        if (
            (_number(detail.get("hedge_cost_return")) or 0.0) > 0
            or (_number(detail.get("residual_delta_penalty")) or 0.0) > 0
            or (_number(detail.get("gamma_gap_penalty")) or 0.0) > 0
        ):
            categories.append("hedge_error")
        if detail.get("exit_liquidity_sufficient") is False:
            categories.append("liquidity_loss")
        if (_number(detail.get("capture_probability")) or 1.0) < 0.50:
            categories.append("capture_probability")
        lower_method = settlement_method.lower()
        if "funding" in lower_method and realized_return < predicted_return:
            categories.append("funding_change")
        if "transfer" in lower_method and realized_return < predicted_return:
            categories.append("transfer_or_settlement_delay")
        if realized_return <= 0 < predicted_return:
            categories.append("model_overconfidence")
        return list(dict.fromkeys(categories))

    def record_allocation_forecast(self, trial) -> None:
        strategy = _strategy_key(str(trial.strategy))
        observed = trial.plan_observed_at
        regime = self.market_regime(now=observed)
        holding = (
            max(0.0, (trial.due_at - observed).total_seconds() / 3600.0)
            if trial.due_at is not None
            else None
        )
        self.ledger.record_forecast(
            forecast_key=f"allocation|{trial.candidate_id}|{trial.plan_observed_at.isoformat()}",
            strategy=strategy,
            asset=trial.asset,
            regime=regime,
            observed_at=observed,
            predicted_return=trial.predicted_return_on_reserved_capital,
            predicted_profit_usd=trial.predicted_profit_usd,
            capital_usd=trial.capital_required_usd,
            holding_hours=holding,
            venues=list(trial.venues),
            candidate_id=trial.candidate_id,
        )

    def record_allocation_outcome(
        self,
        trial,
        outcome,
        *,
        settlement_detail: dict[str, object] | None = None,
    ) -> None:
        strategy = _strategy_key(str(trial.strategy))
        regime = self.market_regime(now=trial.plan_observed_at)
        holding = (
            max(0.0, (outcome.matured_at - trial.plan_observed_at).total_seconds() / 3600.0)
            if outcome.matured_at is not None
            else None
        )
        attribution = self.failure_attribution(
            predicted_return=trial.predicted_return_on_reserved_capital,
            realized_return=outcome.realized_net_return,
            settlement_method=outcome.settlement_method,
            detail=settlement_detail,
        )
        self.ledger.record_outcome(
            outcome_key=f"allocation|{outcome.outcome_id}",
            strategy=strategy,
            asset=trial.asset,
            regime=regime,
            observed_at=outcome.matured_at,
            predicted_return=trial.predicted_return_on_reserved_capital,
            realized_return=outcome.realized_net_return,
            predicted_profit_usd=trial.predicted_profit_usd,
            realized_profit_usd=outcome.realized_profit_usd,
            capital_usd=trial.capital_required_usd,
            holding_hours=holding,
            venues=list(trial.venues),
            candidate_id=trial.candidate_id,
            settlement_method=outcome.settlement_method,
            failure_attribution=attribution,
            settlement_detail=settlement_detail,
        )

    def record_mechanism_outcome(self, trial, outcome, *, settlement_detail: dict[str, object]) -> None:
        regime = str(trial.settlement_payload.get("lane_success_regime") or "unknown")
        strategy = f"mechanism:{trial.mechanism_id}"
        attribution = self.failure_attribution(
            predicted_return=trial.predicted_net_return,
            realized_return=outcome.realized_net_return,
            settlement_method=outcome.settlement_method,
            detail=settlement_detail,
        )
        self.ledger.record_outcome(
            outcome_key=f"mechanism|{outcome.trial_id}",
            strategy=strategy,
            asset=trial.asset,
            regime=regime,
            observed_at=outcome.matured_at,
            predicted_return=trial.predicted_net_return,
            realized_return=outcome.realized_net_return,
            predicted_profit_usd=trial.predicted_profit_usd,
            realized_profit_usd=outcome.realized_profit_usd,
            capital_usd=trial.capital_usd,
            holding_hours=max(
                0.0,
                (trial.due_at - trial.source_observed_at).total_seconds() / 3600.0,
            ),
            venues=list(trial.venues),
            candidate_id=trial.trial_id,
            settlement_method=outcome.settlement_method,
            failure_attribution=attribution,
            settlement_detail=settlement_detail,
        )
