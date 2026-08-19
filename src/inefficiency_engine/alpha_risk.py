from __future__ import annotations

import statistics
from typing import Iterable

from pydantic import BaseModel, Field

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaForwardOutcome


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


class AlphaStrategyHealth(BaseModel):
    strategy_id: str
    family: str
    asset: str
    direction: str
    sample_count: int = Field(ge=0)
    recent_sample_count: int = Field(ge=0)
    recent_mean_net_return: float | None = None
    recent_hit_rate: float | None = None
    long_run_mean_net_return: float | None = None
    forecast_capture_ratio_median: float | None = None
    recent_to_long_run_ratio: float | None = None
    max_compounded_drawdown: float | None = None
    trailing_loss_streak: int = Field(ge=0)
    calibration_mae: float | None = None
    health_score: float = Field(ge=0, le=1)
    capital_multiplier: float = Field(ge=0, le=1)
    healthy_for_paper_allocation: bool
    blockers: list[str] = Field(default_factory=list)
    live_execution_authority: bool = False
    paper_only: bool = True


class AlphaRiskController:
    """Conservative post-qualification health and sizing controller.

    Statistical promotion answers whether an alpha family has earned eligibility.
    This controller answers whether that earned edge still appears healthy *now*.
    It can reduce or revoke paper capital, but never create allocation authority.
    """

    def __init__(self, settings):
        self.settings = settings

    @staticmethod
    def _max_compounded_drawdown(values: Iterable[float]) -> float:
        wealth = 1.0
        peak = 1.0
        worst = 0.0
        for value in values:
            # Forward returns are research observations. Clamp pathological input
            # so one malformed observation cannot make the health model undefined.
            wealth *= 1.0 + max(-0.99, value)
            peak = max(peak, wealth)
            if peak > 0:
                worst = max(worst, 1.0 - wealth / peak)
        return worst

    @staticmethod
    def _trailing_loss_streak(values: list[float]) -> int:
        count = 0
        for value in reversed(values):
            if value >= 0:
                break
            count += 1
        return count

    def evaluate(
        self,
        candidate: AlphaCandidate,
        outcomes: list[AlphaForwardOutcome],
    ) -> AlphaStrategyHealth:
        ordered = sorted(outcomes, key=lambda item: (item.matured_at, item.signal_id))
        values = [item.realized_net_return for item in ordered]
        window = max(1, int(self.settings.alpha_health_recent_window))
        recent = ordered[-window:]
        recent_values = [item.realized_net_return for item in recent]

        long_mean = statistics.fmean(values) if values else None
        recent_mean = statistics.fmean(recent_values) if recent_values else None
        recent_hit = (
            sum(value > 0 for value in recent_values) / len(recent_values)
            if recent_values
            else None
        )

        ratios = [
            item.realized_net_return / item.predicted_net_return
            for item in ordered
            if item.predicted_net_return > 1e-12
        ]
        capture_ratio = statistics.median(ratios) if ratios else None
        mae = (
            statistics.fmean(abs(item.realized_net_return - item.predicted_net_return) for item in ordered)
            if ordered
            else None
        )
        recent_to_long = (
            recent_mean / long_mean
            if recent_mean is not None and long_mean is not None and long_mean > 1e-12
            else None
        )
        max_drawdown = self._max_compounded_drawdown(values) if values else None
        loss_streak = self._trailing_loss_streak(values)

        blockers: list[str] = []
        if len(recent_values) < int(self.settings.alpha_health_min_recent_samples):
            blockers.append("insufficient recent independent outcomes for health sizing")
        if recent_mean is None or recent_mean <= self.settings.alpha_health_min_recent_mean_return:
            blockers.append("recent mean net return is below health hurdle")
        if capture_ratio is None or capture_ratio < self.settings.alpha_health_min_capture_ratio:
            blockers.append("realized-to-predicted capture ratio has degraded")
        if recent_to_long is None or recent_to_long < self.settings.alpha_health_min_recent_to_long_ratio:
            blockers.append("recent performance has decayed versus long-run evidence")
        if max_drawdown is None or max_drawdown > self.settings.alpha_health_max_drawdown:
            blockers.append("forward outcome drawdown exceeds health limit")
        if loss_streak >= int(self.settings.alpha_health_max_trailing_losses):
            blockers.append("consecutive forward losses exceed health limit")

        if blockers:
            score = 0.0
            multiplier = 0.0
        else:
            capture_component = _clamp(
                (capture_ratio - self.settings.alpha_health_min_capture_ratio)
                / max(
                    1e-9,
                    self.settings.alpha_health_full_capture_ratio
                    - self.settings.alpha_health_min_capture_ratio,
                )
            )
            hit_component = _clamp(
                ((recent_hit or 0.0) - self.settings.alpha_health_min_recent_hit_rate)
                / max(1e-9, 1.0 - self.settings.alpha_health_min_recent_hit_rate)
            )
            decay_component = _clamp(
                ((recent_to_long or 0.0) - self.settings.alpha_health_min_recent_to_long_ratio)
                / max(1e-9, 1.0 - self.settings.alpha_health_min_recent_to_long_ratio)
            )
            drawdown_component = _clamp(
                1.0 - (max_drawdown or 0.0) / max(1e-9, self.settings.alpha_health_max_drawdown)
            )
            score = statistics.fmean(
                [capture_component, hit_component, decay_component, drawdown_component]
            )
            floor = _clamp(self.settings.alpha_health_capital_multiplier_floor)
            multiplier = floor + (1.0 - floor) * score

        return AlphaStrategyHealth(
            strategy_id=candidate.strategy_id,
            family=candidate.family,
            asset=candidate.asset,
            direction=candidate.direction,
            sample_count=len(values),
            recent_sample_count=len(recent_values),
            recent_mean_net_return=recent_mean,
            recent_hit_rate=recent_hit,
            long_run_mean_net_return=long_mean,
            forecast_capture_ratio_median=capture_ratio,
            recent_to_long_run_ratio=recent_to_long,
            max_compounded_drawdown=max_drawdown,
            trailing_loss_streak=loss_streak,
            calibration_mae=mae,
            health_score=score,
            capital_multiplier=multiplier,
            healthy_for_paper_allocation=not blockers,
            blockers=blockers,
            live_execution_authority=False,
            paper_only=True,
        )
