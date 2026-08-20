from __future__ import annotations

from dataclasses import dataclass

from inefficiency_engine.cycle_probation import CycleReplaySummary


FORWARD_EVIDENCE_STEP_SAMPLES = 3
FORWARD_EVIDENCE_FULL_TARGET = 30
INCREMENTAL_FORWARD_MIN_HIT_RATE = 0.55


@dataclass(frozen=True)
class IncrementalForwardPolicyDecision:
    eligible: bool
    allocation_fraction: float
    blockers: tuple[str, ...]


def forward_evidence_allocation_fraction(
    sample_count: int,
    *,
    full_target: int = FORWARD_EVIDENCE_FULL_TARGET,
) -> float:
    """Return the stepwise evidence cap: 3->10%, 6->20%, ... 30->100%."""
    samples = max(0, int(sample_count))
    target = max(FORWARD_EVIDENCE_STEP_SAMPLES, int(full_target))
    if samples < FORWARD_EVIDENCE_STEP_SAMPLES:
        return 0.0
    if samples >= target:
        return 1.0
    earned_samples = (samples // FORWARD_EVIDENCE_STEP_SAMPLES) * FORWARD_EVIDENCE_STEP_SAMPLES
    return max(0.0, min(1.0, earned_samples / target))


def incremental_forward_policy(
    qualification,
    health,
    replay: CycleReplaySummary | None,
    settings,
) -> IncrementalForwardPolicyDecision:
    """Pre-full-certification gate for incrementally sized paper learning.

    The forward sample count controls only the maximum paper size. Current
    economics, raw forward quality, adaptive-health metrics, historical
    walk-forward support and every downstream execution/risk gate remain
    subtractive. At the full sample target, this path closes and the ordinary
    statistical qualification path must authorize the candidate.
    """

    target = max(
        FORWARD_EVIDENCE_STEP_SAMPLES,
        int(getattr(settings, "alpha_min_forward_samples", FORWARD_EVIDENCE_FULL_TARGET)),
    )
    allocation_fraction = forward_evidence_allocation_fraction(
        qualification.sample_count,
        full_target=target,
    )
    blockers: list[str] = []

    if qualification.statistically_qualified:
        blockers.append("already fully qualified")
    if qualification.sample_count < FORWARD_EVIDENCE_STEP_SAMPLES:
        blockers.append("fewer than three genuine independent forward outcomes")
    elif qualification.sample_count >= target:
        blockers.append("full forward target reached without full statistical qualification")

    if (
        qualification.mean_realized_net_return is None
        or qualification.mean_realized_net_return <= qualification.required_mean_lower_bound
    ):
        blockers.append("forward mean return is below incremental-paper hurdle")
    if (
        qualification.hit_rate is None
        or qualification.hit_rate < INCREMENTAL_FORWARD_MIN_HIT_RATE
    ):
        blockers.append("forward hit rate is below incremental-paper hurdle")
    if qualification.regime_count < 1:
        blockers.append("no genuine forward regime coverage")

    if health.recent_sample_count < FORWARD_EVIDENCE_STEP_SAMPLES:
        blockers.append("insufficient recent outcomes for incremental health evaluation")
    if (
        health.recent_mean_net_return is None
        or health.recent_mean_net_return
        <= float(getattr(settings, "alpha_health_min_recent_mean_return", 0.0))
    ):
        blockers.append("recent mean net return is below health hurdle")
    if (
        health.recent_hit_rate is None
        or health.recent_hit_rate
        < float(getattr(settings, "alpha_health_min_recent_hit_rate", 0.50))
    ):
        blockers.append("recent hit rate is below health hurdle")
    if (
        health.forecast_capture_ratio_median is None
        or health.forecast_capture_ratio_median
        < float(getattr(settings, "alpha_health_min_capture_ratio", 0.35))
    ):
        blockers.append("realized-to-predicted capture ratio is below health hurdle")
    if (
        health.recent_to_long_run_ratio is None
        or health.recent_to_long_run_ratio
        < float(getattr(settings, "alpha_health_min_recent_to_long_ratio", 0.35))
    ):
        blockers.append("recent performance has decayed versus long-run evidence")
    if (
        health.max_compounded_drawdown is None
        or health.max_compounded_drawdown
        > float(getattr(settings, "alpha_health_max_drawdown", 0.06))
    ):
        blockers.append("forward outcome drawdown exceeds health limit")
    if health.trailing_loss_streak >= int(
        getattr(settings, "alpha_health_max_trailing_losses", 4)
    ):
        blockers.append("consecutive forward losses exceed health limit")

    if replay is None or not replay.qualified_for_probationary_support:
        blockers.append("historical walk-forward support is not qualified")

    eligible = not blockers and allocation_fraction > 0.0
    return IncrementalForwardPolicyDecision(
        eligible=eligible,
        allocation_fraction=allocation_fraction if eligible else 0.0,
        blockers=tuple(blockers),
    )
