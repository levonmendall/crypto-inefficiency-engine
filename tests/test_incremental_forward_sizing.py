from types import SimpleNamespace

import pytest

from inefficiency_engine.cycle_probation import CycleReplaySummary
from inefficiency_engine.incremental_forward_sizing import (
    FORWARD_EVIDENCE_STEP_SAMPLES,
    forward_evidence_allocation_fraction,
    incremental_forward_policy,
)


def _settings():
    return SimpleNamespace(
        alpha_min_forward_samples=30,
        alpha_health_min_recent_mean_return=0.00025,
        alpha_health_min_recent_hit_rate=0.50,
        alpha_health_min_capture_ratio=0.35,
        alpha_health_min_recent_to_long_ratio=0.35,
        alpha_health_max_drawdown=0.06,
        alpha_health_max_trailing_losses=4,
    )


def _qualification(sample_count: int):
    return SimpleNamespace(
        statistically_qualified=False,
        sample_count=sample_count,
        mean_realized_net_return=0.01,
        required_mean_lower_bound=0.001,
        hit_rate=2.0 / 3.0,
        regime_count=1,
    )


def _health(sample_count: int):
    return SimpleNamespace(
        # The ordinary health path may still be false before its normal 8-sample
        # minimum. Incremental paper uses the same raw health measurements with a
        # three-sample floor rather than silently restoring the old 8-sample veto.
        healthy_for_paper_allocation=False,
        capital_multiplier=0.0,
        recent_sample_count=sample_count,
        recent_mean_net_return=0.01,
        recent_hit_rate=2.0 / 3.0,
        forecast_capture_ratio_median=0.80,
        recent_to_long_run_ratio=1.0,
        max_compounded_drawdown=0.01,
        trailing_loss_streak=0,
    )


def _replay():
    return CycleReplaySummary(
        strategy_id="cycle_aware_multi_horizon_trend_v1",
        asset="BTC",
        direction="long",
        sample_count=40,
        positive_count=25,
        hit_rate=0.625,
        mean_realized_net_return=0.01,
        regime_count=2,
        regime_means={"normal": 0.01, "high_vol": 0.008},
        qualified_for_probationary_support=True,
    )


def test_forward_evidence_allocation_fraction_increases_every_three_outcomes():
    expected = {
        0: 0.0,
        2: 0.0,
        3: 0.10,
        5: 0.10,
        6: 0.20,
        8: 0.20,
        9: 0.30,
        12: 0.40,
        15: 0.50,
        18: 0.60,
        21: 0.70,
        24: 0.80,
        27: 0.90,
        29: 0.90,
        30: 1.00,
        45: 1.00,
    }
    assert FORWARD_EVIDENCE_STEP_SAMPLES == 3
    for samples, fraction in expected.items():
        assert forward_evidence_allocation_fraction(samples, full_target=30) == pytest.approx(fraction)


def test_incremental_policy_allows_three_outcomes_at_ten_percent_when_quality_is_good():
    decision = incremental_forward_policy(
        _qualification(3),
        _health(3),
        _replay(),
        _settings(),
    )

    assert decision.eligible is True
    assert decision.allocation_fraction == pytest.approx(0.10)
    assert decision.blockers == ()


def test_incremental_policy_steps_to_ninety_percent_but_full_target_requires_full_qualification():
    twenty_seven = incremental_forward_policy(
        _qualification(27),
        _health(12),
        _replay(),
        _settings(),
    )
    assert twenty_seven.eligible is True
    assert twenty_seven.allocation_fraction == pytest.approx(0.90)

    thirty = incremental_forward_policy(
        _qualification(30),
        _health(12),
        _replay(),
        _settings(),
    )
    assert thirty.eligible is False
    assert thirty.allocation_fraction == 0.0
    assert "full forward target reached without full statistical qualification" in thirty.blockers


def test_incremental_policy_keeps_health_and_historical_support_as_vetoes():
    health = _health(6)
    health.max_compounded_drawdown = 0.10
    decision = incremental_forward_policy(
        _qualification(6),
        health,
        _replay(),
        _settings(),
    )

    assert decision.eligible is False
    assert decision.allocation_fraction == 0.0
    assert "forward outcome drawdown exceeds health limit" in decision.blockers
