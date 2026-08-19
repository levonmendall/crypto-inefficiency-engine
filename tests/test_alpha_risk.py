from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaForwardOutcome
from inefficiency_engine.alpha_risk import AlphaRiskController
from inefficiency_engine.models import MarketKind


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


class RiskSettings(SimpleNamespace):
    alpha_health_recent_window = 12
    alpha_health_min_recent_samples = 8
    alpha_health_min_recent_mean_return = 0.00025
    alpha_health_min_recent_hit_rate = 0.50
    alpha_health_min_capture_ratio = 0.35
    alpha_health_full_capture_ratio = 0.80
    alpha_health_min_recent_to_long_ratio = 0.35
    alpha_health_max_drawdown = 0.06
    alpha_health_max_trailing_losses = 4
    alpha_health_capital_multiplier_floor = 0.25


def candidate() -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id="alpha-health-btc",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=60000.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.002,
        expected_net_return=0.008,
        expected_profit_usd=80.0,
        notional_usd=10000.0,
        capital_required_usd=10000.0,
        confidence_score=0.8,
        regime="normal",
    )


def outcome(index: int, realized: float, predicted: float = 0.008) -> AlphaForwardOutcome:
    observed = NOW + timedelta(hours=6 * index)
    due = observed + timedelta(hours=6)
    return AlphaForwardOutcome(
        signal_id=f"signal-{index}",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=observed,
        due_at=due,
        matured_at=due,
        horizon_hours=6.0,
        regime="normal",
        predicted_net_return=predicted,
        entry_price=60000.0,
        exit_price=60600.0,
        realized_gross_return=realized + 0.002,
        realized_net_return=realized,
        correct_direction=realized > 0,
    )


def test_healthy_forward_edge_receives_fractional_capital_not_unbounded_authority():
    controller = AlphaRiskController(RiskSettings())
    outcomes = [outcome(index, 0.006 if index % 5 else 0.003) for index in range(30)]
    health = controller.evaluate(candidate(), outcomes)

    assert health.healthy_for_paper_allocation is True
    assert 0.25 <= health.capital_multiplier <= 1.0
    assert health.forecast_capture_ratio_median == pytest.approx(0.75)
    assert health.recent_mean_net_return is not None and health.recent_mean_net_return > 0
    assert health.live_execution_authority is False


def test_recent_decay_revokes_paper_capital_even_when_long_run_history_was_profitable():
    controller = AlphaRiskController(RiskSettings())
    good = [outcome(index, 0.008) for index in range(22)]
    degraded = [outcome(22 + index, -0.003) for index in range(8)]
    health = controller.evaluate(candidate(), [*good, *degraded])

    assert health.long_run_mean_net_return is not None and health.long_run_mean_net_return > 0
    assert health.healthy_for_paper_allocation is False
    assert health.capital_multiplier == 0.0
    assert any("recent mean" in blocker or "loss" in blocker or "decayed" in blocker for blocker in health.blockers)


def test_drawdown_and_loss_streak_are_explicit_health_blockers():
    controller = AlphaRiskController(RiskSettings())
    values = [0.01] * 22 + [-0.02, -0.02, -0.02, -0.02, -0.02, -0.02, 0.001, 0.001]
    health = controller.evaluate(candidate(), [outcome(index, value) for index, value in enumerate(values)])

    assert health.max_compounded_drawdown is not None
    assert health.max_compounded_drawdown > 0.06
    assert health.healthy_for_paper_allocation is False
    assert "forward outcome drawdown exceeds health limit" in health.blockers
