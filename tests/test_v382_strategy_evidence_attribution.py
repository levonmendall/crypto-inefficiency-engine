from types import SimpleNamespace

from inefficiency_engine import __version__
from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_resilience import RESILIENT_DASHBOARD_HTML
from inefficiency_engine.strategy_evidence_read import (
    _mixed_lane_state,
    diagnose_alpha_strategy,
)


def _settings():
    return Settings()


def _outcomes(values, *, regimes=("low_vol", "normal")):
    rows = []
    for index, value in enumerate(values):
        rows.append({
            "strategy_id": "test_strategy",
            "asset": "BTC",
            "direction": "long",
            "realized_net_return": value,
            "regime": regimes[index % len(regimes)],
        })
    return rows


def test_new_strategy_collecting_does_not_inherit_legacy_failure():
    settings = _settings()
    old = diagnose_alpha_strategy(
        strategy_id="time_series_momentum_v1",
        name="Original time-series momentum",
        family="directional_time_series",
        signal_count=30,
        outcomes=_outcomes([-0.001] * 30),
        allocator={},
        settings=settings,
        strategy_count=7,
    )
    new = diagnose_alpha_strategy(
        strategy_id="cycle_aware_multi_horizon_trend_v1",
        name="Cycle-aware multi-horizon trend",
        family="directional_time_series",
        signal_count=0,
        outcomes=[],
        allocator={},
        settings=settings,
        strategy_count=7,
    )

    assert old["state"] == "poor_economics"
    assert new["state"] == "collecting"
    assert new["independent_forward_outcome_count"] == 0
    assert new["required_forward_outcomes"] == 30
    assert _mixed_lane_state([old, new]) == "collecting"


def test_statistical_failure_exposes_exact_threshold_gap_without_relaxing_it():
    settings = _settings()
    row = diagnose_alpha_strategy(
        strategy_id="test_strategy",
        name="Test strategy",
        family="directional_time_series",
        signal_count=30,
        outcomes=_outcomes([0.0001] * 30),
        allocator={},
        settings=settings,
        strategy_count=7,
    )

    assert row["state"] == "statistical_failure"
    assert row["required_forward_outcomes"] == 30
    assert row["required_hit_rate_ci_lower"] == 0.50
    assert row["required_regimes"] == 2
    assert row["required_mean_return_ci_lower"] > settings.alpha_min_forward_mean_return
    assert any("mean-return CI lower" in gate for gate in row["failed_gates"])


def test_v382_dashboard_exposes_strategy_evidence_and_preserves_authority_boundary():
    assert __version__ == "3.8.2"
    assert "Strategy evidence" in RESILIENT_DASHBOARD_HTML
    assert "Capital authority remains qualified independently by strategy + asset + direction" in RESILIENT_DASHBOARD_HTML
    assert "historical failed cohorts are not reset" in RESILIENT_DASHBOARD_HTML

    settings = Settings()
    assert settings.alpha_min_forward_samples == 30
    assert settings.alpha_min_hit_rate_lower_bound == 0.50
    assert settings.alpha_min_regimes == 2
    assert settings.alpha_evidence_every_cycles == 10
    assert settings.alpha_min_forward_mean_return == 0.0005
