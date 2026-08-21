from __future__ import annotations

from inefficiency_engine.config import Settings
from inefficiency_engine.strategy_evidence_read import (
    _CANONICAL_ALPHA_STRATEGIES,
    diagnose_alpha_strategy,
)


def _outcome(asset: str, index: int, *, direction: str = "long") -> dict[str, object]:
    return {
        "strategy_id": "mean_reversion_multi_horizon_v1",
        "asset": asset,
        "direction": direction,
        "regime": "normal" if index % 2 == 0 else "high_vol",
        "realized_net_return": 0.009,
    }


def test_strategy_inventory_exposes_all_current_executable_alpha_strategies():
    rows = {strategy_id for _, _, strategy_id, _ in _CANONICAL_ALPHA_STRATEGIES}
    assert len(rows) == 15
    assert "mean_reversion_liquidity_conditioned_v1" in rows
    assert "public_trade_flow_imbalance_v1" in rows
    assert "public_trade_flow_lead_lag_v1" in rows
    assert "onchain_factor_breadth_v1" in rows
    assert "event_driven_surprise_v1" in rows


def test_read_model_does_not_treat_cross_asset_rows_as_fully_independent():
    settings = Settings()
    outcomes = [
        _outcome(f"ASSET{asset_index}", sample_index)
        for asset_index in range(10)
        for sample_index in range(3)
    ]
    row = diagnose_alpha_strategy(
        strategy_id="mean_reversion_multi_horizon_v1",
        name="Multi-horizon mean reversion",
        family="directional_reversal",
        signal_count=30,
        outcomes=outcomes,
        allocator={},
        settings=settings,
        strategy_count=15,
    )
    assert row["candidate_local_forward_outcome_count"] == 3
    assert row["independent_forward_outcome_count"] < settings.alpha_min_forward_samples
    assert row["state"] == "collecting"


def test_read_model_matches_discounted_pool_when_local_minimum_is_met():
    settings = Settings()
    outcomes = [_outcome("BTC", index) for index in range(3)]
    for asset_index, asset in enumerate(("ETH", "SOL", "AVAX"), start=1):
        outcomes.extend(_outcome(asset, asset_index * 100 + index) for index in range(26))

    row = diagnose_alpha_strategy(
        strategy_id="mean_reversion_multi_horizon_v1",
        name="Multi-horizon mean reversion",
        family="directional_reversal",
        signal_count=len(outcomes),
        outcomes=outcomes,
        allocator={},
        settings=settings,
        strategy_count=15,
    )
    assert row["candidate_local_forward_outcome_count"] >= 3
    assert row["independent_forward_outcome_count"] >= settings.alpha_min_forward_samples
    assert row["diagnostic_mirrors_runtime_alpha_qualification"] is True
    assert row["state"] == "certifying"
