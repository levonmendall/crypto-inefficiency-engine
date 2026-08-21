from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.priority_source_event_yield import (
    _alpha_asset,
    _liquidity_coverage_score,
)


def _lane(*sources):
    return SimpleNamespace(sources=list(sources))


def test_morpho_wrapper_assets_normalize_to_market_underlyings():
    assert _alpha_asset("WETH") == "ETH"
    assert _alpha_asset("WBTC") == "BTC"
    assert _alpha_asset("USDC") == "USDC"


def test_protocol_liquidity_coverage_factor_is_bounded_and_neutral_at_half_coverage():
    assert _liquidity_coverage_score(liquidity_usd=50.0, supply_usd=100.0) == 0.0
    assert _liquidity_coverage_score(liquidity_usd=100.0, supply_usd=100.0) == 1.0
    assert _liquidity_coverage_score(liquidity_usd=0.0, supply_usd=100.0) == -1.0
    assert _liquidity_coverage_score(liquidity_usd=500.0, supply_usd=100.0) == 1.0


def test_yield_forward_trial_requires_complete_economics_not_only_present_fields():
    incomplete = _lane(
        {
            "admitted": True,
            "classes": ["yield_rate", "capacity", "exit_liquidity"],
            "economic_fields_complete": False,
        }
    )
    assert (
        GovernedMechanismExecutionService._semantic_economics_ready("yield", incomplete)
        is False
    )

    complete = _lane(
        {
            "admitted": True,
            "classes": ["yield_rate", "capacity", "exit_liquidity"],
            "economic_fields_complete": True,
        }
    )
    assert (
        GovernedMechanismExecutionService._semantic_economics_ready("yield", complete)
        is True
    )


def test_semantic_economics_guard_does_not_change_other_mechanism_gates():
    empty = _lane()
    for mechanism_id in (
        "liquidity_provision",
        "volatility",
        "liquidation_distress",
        "capital_location_settlement",
    ):
        assert (
            GovernedMechanismExecutionService._semantic_economics_ready(
                mechanism_id,
                empty,
            )
            is True
        )
