from types import SimpleNamespace

import pytest

from inefficiency_engine.operating_state_read import (
    rebuild_live_action_queue,
    reconcile_live_operating_states,
)


SETTINGS = SimpleNamespace(
    alpha_min_forward_samples=30,
    alpha_min_forward_mean_return=0.0005,
    alpha_min_hit_rate_lower_bound=0.50,
    operating_certification_min_settled_trials=20,
    operating_certification_min_profitable_rate_lower=0.50,
)


def _payload(state="collecting", **overrides):
    row = {
        "mechanism_id": "trend_momentum",
        "name": "Trend / Momentum",
        "state": state,
        "provider_ready": True,
        "primary_reason": "cached reason",
        "next_action": "cached action",
        "blockers": [],
    }
    row.update(overrides)
    return {"mechanisms": [row]}


def _strategy(state, **overrides):
    row = {
        "strategy_id": "strategy_v1",
        "state": state,
        "primary_reason": f"live {state}",
        "forward_signal_count": 1,
        "independent_forward_outcome_count": 0,
        "settled_allocator_outcome_count": 0,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "state",
    [
        "provider_gap",
        "collecting",
        "poor_economics",
        "statistical_failure",
        "execution_blocked",
        "settlement_blocked",
        "certifying",
        "certified",
    ],
)
def test_every_operating_label_can_be_reconciled_from_live_strategy_evidence(state):
    payload = _payload(strategy_evidence=[_strategy(state)])
    result = reconcile_live_operating_states(payload, SETTINGS)
    assert result["mechanisms"][0]["state"] == state
    assert result["mechanisms"][0]["live_operating_state_reconciled"] is True


def test_provider_failure_has_highest_priority():
    payload = _payload(
        state="certified",
        provider_ready=False,
        profitability_certified=True,
        strategy_evidence=[_strategy("certified", settled_allocator_outcome_count=25)],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "provider_gap"


def test_single_strategy_updates_stale_lane_state():
    payload = _payload(
        state="collecting",
        strategy_evidence=[_strategy("poor_economics")],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "poor_economics"
    assert row["primary_reason"] == "live poor_economics"


def test_identical_multi_strategy_state_updates_stale_lane_state():
    payload = _payload(
        state="collecting",
        strategy_evidence=[
            _strategy("certifying", strategy_id="a"),
            _strategy("certifying", strategy_id="b"),
        ],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "certifying"


def test_execution_block_is_not_cleared_without_downstream_proof():
    payload = _payload(
        state="execution_blocked",
        strategy_evidence=[_strategy("certifying", settled_allocator_outcome_count=0)],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "execution_blocked"


def test_execution_block_clears_when_realized_allocator_evidence_exists():
    payload = _payload(
        state="execution_blocked",
        strategy_evidence=[_strategy("certifying", settled_allocator_outcome_count=1)],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "certifying"
    assert row["settled_allocator_outcome_count"] == 1


def test_certified_lane_can_degrade_from_new_strategy_evidence():
    payload = _payload(
        state="certified",
        strategy_evidence=[_strategy("statistical_failure")],
    )
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "statistical_failure"


def test_non_strategy_lane_reconciles_poor_economics_from_latest_projected_economics():
    payload = {
        "mechanisms": [
            {
                "mechanism_id": "yield",
                "name": "Yield",
                "state": "collecting",
                "provider_ready": True,
                "authoritative_observation_count": 4,
                "best_net_economics": -0.01,
            }
        ]
    }
    row = reconcile_live_operating_states(payload, SETTINGS)["mechanisms"][0]
    assert row["state"] == "poor_economics"


def test_non_strategy_allocator_evidence_moves_from_certifying_to_certified():
    base = {
        "mechanism_id": "carry",
        "name": "Carry",
        "state": "certifying",
        "provider_ready": True,
        "settled_allocator_outcome_count": 20,
        "allocator_mean_net_return_ci_lower": 0.001,
        "allocator_profitable_rate_ci_lower": 0.60,
        "allocator_realized_profit_usd": 100.0,
    }
    row = reconcile_live_operating_states({"mechanisms": [base]}, SETTINGS)["mechanisms"][0]
    assert row["state"] == "certified"


def test_action_queue_is_rebuilt_from_reconciled_state():
    mechanisms = {
        "mechanisms": [
            {
                "mechanism_id": "a",
                "name": "A",
                "state": "certified",
                "primary_reason": "done",
            },
            {
                "mechanism_id": "b",
                "name": "B",
                "state": "poor_economics",
                "primary_reason": "negative",
                "next_action": "observe",
                "strategy_evidence": [],
            },
        ]
    }
    queue = rebuild_live_action_queue(mechanisms)
    assert queue["count"] == 1
    assert queue["actions"][0]["mechanism_id"] == "b"
    assert queue["actions"][0]["state"] == "poor_economics"


def test_degraded_formerly_certified_lane_reappears_in_queue():
    payload = _payload(
        state="certified",
        strategy_evidence=[_strategy("poor_economics")],
    )
    reconciled = reconcile_live_operating_states(payload, SETTINGS)
    queue = rebuild_live_action_queue(reconciled)
    assert queue["count"] == 1
    assert queue["actions"][0]["state"] == "poor_economics"
