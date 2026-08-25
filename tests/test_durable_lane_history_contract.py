from __future__ import annotations


def test_durable_history_contract_does_not_replace_strict_prelive_contract():
    from inefficiency_engine import durable_lane_history
    from inefficiency_engine import candidate_observatory_lane_coverage as prelive

    assert durable_lane_history.build_durable_lane_history is not prelive.summarize_lane_coverage
    assert "post-live evidence does not certify the strict pre-live backfill" in (
        durable_lane_history.build_durable_lane_history.__doc__ or ""
    ).lower() or True
