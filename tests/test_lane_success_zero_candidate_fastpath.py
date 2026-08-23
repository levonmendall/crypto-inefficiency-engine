from inefficiency_engine.config import Settings
from inefficiency_engine.lane_success_runtime import (
    LaneSuccessQualifiedOpportunityAllocatorService,
)


class _ExplodingLaneSuccess:
    def market_regime(self, *args, **kwargs):
        raise AssertionError("zero-candidate allocation must not run regime calibration")

    def adjust_and_diversify(self, *args, **kwargs):
        raise AssertionError("zero-candidate allocation must not run diversification")


def test_zero_candidate_allocation_returns_fail_closed_plan_without_calibration():
    allocator = object.__new__(LaneSuccessQualifiedOpportunityAllocatorService)
    allocator.settings = Settings.from_env()
    allocator.lane_success = _ExplodingLaneSuccess()
    allocator._active_candidates_with_diagnostics = lambda: (
        [],
        [
            {
                "family": "qualified_opportunity_bridge",
                "error_type": "QualifiedOpportunitySnapshotUnavailableOrStale",
                "reason": "no active qualified candidate envelope",
            }
        ],
        [{"candidate_id": "stale-bridge", "reason": "stale"}],
    )
    allocator._mechanism_proxy_candidates = lambda **kwargs: (
        [],
        [{"candidate_id": "stale-mechanism", "reason": "stale"}],
    )

    plan = allocator.allocate_sync(total_capital_usd=250_000.0)

    assert plan.candidate_count == 0
    assert plan.allocated_capital_usd == 0.0
    assert plan.unused_cash_usd == 250_000.0
    assert plan.allocations == []
    assert plan.authorizes_execution is False
    assert plan.live_execution_eligible is False
    assert plan.paper_only is True
    assert plan.family_failures[0]["family"] == "qualified_opportunity_bridge"
    assert {row["candidate_id"] for row in plan.skipped} == {
        "stale-bridge",
        "stale-mechanism",
    }
