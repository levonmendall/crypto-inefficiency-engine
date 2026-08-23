from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.permanent_mechanism_worker import mechanism_forward_funnel


class FakeExecution:
    def readiness_summary(self):
        return {
            "maker_rebate": {
                "forward_outcome_count": 11,
                "incremental_qualified_cohort_count": 1,
                "full_qualified_cohort_count": 0,
                "currently_qualified": True,
                "current_promoted_candidate_count": 1,
            },
            "liquidation": {
                "forward_outcome_count": 7,
                "incremental_qualified_cohort_count": 0,
                "full_qualified_cohort_count": 1,
                "currently_qualified": True,
                "current_promoted_candidate_count": 2,
            },
        }


def test_mechanism_forward_funnel_reports_durable_qualification_progress():
    # The permanent plane must expose progress at each mechanism-qualification stage.
    cycle = SimpleNamespace(
        current_specs=5,
        trials_recorded=3,
        outcomes_matured=2,
        promoted_candidates=3,
    )

    funnel = mechanism_forward_funnel(FakeExecution(), cycle)

    assert funnel["mechanism_count"] == 2
    assert funnel["forward_outcome_count"] == 18
    assert funnel["incremental_qualified_cohort_count"] == 1
    assert funnel["full_qualified_cohort_count"] == 1
    assert funnel["currently_qualified_mechanism_count"] == 2
    assert funnel["current_promoted_candidate_count"] == 3
    assert funnel["cycle_promoted_candidate_count"] == 3
