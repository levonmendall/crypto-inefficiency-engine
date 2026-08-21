from inefficiency_engine.research_closure_worker import (
    _canonical_capabilities,
    _decision_grade_location_projection,
)


def test_research_only_capital_location_is_not_dashboard_forward_testable():
    research = {
        "available": True,
        "trial_count": 12,
        "outcome_count": 11,
        "mean_incremental_option_value": 0.03,
        "positive_incremental_rate": 0.72,
        "transfer_evidence_complete": False,
        "decision_grade": False,
    }

    assert _decision_grade_location_projection(research) == {}


def test_capital_location_projection_requires_both_transfer_and_decision_grade():
    transfer_only = {
        "available": True,
        "trial_count": 3,
        "outcome_count": 3,
        "transfer_evidence_complete": True,
        "decision_grade": False,
    }
    decision_only = {
        "available": True,
        "trial_count": 3,
        "outcome_count": 3,
        "transfer_evidence_complete": False,
        "decision_grade": True,
    }
    complete = {
        "available": True,
        "trial_count": 3,
        "outcome_count": 3,
        "transfer_evidence_complete": True,
        "decision_grade": True,
    }

    assert _decision_grade_location_projection(transfer_only) == {}
    assert _decision_grade_location_projection(decision_only) == {}
    assert _decision_grade_location_projection(complete) == complete


def test_canonical_capabilities_do_not_claim_capital_location_connection():
    capabilities = _canonical_capabilities()

    assert capabilities["capital_location_forward_testing"] is True
    assert capabilities["capital_location_transfer_evidence_connected"] is False
    assert capabilities["capital_location_allocation_grade"] is False
