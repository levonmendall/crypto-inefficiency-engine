from inefficiency_engine.profit_coverage import (
    build_profit_coverage_summary,
    profit_coverage_gaps,
)


ALPHA_FAMILIES = {
    "directional_time_series",
    "directional_reversal",
    "onchain_fundamental",
}


def by_id(summary, mechanism_id: str):
    return next(item for item in summary.mechanisms if item.mechanism_id == mechanism_id)


def test_taxonomy_coverage_does_not_masquerade_as_decision_grade_coverage():
    summary = build_profit_coverage_summary(
        version="2.5.0",
        alpha_families=ALPHA_FAMILIES,
        fundamental_authoritative_observation_count=0,
    )

    assert summary.mechanism_count == 13
    assert summary.taxonomy_coverage_fraction == 1.0
    assert summary.decision_grade_coverage_fraction < 1.0
    assert summary.paper_capable_coverage_fraction < 1.0
    assert summary.profitability_certifiable_coverage_fraction < 1.0
    assert summary.failure_conclusion_ready is False
    assert summary.failure_conclusion_blockers


def test_existing_structural_and_predictive_mechanisms_are_classified_without_overclaiming():
    summary = build_profit_coverage_summary(
        version="2.5.0",
        alpha_families=ALPHA_FAMILIES,
        fundamental_authoritative_observation_count=0,
    )

    discrepancy = by_id(summary, "price_discrepancy")
    assert discrepancy.decision_grade is True
    assert discrepancy.paper_allocation_available is True
    assert discrepancy.profitability_certification_available is False
    assert discrepancy.fully_covered is False

    momentum = by_id(summary, "trend_momentum")
    assert momentum.decision_grade is True
    assert momentum.paper_allocation_available is True
    assert momentum.profitability_certification_available is True

    reversion = by_id(summary, "mean_reversion")
    assert reversion.decision_grade is True
    assert reversion.profitability_certification_available is True

    factor = by_id(summary, "fundamental_onchain")
    assert factor.discovery_available is True
    assert factor.authoritative_data_available is False
    assert factor.decision_grade is False


def test_authoritative_factor_evidence_advances_factor_mechanism_to_decision_grade():
    summary = build_profit_coverage_summary(
        version="2.5.0",
        alpha_families=ALPHA_FAMILIES,
        fundamental_authoritative_observation_count=3,
    )
    factor = by_id(summary, "fundamental_onchain")
    assert factor.authoritative_data_available is True
    assert factor.decision_grade is True
    assert factor.paper_allocation_available is True
    assert factor.profitability_certification_available is True


def test_gap_map_names_the_next_missing_capability_instead_of_declaring_failure():
    summary = build_profit_coverage_summary(
        version="2.5.0",
        alpha_families=ALPHA_FAMILIES,
        fundamental_authoritative_observation_count=0,
    )
    gaps = profit_coverage_gaps(summary)
    by_gap = {item.mechanism_id: item for item in gaps}

    assert by_gap["yield"].next_required_capability == "authoritative point-in-time data"
    assert by_gap["microstructure"].next_required_capability == "forward/out-of-sample evidence loop"
    assert by_gap["carry"].next_required_capability == "allocator-level forward profitability settlement"
    assert by_gap["fundamental_onchain"].next_required_capability == "authoritative point-in-time data"
