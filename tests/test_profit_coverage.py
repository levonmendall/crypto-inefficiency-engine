from inefficiency_engine.profit_coverage import (
    build_profit_coverage_summary,
    profit_coverage_gaps,
)


ALPHA_FAMILIES = {
    "directional_time_series",
    "directional_reversal",
    "onchain_fundamental",
    "cross_sectional_relative_value",
    "microstructure_orderflow",
    "event_driven",
}


def by_id(summary, mechanism_id: str):
    return next(item for item in summary.mechanisms if item.mechanism_id == mechanism_id)


def test_taxonomy_coverage_does_not_masquerade_as_decision_grade_coverage():
    summary = build_profit_coverage_summary(version="3.0.0", alpha_families=ALPHA_FAMILIES)
    assert summary.mechanism_count == 13
    assert summary.taxonomy_coverage_fraction == 1.0
    assert summary.decision_grade_coverage_fraction < 1.0
    assert summary.paper_capable_coverage_fraction < 1.0
    assert summary.profitability_certifiable_coverage_fraction < 1.0
    assert summary.failure_conclusion_ready is False
    assert summary.failure_conclusion_blockers


def test_active_structural_and_alpha_mechanisms_advance_without_overclaiming_provider_dependent_families():
    summary = build_profit_coverage_summary(version="3.0.0", alpha_families=ALPHA_FAMILIES)

    discrepancy = by_id(summary, "price_discrepancy")
    assert discrepancy.decision_grade is True
    assert discrepancy.paper_allocation_available is True
    assert discrepancy.profitability_certification_available is False

    for mechanism_id in ("trend_momentum", "mean_reversion", "cross_sectional_relative_value", "microstructure"):
        row = by_id(summary, mechanism_id)
        assert row.decision_grade is True
        assert row.paper_allocation_available is True
        assert row.profitability_certification_available is True

    factor = by_id(summary, "fundamental_onchain")
    event = by_id(summary, "event_driven")
    yield_row = by_id(summary, "yield")
    volatility = by_id(summary, "volatility")
    distress = by_id(summary, "liquidation_distress")
    assert factor.authoritative_data_available is False
    assert event.authoritative_data_available is False
    assert yield_row.economics_model_available is True
    assert volatility.economics_model_available is True
    assert distress.economics_model_available is True
    assert all(not row.decision_grade for row in (factor, event, yield_row, volatility, distress))


def test_authoritative_provider_evidence_advances_only_families_with_complete_forward_pipeline():
    summary = build_profit_coverage_summary(
        version="3.0.0",
        alpha_families=ALPHA_FAMILIES,
        fundamental_authoritative_observation_count=3,
        event_authoritative_observation_count=4,
        yield_authoritative_observation_count=5,
        option_authoritative_observation_count=6,
        distress_authoritative_observation_count=7,
    )
    assert by_id(summary, "fundamental_onchain").decision_grade is True
    assert by_id(summary, "event_driven").decision_grade is True
    assert by_id(summary, "yield").authoritative_data_available is True
    assert by_id(summary, "yield").decision_grade is False
    assert by_id(summary, "volatility").authoritative_data_available is True
    assert by_id(summary, "volatility").decision_grade is False
    assert by_id(summary, "liquidation_distress").authoritative_data_available is True
    assert by_id(summary, "liquidation_distress").decision_grade is False


def test_gap_map_names_next_empirical_requirement_after_architecture_closure():
    summary = build_profit_coverage_summary(version="3.0.0", alpha_families=ALPHA_FAMILIES)
    gaps = {item.mechanism_id: item for item in profit_coverage_gaps(summary)}

    assert gaps["yield"].next_required_capability == "authoritative point-in-time data"
    assert gaps["volatility"].next_required_capability == "authoritative point-in-time data"
    assert gaps["event_driven"].next_required_capability == "authoritative point-in-time data"
    assert gaps["liquidation_distress"].next_required_capability == "authoritative point-in-time data"
    assert gaps["liquidity_provision"].next_required_capability == "forward/out-of-sample evidence loop"
    assert gaps["capital_location_settlement"].next_required_capability == "forward/out-of-sample evidence loop"
    assert gaps["carry"].next_required_capability == "allocator-level forward profitability settlement"
    assert gaps["microstructure"].next_required_capability == "close remaining sub-mechanism coverage gaps"
