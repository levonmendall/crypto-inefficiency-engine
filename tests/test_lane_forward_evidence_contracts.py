from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


def _classes_for_lane(lane_id: str) -> set[str]:
    return {
        str(cls)
        for source in SOURCES
        if lane_id in list(source["lanes"])
        for cls in list(source["classes"])
        if bool(source.get("authoritative", True))
    }


def test_yield_keeps_source_learning_contract_and_exposes_risk_calibration_downstream():
    required = set(LANES["yield"]["required"])
    assert required == {"yield_rate", "capacity", "exit_liquidity"}
    assert "protocol-loss statistical calibration" in LANES["yield"]["downstream"]
    assert "realized-yield forward cohort" in LANES["yield"]["downstream"]


def test_volatility_requires_explicit_normalized_option_capacity_before_forward_testing():
    required = set(LANES["volatility"]["required"])
    available = _classes_for_lane("volatility")
    assert required == {
        "option_quotes",
        "option_greeks",
        "option_depth",
        "option_capacity",
    }
    assert required.issubset(available)
    capacity_sources = [
        source
        for source in SOURCES
        if "volatility" in list(source["lanes"])
        and "option_capacity" in list(source["classes"])
    ]
    assert [source["id"] for source in capacity_sources] == ["deribit-option-capacity"]


def test_other_core_lane_requirements_are_not_relaxed():
    assert set(LANES["price_discrepancy"]["required"]) == {
        "market_quotes",
        "executable_depth",
    }
    assert set(LANES["carry"]["required"]) == {
        "market_quotes",
        "funding_or_basis",
        "executable_depth",
    }
    assert set(LANES["microstructure"]["required"]) == {
        "order_book",
        "trade_flow",
    }
