from types import SimpleNamespace

from inefficiency_engine.portfolio_risk import PortfolioRiskOverlay


def alpha(candidate_id: str, strategy: str, capital: float, exposure: str):
    return SimpleNamespace(
        candidate_id=candidate_id,
        family="alpha",
        strategy=strategy,
        capital_required_usd=capital,
        exposure_kind=exposure,
    )


def neutral(candidate_id: str, capital: float):
    return SimpleNamespace(
        candidate_id=candidate_id,
        family="core_cex",
        strategy="spot_perp_basis",
        capital_required_usd=capital,
        exposure_kind="market_neutral",
    )


def test_market_neutral_opportunities_do_not_consume_directional_budget():
    overlay = PortfolioRiskOverlay(SimpleNamespace(), total_capital_usd=100000.0)
    item = neutral("neutral", 30000.0)
    assert overlay.decision(item).accepted is True
    overlay.register(item)
    state = overlay.snapshot()
    assert state.market_neutral_capital_usd == 30000.0
    assert state.directional_capital_usd == 0.0
    assert state.alpha_capital_usd == 0.0


def test_same_direction_alpha_is_blocked_before_it_can_stack_portfolio_beta():
    settings = SimpleNamespace(
        allocator_max_alpha_fraction=0.60,
        allocator_max_directional_fraction=0.50,
        allocator_max_same_direction_fraction=0.25,
        allocator_max_alpha_strategy_fraction=0.40,
    )
    overlay = PortfolioRiskOverlay(settings, total_capital_usd=100000.0)
    first = alpha("btc-long", "momentum", 15000.0, "directional_long")
    second = alpha("eth-long", "reversal", 15000.0, "directional_long")
    assert overlay.decision(first).accepted is True
    overlay.register(first)
    decision = overlay.decision(second)
    assert decision.accepted is False
    assert decision.reason == "long directional risk budget"


def test_single_predictive_strategy_cannot_dominate_alpha_book():
    settings = SimpleNamespace(
        allocator_max_alpha_fraction=0.80,
        allocator_max_directional_fraction=0.80,
        allocator_max_same_direction_fraction=0.80,
        allocator_max_alpha_strategy_fraction=0.20,
    )
    overlay = PortfolioRiskOverlay(settings, total_capital_usd=100000.0)
    first = alpha("btc-long", "momentum", 15000.0, "directional_long")
    second = alpha("eth-long", "momentum", 10000.0, "directional_long")
    overlay.register(first)
    decision = overlay.decision(second)
    assert decision.accepted is False
    assert decision.reason == "predictive alpha strategy concentration budget"


def test_long_and_short_exposure_report_net_directional_risk_separately():
    settings = SimpleNamespace(
        allocator_max_alpha_fraction=0.80,
        allocator_max_directional_fraction=0.60,
        allocator_max_same_direction_fraction=0.40,
        allocator_max_alpha_strategy_fraction=0.40,
    )
    overlay = PortfolioRiskOverlay(settings, total_capital_usd=100000.0)
    long = alpha("btc-long", "momentum", 15000.0, "directional_long")
    short = alpha("eth-short", "reversal", 10000.0, "directional_short")
    overlay.register(long)
    overlay.register(short)
    state = overlay.snapshot()
    assert state.directional_capital_usd == 25000.0
    assert state.directional_long_capital_usd == 15000.0
    assert state.directional_short_capital_usd == 10000.0
    assert state.directional_net_capital_usd == 5000.0
