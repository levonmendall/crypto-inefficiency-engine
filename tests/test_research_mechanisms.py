from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)
from inefficiency_engine.research_mechanisms import (
    CapitalLocationResearchService,
    DistressOpportunityObservation,
    DistressResearchService,
    MarketMakingResearchService,
    OptionQuoteObservation,
    VolatilityResearchService,
    YieldObservation,
    YieldResearchService,
)


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def test_yield_graph_prices_costs_and_risk_but_never_self_promotes(tmp_path):
    store = EvidenceStore(tmp_path / "yield.sqlite3")
    service = YieldResearchService(store)
    service.record(YieldObservation(
        provider="authoritative-yield-feed",
        protocol="ExampleLend",
        venue_or_chain="ethereum",
        asset="USDC",
        kind="lending",
        observed_at=NOW,
        as_of_at=NOW,
        gross_apy=0.12,
        capacity_usd=1_000_000.0,
        holding_hours=24.0 * 30.0,
        entry_exit_cost_bps=20.0,
        credit_or_protocol_risk_haircut_apy=0.02,
        slashing_or_liquidation_risk_haircut_apy=0.005,
        incentive_decay_haircut_apy=0.01,
        authoritative=True,
        commercial_use_permitted=True,
    ))
    rows = service.candidates(now=NOW)
    assert len(rows) == 1
    row = rows[0]
    assert row.conservative_net_apy < row.gross_apy
    assert row.total_risk_haircut_apy == pytest.approx(0.035)
    assert row.paper_allocation_eligible is False
    assert "statistical promotion" in row.blocker


def test_options_vrp_engine_requires_authoritative_quotes_and_remains_research_only(tmp_path):
    store = EvidenceStore(tmp_path / "options.sqlite3")
    service = VolatilityResearchService(store)
    expiry = NOW + timedelta(days=30)
    for option_type, delta in [("call", 0.52), ("put", -0.48)]:
        service.record(OptionQuoteObservation(
            provider="authoritative-options-feed",
            venue="Deribit",
            underlying="BTC",
            expiry=expiry,
            strike=60000.0,
            option_type=option_type,
            bid=2900.0,
            ask=3000.0,
            implied_volatility=0.72,
            delta=delta,
            gamma=0.00001,
            vega=120.0,
            observed_at=NOW,
            authoritative=True,
            commercial_use_permitted=True,
        ))
    rows = service.candidates(realized_volatility_by_underlying={"BTC": 0.50}, hedge_cost_bps=8.0)
    assert len(rows) == 1
    row = rows[0]
    assert row.direction == "short_volatility"
    assert row.volatility_risk_premium == pytest.approx(0.22)
    assert row.paper_allocation_eligible is False
    assert row.blockers


def test_market_making_simulator_refuses_decision_grade_without_queue_fill_and_adverse_selection():
    book = OrderBookSnapshot(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        bids=[OrderBookLevel(price=59990.0, size=1.0)],
        asks=[OrderBookLevel(price=60010.0, size=1.0)],
        observed_at=NOW,
        source="test",
    )
    blocked = MarketMakingResearchService.simulate(book)
    assert blocked.decision_grade is False
    assert blocked.expected_net_bps_per_completed_roundtrip is None
    assert "empirical maker fill probability is unavailable" in blocked.blockers
    assert blocked.paper_allocation_eligible is False

    modeled = MarketMakingResearchService.simulate(
        book,
        empirical_fill_probability=0.7,
        maker_rebate_bps_roundtrip=1.0,
        adverse_selection_bps=1.5,
        inventory_penalty_bps=0.5,
        queue_model_empirical=True,
    )
    assert modeled.economics_complete is True
    assert modeled.decision_grade is True
    assert modeled.expected_net_bps_per_completed_roundtrip is not None
    assert modeled.paper_allocation_eligible is False


def test_distress_engine_haircuts_capture_settlement_and_failure_recovery(tmp_path):
    store = EvidenceStore(tmp_path / "distress.sqlite3")
    service = DistressResearchService(store)
    service.record(DistressOpportunityObservation(
        provider="authoritative-auction-feed",
        venue_or_protocol="ExampleProtocol",
        asset="ETH",
        kind="liquidation",
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        capacity_usd=10000.0,
        gross_reward_usd=500.0,
        execution_cost_usd=50.0,
        worst_case_recovery_loss_usd=250.0,
        capture_probability=0.8,
        settlement_probability=0.9,
        authoritative=True,
        commercial_use_permitted=True,
    ))
    rows = service.candidates(now=NOW)
    assert len(rows) == 1
    row = rows[0]
    expected = 0.72 * 450.0 - 0.28 * 300.0
    assert row.conservative_expected_profit_usd == pytest.approx(expected)
    assert row.paper_allocation_eligible is False
    assert row.blockers


def test_capital_location_optimizer_learns_only_from_persisted_opportunity_history(tmp_path):
    store = EvidenceStore(tmp_path / "location.sqlite3")
    service = CapitalLocationResearchService(store)
    empty = service.plan(reserve_capital_usd=10000.0)
    assert empty.recommendations == []
    assert empty.blockers

    opportunity = Opportunity(
        id="basis-btc",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="BTC",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="BTC", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=2.0,
        modeled_cost_bps=1.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=1.0,
        net_annualized_return=0.30,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[opportunity],
        providers=[],
        started_at=NOW,
        completed_at=NOW,
    )
    plan = service.plan(reserve_capital_usd=10000.0, max_location_fraction=0.6)
    assert len(plan.recommendations) == 2
    assert all(row.recommended_reserve_usd > 0 for row in plan.recommendations)
    assert plan.allocation_authority is False
    assert plan.live_execution_authority is False
    assert plan.blockers
