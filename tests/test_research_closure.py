from datetime import datetime, timedelta, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)
from inefficiency_engine.research_closure import (
    ResearchClosureService,
    classify_research_worker_state,
)
from inefficiency_engine.research_mechanisms import (
    CapitalLocationPlan,
    CapitalLocationScore,
)


def _book(*, observed_at: datetime, bid: float, ask: float) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="venue-a",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        bids=[OrderBookLevel(price=bid, size=10.0)],
        asks=[OrderBookLevel(price=ask, size=10.0)],
        observed_at=observed_at,
        source="test",
    )


def _positive_opportunity(observed_at: datetime, *, venue: str = "venue-a") -> Opportunity:
    return Opportunity(
        id=f"op-{observed_at.timestamp()}-{venue}",
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue=venue,
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol="BTC-USD",
                reference_price=100.0,
            ),
            OpportunityLeg(
                venue="venue-b",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.SHORT,
                symbol="BTC-USD",
                reference_price=101.0,
            ),
        ],
        gross_edge_bps_per_hour=100.0,
        modeled_cost_bps=20.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=0.02,
        net_edge_bps_per_hour=79.98,
        net_annualized_return=0.25,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=2),
    )


def test_worker_cadence_state_distinguishes_wait_late_stall_and_failure():
    now = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    assert classify_research_worker_state(
        now=now,
        heartbeat_at=now - timedelta(seconds=5),
        heartbeat_state="success",
        error_type=None,
        last_cycle_at=now - timedelta(seconds=30),
        expected_interval_seconds=60,
        stale_after_seconds=180,
    ) == "waiting_scheduled"
    assert classify_research_worker_state(
        now=now,
        heartbeat_at=now - timedelta(seconds=5),
        heartbeat_state="running",
        error_type=None,
        last_cycle_at=now - timedelta(seconds=90),
        expected_interval_seconds=60,
        stale_after_seconds=180,
    ) == "late"
    assert classify_research_worker_state(
        now=now,
        heartbeat_at=now - timedelta(seconds=400),
        heartbeat_state="success",
        error_type=None,
        last_cycle_at=now - timedelta(seconds=400),
        expected_interval_seconds=60,
        stale_after_seconds=180,
    ) == "stalled"
    assert classify_research_worker_state(
        now=now,
        heartbeat_at=now - timedelta(seconds=5),
        heartbeat_state="success",
        error_type="RuntimeError",
        last_cycle_at=now - timedelta(seconds=5),
        expected_interval_seconds=60,
        stale_after_seconds=180,
    ) == "failed"


def test_rejection_funnel_exposes_best_near_miss_without_lowering_hurdle(tmp_path):
    store = EvidenceStore(tmp_path / "closure.db")
    service = ResearchClosureService(store, Settings())
    observed = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    quotes = [
        MarketQuote(
            venue="venue-a",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-A",
            quote_currency="USD",
            bid=99.9,
            ask=100.0,
            mid=99.95,
            observed_at=observed,
            source="test",
        ),
        MarketQuote(
            venue="venue-b",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-B",
            quote_currency="USD",
            bid=100.1,
            ask=100.2,
            mid=100.15,
            observed_at=observed,
            source="test",
        ),
    ]
    rows = service.record_rejection_funnels(
        market_quotes=quotes,
        funding_quotes=[],
        opportunities=[],
        order_books=[_book(observed_at=observed, bid=100.0, ask=100.1)],
        microstructure_emitted_count=0,
        observed_at=observed,
    )
    price = rows["price_discrepancy"]
    assert price.raw_candidate_count > 0
    assert price.emitted_candidate_count == 0
    assert price.best_net_economics is not None
    assert price.required_net_economics == Settings().min_net_annualized_return
    assert price.best_net_economics < price.required_net_economics
    assert price.dominant_rejection_gate in {"net_return_hurdle", "gross_edge_not_positive"}
    persisted = service.ledger.latest_rejection("price_discrepancy")
    assert persisted is not None
    assert persisted.snapshot_id == price.snapshot_id


def test_capital_location_recommendation_is_forward_evaluated_without_authority(tmp_path):
    store = EvidenceStore(tmp_path / "location.db")
    service = ResearchClosureService(store, Settings())
    start = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    plan = CapitalLocationPlan(
        observed_at=start,
        reserve_capital_usd=100_000.0,
        historical_opportunity_count=10,
        recommendations=[
            CapitalLocationScore(
                venue="venue-a",
                asset="BTC",
                opportunity_count=8,
                mean_positive_net_annualized_return=0.20,
                max_positive_net_annualized_return=0.40,
                raw_score=8.0,
                recommended_weight=0.8,
                recommended_reserve_usd=80_000.0,
            ),
            CapitalLocationScore(
                venue="venue-c",
                asset="ETH",
                opportunity_count=2,
                mean_positive_net_annualized_return=0.05,
                max_positive_net_annualized_return=0.08,
                raw_score=2.0,
                recommended_weight=0.2,
                recommended_reserve_usd=20_000.0,
            ),
        ],
    )
    first = service.run_capital_location_forward_cycle(plan, now=start, horizon_hours=1.0)
    assert first["trial_count"] == 1
    assert first["outcome_count"] == 0

    future = start + timedelta(minutes=30)
    store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[_positive_opportunity(future, venue="venue-a")],
        providers=[],
        started_at=future,
        completed_at=future,
    )
    matured = service.run_capital_location_forward_cycle(
        plan,
        now=start + timedelta(hours=1, minutes=1),
        horizon_hours=1.0,
    )
    assert matured["outcome_count"] == 1
    assert matured["mean_incremental_option_value"] > 0
    assert matured["transfer_evidence_complete"] is False
    assert matured["decision_grade"] is False


def test_maker_shadow_collects_cross_through_without_claiming_queue_fill(tmp_path):
    store = EvidenceStore(tmp_path / "maker.db")
    service = ResearchClosureService(store, Settings())
    start = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    first = service.run_maker_shadow_cycle(
        [_book(observed_at=start, bid=100.0, ask=101.0)],
        now=start,
        horizon_seconds=60.0,
    )
    assert first["trial_count"] == 1
    assert first["outcome_count"] == 0

    later = start + timedelta(seconds=61)
    matured = service.run_maker_shadow_cycle(
        [_book(observed_at=later, bid=99.0, ask=99.5)],
        now=later,
        horizon_seconds=60.0,
    )
    assert matured["outcome_count"] == 1
    assert matured["crossed_through_count"] == 1
    assert matured["queue_fill_confirmed_count"] == 0
    assert matured["queue_position_observable"] is False
    assert matured["decision_grade"] is False
