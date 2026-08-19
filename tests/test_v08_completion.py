from datetime import datetime, timedelta, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.latency import EmpiricalLatencyResolver
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    ShadowCycle,
    ShadowLegAttribution,
    ShadowObservation,
    ShadowOutcome,
    Side,
    Strategy,
)

NOW = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
VENUE_PAIR = "Coinbase|HlPerp"


def opportunity() -> Opportunity:
    return Opportunity(
        id="completion-op",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=20.0,
        modeled_cost_bps=0.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=20.0,
        net_annualized_return=1.0,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=2),
    )


def row(
    event: int,
    *,
    notional: float = 1000.0,
    fillable: bool = True,
    data_latency_ms: float = 400.0,
) -> ShadowObservation:
    unhedged = 0.0 if fillable else 0.5
    return ShadowObservation(
        shadow_id=f"shadow-{event}-{notional}",
        initial_scan_id=f"event-{event}",
        verification_scan_id=f"verify-{event}-{notional}",
        opportunity_signature="same-economic-event",
        opportunity_id="op",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        notional_usd_per_leg=notional,
        target_base_quantity=10.0,
        started_at=NOW,
        verified_at=NOW + timedelta(seconds=1),
        delay_seconds=1.0,
        verification_scan_latency_ms=data_latency_ms + 200.0,
        verification_data_path_latency_ms=data_latency_ms,
        initial_net_annualized_return=0.20,
        initial_capacity_notional_usd=100000.0,
        survived=fillable,
        pair_fillable=fillable,
        pair_fillable_with_reserve=fillable,
        hedge_recovery_required=not fillable,
        pair_fill_fraction=1.0 if fillable else 0.5,
        max_leg_fill_fraction=1.0,
        unhedged_fraction=unhedged,
        partial_fill_state=not fillable,
        hedge_recovery_loss_proxy_bps=unhedged * 8.0,
        verification_net_annualized_return=0.18 if fillable else 0.0,
        outcome=ShadowOutcome.SURVIVED if fillable else ShadowOutcome.EXECUTABILITY_FAILED,
        venue_pair=VENUE_PAIR,
        leg_attribution=[
            ShadowLegAttribution(
                venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG,
                adverse_selection_bps=4.0,
            ),
            ShadowLegAttribution(
                venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT,
                adverse_selection_bps=2.0,
            ),
        ],
    )


def persist(store: EvidenceStore, rows: list[ShadowObservation], cycle_id: str = "completion") -> None:
    store.record_shadow_cycle(
        ShadowCycle(
            cycle_id=cycle_id,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
            delay_seconds=1.0,
            horizons_seconds=[1.0],
            initial_scan_id="initial",
            verification_scan_id=rows[-1].verification_scan_id,
            verification_scan_ids=[item.verification_scan_id for item in rows],
            observations=rows,
        )
    )


def test_correlated_capital_tiers_do_not_inflate_effective_sample_size(tmp_path):
    store = EvidenceStore(tmp_path / "effective.sqlite3")
    rows: list[ShadowObservation] = []
    for event in range(4):
        rows.extend([
            row(event, notional=1000.0),
            row(event, notional=5000.0),
            row(event, notional=10000.0),
        ])
    persist(store, rows)
    cfg = Settings(
        expected_order_ack_latency_ms=0.0,
        expected_hedge_latency_ms=0.0,
        empirical_latency_min_samples=6,
        empirical_latency_min_scan_samples=3,
        empirical_latency_min_effective_samples=5,
        empirical_probability_max_ci_width=1.0,
    )

    model = EmpiricalLatencyResolver(store, cfg).resolve(opportunity(), 25000.0)

    assert model.usable_for_qualification is False
    assert model.cohort_sample_count == 12
    assert model.effective_sample_size == 4
    assert "effective" in (model.reason or "")


def test_probability_confidence_width_can_block_empirical_calibration(tmp_path):
    store = EvidenceStore(tmp_path / "confidence.sqlite3")
    rows = [row(i, fillable=(i % 2 == 0)) for i in range(4)]
    persist(store, rows)
    cfg = Settings(
        expected_order_ack_latency_ms=0.0,
        expected_hedge_latency_ms=0.0,
        empirical_latency_min_samples=4,
        empirical_latency_min_scan_samples=3,
        empirical_latency_min_effective_samples=4,
        empirical_probability_confidence_level=0.95,
        empirical_probability_max_ci_width=0.20,
    )

    model = EmpiricalLatencyResolver(store, cfg).resolve(opportunity(), 1000.0)

    assert model.usable_for_qualification is False
    assert model.probability_max_ci_width is not None
    assert model.probability_max_ci_width > 0.20
    assert model.confidence_gate_passed is False


def test_collector_latency_and_assumed_execution_timing_are_separate(tmp_path):
    store = EvidenceStore(tmp_path / "separation.sqlite3")
    rows = [row(i, data_latency_ms=400.0) for i in range(3)]
    persist(store, rows)
    cfg = Settings(
        expected_order_ack_latency_ms=200.0,
        expected_hedge_latency_ms=400.0,
        empirical_latency_min_samples=3,
        empirical_latency_min_scan_samples=3,
        empirical_latency_min_effective_samples=3,
        empirical_probability_max_ci_width=1.0,
    )

    model = EmpiricalLatencyResolver(store, cfg).resolve(opportunity(), 1000.0)

    assert model.usable_for_qualification is True
    assert model.data_latency_source == "l2_request_roundtrip"
    assert model.collector_latency_reference_ms == 400.0
    assert model.assumed_order_ack_latency_ms == 200.0
    assert model.assumed_second_leg_latency_ms == 400.0
    assert model.effective_decision_to_hedge_latency_ms == 1000.0
    assert model.reference_horizon_seconds == 1.0
    assert model.execution_latency_empirical is False
    assert model.queue_position_supported is False
    assert model.maker_fill_probability is None
