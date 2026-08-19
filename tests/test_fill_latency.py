from datetime import datetime, timedelta, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.fill_model import reconstruct_partial_fill_state
from inefficiency_engine.latency import build_empirical_latency_model
from inefficiency_engine.models import (
    EmpiricalLatencyModel,
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    ShadowCycle,
    ShadowLegAttribution,
    ShadowObservation,
    ShadowOutcome,
    Side,
    Strategy,
)
from inefficiency_engine.shadow import build_leg_attribution, reconstruct_pair_fill_state

NOW = datetime(2026, 8, 18, 23, 30, tzinfo=timezone.utc)


def opportunity() -> Opportunity:
    return Opportunity(
        id="latency-op",
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


def books(*, spot_size=100.0, perp_size=100.0, observed_at=NOW):
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=99.9, size=spot_size)],
            asks=[OrderBookLevel(price=100.0, size=spot_size)],
            observed_at=observed_at, source="fixture", request_latency_ms=180.0,
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=[OrderBookLevel(price=101.0, size=perp_size)],
            asks=[OrderBookLevel(price=101.1, size=perp_size)],
            observed_at=observed_at, source="fixture", request_latency_ms=220.0,
        ),
    ]


def settings(**overrides) -> Settings:
    base = dict(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        hedge_recovery_buffer_bps=0.0,
        expected_order_ack_latency_ms=0.0,
        expected_hedge_latency_ms=0.0,
        empirical_latency_min_effective_samples=1,
        empirical_probability_max_ci_width=1.0,
    )
    base.update(overrides)
    return Settings(**base)


def test_reconstruct_pair_fillability_and_asymmetric_hedge_recovery():
    initial = books(spot_size=100.0, perp_size=100.0)
    verification = books(spot_size=20.0, perp_size=5.0)
    attribution, _ = build_leg_attribution(opportunity(), initial, verification, target_quantity=10.0)
    pair_fillable, reserve_fillable, hedge_recovery = reconstruct_pair_fill_state(
        attribution, reserve_ratio=1.25
    )
    partial = reconstruct_partial_fill_state(attribution, reserve_ratio=1.25)

    assert attribution[0].verification_depth_multiple == 2.0
    assert attribution[1].verification_depth_multiple == 0.5
    assert pair_fillable is False
    assert reserve_fillable is False
    assert hedge_recovery is True
    assert partial.pair_fill_fraction == 0.5
    assert partial.max_leg_fill_fraction == 1.0
    assert partial.unhedged_fraction == 0.5
    assert partial.partial_fill_state is True


def _shadow_observation(index: int, *, fillable: bool, reserve: bool, survived: bool, adverse: float) -> ShadowObservation:
    unhedged = 0.0 if fillable else 0.5
    return ShadowObservation(
        shadow_id=f"shadow-{index}",
        initial_scan_id=f"initial-{index}",
        verification_scan_id=f"verification-{index}",
        opportunity_signature="sig",
        opportunity_id="op",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        notional_usd_per_leg=1000.0,
        target_base_quantity=10.0,
        started_at=NOW,
        verified_at=NOW + timedelta(seconds=1),
        delay_seconds=1.0,
        initial_scan_latency_ms=350.0,
        verification_scan_latency_ms=400.0 + (index * 100.0),
        initial_data_path_latency_ms=180.0,
        verification_data_path_latency_ms=200.0 + (index * 20.0),
        initial_net_annualized_return=0.20,
        initial_capacity_notional_usd=50000.0,
        survived=survived,
        pair_fillable=fillable,
        pair_fillable_with_reserve=reserve,
        hedge_recovery_required=not fillable and index == 2,
        pair_fill_fraction=1.0 if fillable else 0.5,
        max_leg_fill_fraction=1.0,
        unhedged_fraction=unhedged,
        partial_fill_state=not fillable,
        hedge_recovery_loss_proxy_bps=unhedged * adverse,
        verification_net_annualized_return=0.18 if survived else 0.0,
        outcome=ShadowOutcome.SURVIVED if survived else ShadowOutcome.EXECUTABILITY_FAILED,
        leg_attribution=[
            ShadowLegAttribution(
                venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG,
                adverse_selection_bps=adverse,
            ),
            ShadowLegAttribution(
                venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT,
                adverse_selection_bps=adverse / 2.0,
            ),
        ],
    )


def test_empirical_latency_model_uses_l2_latency_and_fill_reconstruction(tmp_path):
    store = EvidenceStore(tmp_path / "latency.sqlite3")
    observations = [
        _shadow_observation(0, fillable=True, reserve=True, survived=True, adverse=1.0),
        _shadow_observation(1, fillable=True, reserve=True, survived=True, adverse=2.0),
        _shadow_observation(2, fillable=False, reserve=False, survived=False, adverse=4.0),
    ]
    store.record_shadow_cycle(
        ShadowCycle(
            cycle_id="cycle", started_at=NOW, completed_at=NOW + timedelta(seconds=1),
            delay_seconds=1.0, horizons_seconds=[1.0], initial_scan_id="initial",
            verification_scan_id="verification-2",
            verification_scan_ids=["verification-0", "verification-1", "verification-2"],
            observations=observations,
        )
    )
    cfg = settings(
        empirical_latency_min_samples=3,
        empirical_latency_min_scan_samples=3,
        empirical_latency_min_effective_samples=3,
        empirical_latency_quantile=0.95,
    )

    model = build_empirical_latency_model(store, cfg)

    assert model.usable_for_qualification is True
    assert model.data_latency_source == "l2_request_roundtrip"
    assert model.data_latency_sample_count == 3
    assert model.effective_sample_size == 3
    assert model.reference_horizon_seconds == 1.0
    assert model.pair_fill_probability == 2 / 3
    assert model.reserve_fill_probability == 2 / 3
    assert model.capture_probability == 2 / 3
    assert model.pair_fill_ci_lower is not None
    assert model.pair_fill_ci_upper is not None
    assert model.partial_fill_probability == 1 / 3
    assert model.hedge_recovery_loss_p95_bps is not None
    assert model.adverse_selection_p95_bps is not None
    assert model.empirical_latency_risk_bps == model.adverse_selection_p95_bps
    assert model.queue_position_supported is False
    assert model.maker_fill_probability is None
    assert model.execution_latency_empirical is False


def test_empirical_model_replaces_latency_risk_but_recovery_keeps_fixed_floor():
    cfg = settings(
        expected_hedge_latency_ms=1000.0,
        latency_risk_bps_per_second=2.0,
        hedge_recovery_buffer_bps=2.0,
    )
    aged_books = books(observed_at=NOW - timedelta(seconds=3))
    model = EmpiricalLatencyModel(
        cohort_sample_count=100,
        effective_sample_size=80,
        confidence_gate_passed=True,
        effective_decision_to_hedge_latency_ms=500.0,
        collector_latency_reference_ms=250.0,
        pair_fill_probability=0.9,
        pair_fill_ci_lower=0.8,
        capture_probability=0.8,
        capture_ci_lower=0.7,
        hedge_recovery_loss_p95_bps=5.0,
        adverse_selection_p95_bps=7.0,
        empirical_latency_risk_bps=7.0,
        usable_for_qualification=True,
    )

    tier = qualify_opportunity(opportunity(), aged_books, cfg, now=NOW, latency_model=model).tiers[0]

    assert tier.latency_model_source == "empirical_shadow"
    assert tier.latency_risk_bps == 13.0  # 3s book age * 2 bps/s + empirical 7 bps
    assert tier.latency_reference_ms == 500.0
    assert tier.collector_latency_reference_ms == 250.0
    assert tier.latency_effective_sample_size == 80
    assert tier.empirical_pair_fill_probability == 0.9
    assert tier.empirical_pair_fill_ci_lower == 0.8
    assert tier.empirical_capture_probability == 0.8
    assert tier.hedge_recovery_buffer_bps == 5.0
    assert tier.hedge_recovery_buffer_source == "max_fixed_empirical"
    assert tier.execution_latency_empirical is False


def test_insufficient_empirical_evidence_keeps_fixed_latency_model():
    cfg = settings(
        expected_hedge_latency_ms=1000.0,
        latency_risk_bps_per_second=2.0,
    )
    unusable = EmpiricalLatencyModel(
        cohort_sample_count=2,
        empirical_latency_risk_bps=0.1,
        usable_for_qualification=False,
        reason="insufficient samples",
    )

    tier = qualify_opportunity(opportunity(), books(), cfg, now=NOW, latency_model=unusable).tiers[0]

    assert tier.latency_model_source == "fixed"
    assert tier.latency_risk_bps == 2.0
    assert tier.latency_reference_ms == 1000.0
    assert tier.latency_sample_count == 0
    assert tier.hedge_recovery_buffer_source == "fixed"
