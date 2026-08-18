from datetime import datetime, timedelta, timezone

import pytest

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

NOW = datetime(2026, 8, 18, 23, 55, tzinfo=timezone.utc)
VENUE_PAIR = "Coinbase|HlPerp"


def cfg() -> Settings:
    return Settings(
        empirical_latency_min_samples=3,
        empirical_latency_min_scan_samples=3,
        empirical_latency_quantile=0.95,
    )


def opportunity() -> Opportunity:
    return Opportunity(
        id="interval-op",
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


def row(index: int, *, horizon: float, fillable: bool, adverse: float, notional: float = 1000.0) -> ShadowObservation:
    return ShadowObservation(
        shadow_id=f"shadow-{horizon}-{index}-{notional}",
        initial_scan_id=f"initial-{index}",
        verification_scan_id=f"verify-{horizon}-{index}-{notional}",
        opportunity_signature="sig",
        opportunity_id="op",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        notional_usd_per_leg=notional,
        target_base_quantity=10.0,
        started_at=NOW,
        verified_at=NOW + timedelta(seconds=horizon),
        delay_seconds=horizon,
        initial_scan_latency_ms=9000.0,
        verification_scan_latency_ms=10000.0,
        initial_net_annualized_return=0.20,
        initial_capacity_notional_usd=100000.0,
        survived=fillable,
        pair_fillable=fillable,
        pair_fillable_with_reserve=fillable,
        hedge_recovery_required=not fillable,
        verification_net_annualized_return=0.18 if fillable else 0.0,
        outcome=ShadowOutcome.SURVIVED if fillable else ShadowOutcome.EXECUTABILITY_FAILED,
        venue_pair=VENUE_PAIR,
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


def persist(store: EvidenceStore, rows: list[ShadowObservation]) -> None:
    store.record_shadow_cycle(
        ShadowCycle(
            cycle_id="interval-cycle",
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=15),
            delay_seconds=15.0,
            horizons_seconds=[5.0, 15.0],
            initial_scan_id="initial",
            verification_scan_id=rows[-1].verification_scan_id,
            verification_scan_ids=[item.verification_scan_id for item in rows],
            observations=rows,
        )
    )


def test_latency_between_horizons_uses_conservative_interpolation(tmp_path):
    store = EvidenceStore(tmp_path / "interval.sqlite3")
    rows = [row(i, horizon=5.0, fillable=True, adverse=2.0) for i in range(3)]
    rows += [
        row(10, horizon=15.0, fillable=True, adverse=10.0),
        row(11, horizon=15.0, fillable=False, adverse=10.0),
        row(12, horizon=15.0, fillable=False, adverse=10.0),
    ]
    persist(store, rows)

    model = EmpiricalLatencyResolver(store, cfg()).resolve(opportunity(), 1000.0)

    assert model.usable_for_qualification is True
    assert model.reference_latency_ms == 10000.0
    assert model.reference_lower_horizon_seconds == 5.0
    assert model.reference_upper_horizon_seconds == 15.0
    assert model.reference_horizon_seconds == 15.0
    assert model.interpolation_mode == "linear_interval"
    assert model.interpolation_weight == pytest.approx(0.5)
    assert model.pair_fill_probability == pytest.approx(2 / 3)
    assert model.capture_probability == pytest.approx(2 / 3)
    assert model.adverse_selection_p95_bps == pytest.approx(9.0)
    assert model.empirical_latency_risk_bps == pytest.approx(9.0)


def test_probability_cannot_improve_and_risk_cannot_fall_with_time(tmp_path):
    store = EvidenceStore(tmp_path / "monotonic.sqlite3")
    rows = [
        row(0, horizon=5.0, fillable=True, adverse=8.0),
        row(1, horizon=5.0, fillable=False, adverse=8.0),
        row(2, horizon=5.0, fillable=False, adverse=8.0),
    ]
    rows += [row(i + 10, horizon=15.0, fillable=True, adverse=2.0) for i in range(3)]
    persist(store, rows)

    model = EmpiricalLatencyResolver(store, cfg()).resolve(opportunity(), 1000.0)

    assert model.pair_fill_probability == pytest.approx(1 / 3)
    assert model.capture_probability == pytest.approx(1 / 3)
    assert model.adverse_selection_p95_bps == pytest.approx(12.0)


def test_scope_requires_enough_samples_at_both_interval_endpoints(tmp_path):
    store = EvidenceStore(tmp_path / "scope-interval.sqlite3")
    rows = [row(i, horizon=5.0, fillable=True, adverse=2.0, notional=1000.0) for i in range(3)]
    rows += [row(10, horizon=15.0, fillable=True, adverse=4.0, notional=1000.0)]
    rows += [row(i + 20, horizon=15.0, fillable=True, adverse=4.0, notional=5000.0) for i in range(2)]
    rows += [row(i + 30, horizon=5.0, fillable=True, adverse=3.0, notional=5000.0) for i in range(3)]
    persist(store, rows)

    model = EmpiricalLatencyResolver(store, cfg()).resolve(opportunity(), 1000.0)

    assert model.usable_for_qualification is True
    assert model.model_scope == "strategy+venue_pair+asset"
    assert model.scope_fallbacks == ["strategy+venue_pair+asset+capital:1"]
    assert model.lower_horizon_sample_count == 6
    assert model.upper_horizon_sample_count == 3
