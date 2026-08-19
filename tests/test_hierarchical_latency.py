from datetime import datetime, timedelta, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.latency import EmpiricalLatencyResolver
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

NOW = datetime(2026, 8, 18, 23, 45, tzinfo=timezone.utc)
VENUE_PAIR = "Coinbase|HlPerp"


def opportunity(asset: str = "ETH") -> Opportunity:
    return Opportunity(
        id=f"hierarchy-{asset}",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset=asset,
        legs=[
            OpportunityLeg(venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
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


def books(asset: str = "ETH") -> list[OrderBookSnapshot]:
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT, symbol=f"{asset}-USD",
            bids=[OrderBookLevel(price=99.9, size=5000)], asks=[OrderBookLevel(price=100.0, size=5000)],
            observed_at=NOW, source="fixture",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset=asset, market_kind=MarketKind.PERPETUAL, symbol=asset,
            bids=[OrderBookLevel(price=101.0, size=5000)], asks=[OrderBookLevel(price=101.1, size=5000)],
            observed_at=NOW, source="fixture",
        ),
    ]


def cfg() -> Settings:
    return Settings(
        min_net_annualized_return=0.0,
        capital_tiers_usd=(1000.0, 5000.0),
        max_order_book_age_seconds=30.0,
        hedge_liquidity_reserve_ratio=1.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        hedge_recovery_buffer_bps=0.0,
        expected_order_ack_latency_ms=0.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=1.0,
        empirical_latency_min_samples=3,
        empirical_latency_min_scan_samples=3,
        empirical_latency_min_effective_samples=3,
        empirical_probability_max_ci_width=1.0,
    )


def observation(index: int, *, asset: str, notional: float, adverse: float) -> ShadowObservation:
    return ShadowObservation(
        shadow_id=f"shadow-{asset}-{notional}-{index}",
        initial_scan_id=f"initial-{index}",
        verification_scan_id=f"verify-{asset}-{notional}-{index}",
        opportunity_signature=f"sig-{asset}",
        opportunity_id=f"op-{asset}",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset=asset,
        notional_usd_per_leg=notional,
        target_base_quantity=10.0,
        started_at=NOW,
        verified_at=NOW + timedelta(seconds=1),
        delay_seconds=1.0,
        initial_scan_latency_ms=300.0,
        verification_scan_latency_ms=400.0 + index,
        initial_data_path_latency_ms=250.0,
        verification_data_path_latency_ms=300.0 + index,
        initial_net_annualized_return=0.20,
        initial_capacity_notional_usd=100000.0,
        survived=True,
        pair_fillable=True,
        pair_fillable_with_reserve=True,
        hedge_recovery_required=False,
        pair_fill_fraction=1.0,
        max_leg_fill_fraction=1.0,
        unhedged_fraction=0.0,
        partial_fill_state=False,
        hedge_recovery_loss_proxy_bps=0.0,
        verification_net_annualized_return=0.18,
        outcome=ShadowOutcome.SURVIVED,
        venue_pair=VENUE_PAIR,
        leg_attribution=[
            ShadowLegAttribution(
                venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT, side=Side.LONG,
                adverse_selection_bps=adverse,
            ),
            ShadowLegAttribution(
                venue="HlPerp", asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT,
                adverse_selection_bps=adverse / 2.0,
            ),
        ],
    )


def persist(store: EvidenceStore, rows: list[ShadowObservation]) -> None:
    store.record_shadow_cycle(
        ShadowCycle(
            cycle_id="hierarchy-cycle",
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
            delay_seconds=1.0,
            horizons_seconds=[1.0],
            initial_scan_id="initial",
            verification_scan_id=rows[-1].verification_scan_id,
            verification_scan_ids=[row.verification_scan_id for row in rows],
            observations=rows,
        )
    )


def test_resolver_prefers_exact_strategy_venue_asset_capital_cohort(tmp_path):
    store = EvidenceStore(tmp_path / "hierarchy.sqlite3")
    rows = [observation(i, asset="ETH", notional=1000.0, adverse=1.0 + i) for i in range(3)]
    rows += [observation(i + 10, asset="BTC", notional=1000.0, adverse=20.0) for i in range(3)]
    persist(store, rows)

    model = EmpiricalLatencyResolver(store, cfg()).resolve(opportunity("ETH"), 1000.0)

    assert model.usable_for_qualification is True
    assert model.model_scope == "strategy+venue_pair+asset+capital"
    assert model.scope_asset == "ETH"
    assert model.scope_notional_usd_per_leg == 1000.0
    assert model.cohort_sample_count == 3
    assert model.effective_sample_size == 3
    assert model.adverse_selection_p95_bps is not None
    assert model.adverse_selection_p95_bps < 10.0
    assert model.scope_fallbacks == []


def test_resolver_falls_back_when_exact_capital_cohort_is_sparse(tmp_path):
    store = EvidenceStore(tmp_path / "fallback.sqlite3")
    rows = [
        observation(0, asset="ETH", notional=1000.0, adverse=1.0),
        observation(1, asset="ETH", notional=5000.0, adverse=2.0),
        observation(2, asset="ETH", notional=5000.0, adverse=3.0),
    ]
    persist(store, rows)

    model = EmpiricalLatencyResolver(store, cfg()).resolve(opportunity("ETH"), 25000.0)

    assert model.usable_for_qualification is True
    assert model.model_scope == "strategy+venue_pair+asset"
    assert model.scope_notional_usd_per_leg is None
    assert model.cohort_sample_count == 3
    assert model.scope_fallbacks[0].startswith("strategy+venue_pair+asset+capital:raw=0")


def test_qualification_resolves_empirical_model_per_capital_tier():
    settings = cfg()

    def resolver(_opportunity: Opportunity, notional: float) -> EmpiricalLatencyModel:
        if notional == 1000.0:
            return EmpiricalLatencyModel(
                model_scope="strategy+venue_pair+asset+capital",
                scope_notional_usd_per_leg=1000.0,
                cohort_sample_count=100,
                effective_sample_size=80,
                reference_latency_ms=400.0,
                pair_fill_probability=0.95,
                reserve_fill_probability=0.90,
                capture_probability=0.85,
                hedge_recovery_probability=0.05,
                hedge_recovery_loss_p95_bps=1.0,
                empirical_latency_risk_bps=2.0,
                usable_for_qualification=True,
            )
        return EmpiricalLatencyModel(
            model_scope="strategy+venue_pair+asset",
            scope_fallbacks=["strategy+venue_pair+asset+capital:raw=0"],
            cohort_sample_count=80,
            effective_sample_size=60,
            reference_latency_ms=450.0,
            pair_fill_probability=0.80,
            reserve_fill_probability=0.75,
            capture_probability=0.70,
            hedge_recovery_probability=0.10,
            hedge_recovery_loss_p95_bps=3.0,
            empirical_latency_risk_bps=8.0,
            usable_for_qualification=True,
        )

    result = qualify_opportunity(
        opportunity(), books(), settings,
        notionals_usd=(1000.0, 5000.0), now=NOW,
        latency_model_resolver=resolver,
    )
    first, second = result.tiers

    assert first.latency_model_scope == "strategy+venue_pair+asset+capital"
    assert first.latency_risk_bps == 2.0
    assert first.empirical_capture_probability == 0.85
    assert first.latency_effective_sample_size == 80
    assert second.latency_model_scope == "strategy+venue_pair+asset"
    assert second.latency_risk_bps == 8.0
    assert second.latency_scope_fallbacks == ["strategy+venue_pair+asset+capital:raw=0"]
    assert second.hedge_recovery_buffer_bps == 3.0
