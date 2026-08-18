from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import ProviderStatus, ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    ShadowCycle,
    ShadowFailureCause,
    ShadowObservation,
    ShadowOutcome,
    Side,
    Strategy,
)
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.shadow import summarize_shadow_cycles

NOW = datetime(2026, 8, 18, 23, 0, tzinfo=timezone.utc)


def cfg() -> Settings:
    return Settings(
        min_net_annualized_return=0.01,
        capital_tiers_usd=(1000.0, 5000.0),
        max_order_book_age_seconds=30.0,
        max_order_book_skew_seconds=2.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_liquidity_reserve_ratio=1.0,
        hedge_recovery_buffer_bps=0.0,
        shadow_horizons_seconds=(1.0, 5.0, 15.0, 30.0, 60.0),
        shadow_max_candidates=0,
    )


def opportunity(observed_at: datetime, gross_edge: float = 5.0) -> Opportunity:
    return Opportunity(
        id=f"opp-{observed_at.timestamp()}",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=gross_edge,
        modeled_cost_bps=0.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=gross_edge,
        net_annualized_return=1.0,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=2),
    )


def books(observed_at: datetime, *, spot_ask: float = 100.0, perp_bid: float = 101.0, size: float = 1000.0):
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=spot_ask - 0.1, size=size)],
            asks=[OrderBookLevel(price=spot_ask, size=size)],
            observed_at=observed_at, source="fixture",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=[OrderBookLevel(price=perp_bid, size=size)],
            asks=[OrderBookLevel(price=perp_bid + 0.1, size=size)],
            observed_at=observed_at, source="fixture",
        ),
    ]


def snapshot(scan_id: str, op: Opportunity | None, settings: Settings, *, obs_books=None, provider_ok=True):
    snap_time = op.observed_at if op else NOW
    observed_books = obs_books if obs_books is not None else (books(snap_time) if op else [])
    ops = [op] if op else []
    execs = [qualify_opportunity(op, observed_books, settings, now=snap_time)] if op else []
    return ScanSnapshot(
        scan_id=scan_id,
        started_at=snap_time,
        completed_at=snap_time,
        providers=[ProviderStatus(provider="fixture", ok=provider_ok, item_count=1 if provider_ok else 0, observed_at=snap_time)],
        funding_quotes=[], market_quotes=[], opportunities=ops, order_books=observed_books,
        executability=execs, analysis_config={},
    )


class FakeService(OpportunityService):
    def __init__(self, snapshots, *, settings):
        super().__init__(settings=settings)
        self._snapshots = iter(snapshots)

    async def collect_live_executability(self):
        return next(self._snapshots)


@pytest.mark.asyncio
async def test_multi_horizon_tracks_every_qualified_capital_tier(monkeypatch):
    settings = cfg()
    initial = opportunity(NOW)
    snapshots = [snapshot("s0", initial, settings)]
    for seconds in settings.shadow_horizons_seconds:
        current_time = NOW + timedelta(seconds=seconds)
        snapshots.append(snapshot(f"s{int(seconds)}", opportunity(current_time), settings))

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("inefficiency_engine.service.asyncio.sleep", no_sleep)
    cycle = await FakeService(snapshots, settings=settings).run_shadow_cycle()

    assert cycle.horizons_seconds == [1.0, 5.0, 15.0, 30.0, 60.0]
    assert cycle.verification_scan_ids == ["s1", "s5", "s15", "s30", "s60"]
    assert len(cycle.observations) == 10
    assert {row.notional_usd_per_leg for row in cycle.observations} == {1000.0, 5000.0}
    assert all(row.survived for row in cycle.observations)
    assert all(len(row.leg_attribution) == 2 for row in cycle.observations)


@pytest.mark.asyncio
async def test_provider_failure_is_attributed_fail_closed(monkeypatch):
    settings = cfg()
    initial = opportunity(NOW)
    verification_time = NOW + timedelta(seconds=1)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("inefficiency_engine.service.asyncio.sleep", no_sleep)
    service = FakeService(
        [
            snapshot("s0", initial, settings),
            snapshot("s1", opportunity(verification_time), settings, provider_ok=False),
        ],
        settings=settings,
    )
    cycle = await service.run_shadow_cycle(delay_seconds=1)
    assert cycle.observations
    assert all(not row.survived for row in cycle.observations)
    assert all(row.failure_cause == ShadowFailureCause.STALE_DATA_PROVIDER_FAILURE for row in cycle.observations)


def test_summary_exposes_capture_lifetime_decay_and_segments():
    def obs(horizon: float, survived: bool, decay: float | None) -> ShadowObservation:
        return ShadowObservation(
            shadow_id=f"x-{horizon}", initial_scan_id="s0", verification_scan_id=f"s{horizon}",
            opportunity_signature="sig", opportunity_id="op", strategy=Strategy.SPOT_PERP_BASIS,
            asset="ETH", notional_usd_per_leg=1000.0, started_at=NOW,
            verified_at=NOW + timedelta(seconds=horizon), delay_seconds=horizon,
            initial_net_annualized_return=0.18, initial_capacity_notional_usd=50000.0,
            survived=survived, verification_net_annualized_return=(0.18 - decay) if decay is not None else None,
            outcome=ShadowOutcome.SURVIVED if survived else ShadowOutcome.EXECUTABILITY_FAILED,
            venue_pair="Coinbase|HlPerp", time_of_day_bucket="23:00Z",
            initial_expected_return_bucket="10-20%", edge_decay_annualized=decay,
            verification_capacity_notional_usd=40000.0 if survived else 0.0,
            failure_cause=None if survived else ShadowFailureCause.SLIPPAGE_EXPANSION,
        )

    cycle = ShadowCycle(
        cycle_id="c", started_at=NOW, completed_at=NOW + timedelta(seconds=60),
        delay_seconds=60, horizons_seconds=[1, 5, 15, 30, 60], initial_scan_id="s0",
        verification_scan_id="s60",
        observations=[obs(1, True, 0.01), obs(5, True, 0.02), obs(15, False, 0.05), obs(30, False, 0.08), obs(60, False, 0.10)],
    )
    summary = summarize_shadow_cycles([cycle])

    assert summary["estimated_capture_probability"] == 1.0
    assert summary["probability_surviving_5_seconds"] == 1.0
    assert summary["probability_surviving_15_seconds"] == 0.0
    assert summary["median_opportunity_lifetime_seconds"] == 5
    assert summary["false_positive_rate"] == 0.0
    assert summary["failure_causes"]["slippage_expansion"] == 3
    assert summary["max_realistically_deployable_capital_usd"] == 40000.0
    assert "spot_perp_basis" in summary["survival_by"]["strategy"]
