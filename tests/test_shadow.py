from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus, ScanSnapshot
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)
from inefficiency_engine.service import OpportunityService

NOW = datetime(2026, 8, 18, 22, 0, tzinfo=timezone.utc)


def settings() -> Settings:
    return Settings(
        min_net_annualized_return=0.01,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        max_order_book_skew_seconds=2.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_liquidity_reserve_ratio=1.0,
        hedge_recovery_buffer_bps=0.0,
        shadow_notional_usd=1000.0,
        shadow_delay_seconds=0.0,
    )


def opportunity(observed_at=NOW) -> Opportunity:
    return Opportunity(
        id=f"opp-{observed_at.timestamp()}",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="ETH",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=5.0,
        modeled_cost_bps=20.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=4.0,
        net_annualized_return=1.0,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=2),
    )


def books(observed_at=NOW):
    return [
        OrderBookSnapshot(
            venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
            bids=[OrderBookLevel(price=99.9, size=1000)], asks=[OrderBookLevel(price=100.0, size=1000)],
            observed_at=observed_at, source="fixture",
        ),
        OrderBookSnapshot(
            venue="HlPerp", asset="ETH", market_kind=MarketKind.PERPETUAL, symbol="ETH",
            bids=[OrderBookLevel(price=101.0, size=1000)], asks=[OrderBookLevel(price=101.1, size=1000)],
            observed_at=observed_at, source="fixture",
        ),
    ]


def snapshot(scan_id: str, op: Opportunity | None, cfg: Settings) -> ScanSnapshot:
    obs_books = books(op.observed_at if op else NOW)
    ops = [op] if op else []
    execs = [qualify_opportunity(op, obs_books, cfg, now=op.observed_at)] if op else []
    snap_time = op.observed_at if op else NOW
    return ScanSnapshot(
        scan_id=scan_id,
        started_at=snap_time,
        completed_at=snap_time,
        providers=[ProviderStatus(provider="fixture", ok=True, item_count=1, observed_at=NOW)],
        funding_quotes=[],
        market_quotes=[],
        opportunities=ops,
        order_books=obs_books if op else [],
        executability=execs,
        analysis_config={},
    )


class FakeShadowService(OpportunityService):
    def __init__(self, snapshots, *, cfg, store=None):
        super().__init__(settings=cfg, evidence_store=store)
        self._snapshots = iter(snapshots)

    async def collect_live_executability(self):
        return next(self._snapshots)


@pytest.mark.asyncio
async def test_shadow_cycle_records_survival_when_signal_and_execution_persist(tmp_path):
    cfg = settings()
    first = opportunity(NOW)
    second = opportunity(NOW + timedelta(seconds=5))
    store = EvidenceStore(tmp_path / "shadow.sqlite3")
    service = FakeShadowService([snapshot("s1", first, cfg), snapshot("s2", second, cfg)], cfg=cfg, store=store)

    cycle = await service.run_shadow_cycle(delay_seconds=0)
    assert len(cycle.observations) == 1
    assert cycle.observations[0].survived is True
    assert store.counts().shadow_cycles == 1
    assert store.shadow_summary()["survival_rate"] == 1.0


@pytest.mark.asyncio
async def test_shadow_cycle_marks_disappeared_signal(tmp_path):
    cfg = settings()
    first = opportunity(NOW)
    store = EvidenceStore(tmp_path / "shadow.sqlite3")
    service = FakeShadowService([snapshot("s1", first, cfg), snapshot("s2", None, cfg)], cfg=cfg, store=store)

    cycle = await service.run_shadow_cycle(delay_seconds=0)
    assert len(cycle.observations) == 1
    assert cycle.observations[0].survived is False
    assert cycle.observations[0].outcome.value == "signal_disappeared"
