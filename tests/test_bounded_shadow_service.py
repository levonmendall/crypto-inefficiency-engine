from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.bounded_shadow_service import (
    MemoryBoundedShadowService,
    select_rotating_shadow_opportunities,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import (
    MarketKind,
    Opportunity,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)
from inefficiency_engine.service import OpportunityService, _opportunity_signature


def _opportunity(asset: str, score: float, observed_at: datetime) -> Opportunity:
    return Opportunity(
        id=f"opp-{asset}-{score}",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset=asset,
        legs=[
            OpportunityLeg(venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset=asset, market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=score,
        modeled_cost_bps=0.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=score,
        net_annualized_return=score,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=2),
    )


def test_rotating_scope_keeps_priority_and_eventually_covers_tail():
    now = datetime.now(timezone.utc)
    opportunities = [_opportunity(f"A{index}", float(index), now) for index in range(6)]

    first, cursor = select_rotating_shadow_opportunities(opportunities, limit=4, cursor=0)
    second, _ = select_rotating_shadow_opportunities(opportunities, limit=4, cursor=cursor)

    first_assets = {row.asset for row in first}
    second_assets = {row.asset for row in second}
    assert len(first) == 4
    assert len(second) == 4
    assert {"A5", "A4"}.issubset(first_assets)
    assert {"A5", "A4"}.issubset(second_assets)
    assert first_assets | second_assets == {f"A{index}" for index in range(6)}


class _BookRegistry:
    def __init__(self) -> None:
        self.scopes: list[list[str]] = []

    async def collect_books_for_opportunities(self, opportunities):
        self.scopes.append([_opportunity_signature(opportunity) for opportunity in opportunities])
        observed_at = datetime.now(timezone.utc)
        books: list[OrderBookSnapshot] = []
        for opportunity in opportunities:
            books.extend(
                [
                    OrderBookSnapshot(
                        venue="Coinbase",
                        asset=opportunity.asset,
                        market_kind=MarketKind.SPOT,
                        symbol=f"{opportunity.asset}-USD",
                        bids=[OrderBookLevel(price=99.9, size=10000.0)],
                        asks=[OrderBookLevel(price=100.0, size=10000.0)],
                        observed_at=observed_at,
                        source="fixture",
                    ),
                    OrderBookSnapshot(
                        venue="HlPerp",
                        asset=opportunity.asset,
                        market_kind=MarketKind.PERPETUAL,
                        symbol=opportunity.asset,
                        bids=[OrderBookLevel(price=101.0, size=10000.0)],
                        asks=[OrderBookLevel(price=101.1, size=10000.0)],
                        observed_at=observed_at,
                        source="fixture",
                    ),
                ]
            )
        return books, []

    def provider_venue(self, _provider):
        return None


@pytest.mark.asyncio
async def test_bounded_shadow_persists_full_discovery_but_retains_only_l2_scope(tmp_path):
    now = datetime.now(timezone.utc)
    opportunities = [_opportunity(f"A{index}", float(index + 1), now) for index in range(6)]
    settings = Settings(
        min_net_annualized_return=0.01,
        capital_tiers_usd=(1000.0,),
        max_order_book_age_seconds=30.0,
        max_order_book_skew_seconds=5.0,
        coinbase_spot_taker_fee_bps=0.0,
        hyperliquid_perp_taker_fee_bps=0.0,
        collateral_opportunity_cost_annual=0.0,
        expected_hedge_latency_ms=0.0,
        latency_risk_bps_per_second=0.0,
        hedge_liquidity_reserve_ratio=1.0,
        hedge_recovery_buffer_bps=0.0,
    )
    store = EvidenceStore(tmp_path / "bounded-shadow.sqlite3")
    registry = _BookRegistry()
    core = OpportunityService(settings=settings, evidence_store=store, adapter_registry=registry)  # type: ignore[arg-type]
    bounded = MemoryBoundedShadowService(core, store, max_opportunities=4)

    async def fake_inputs():
        return (
            now,
            [],
            [],
            opportunities,
            [ProviderStatus(provider="fixture", ok=True, item_count=len(opportunities), observed_at=now)],
        )

    bounded._collect_live_inputs = fake_inputs  # type: ignore[method-assign]
    snapshot = await bounded.collect_live_executability()

    assert len(registry.scopes) == 1
    assert len(registry.scopes[0]) == 4
    assert len(snapshot.opportunities) == 4
    assert snapshot.funding_quotes == []
    assert snapshot.market_quotes == []

    persisted = store.load_scan(snapshot.scan_id)
    assert len(persisted.opportunities) == 6
    assert len(persisted.executability) == 4
