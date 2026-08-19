from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.models import (
    CapitalTierQualification,
    MarketKind,
    Opportunity,
    OpportunityExecutability,
    OpportunityLeg,
    Side,
    Strategy,
)
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService, _core_candidates


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def core_opportunity() -> Opportunity:
    return Opportunity(
        id="core-btc",
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
        net_annualized_return=0.50,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        evidence={"canonical_instrument_ids": ["btc-coinbase-spot", "btc-hl-perp"]},
    )


def core_execution() -> OpportunityExecutability:
    tier = CapitalTierQualification(
        opportunity_id="core-btc",
        notional_usd_per_leg=5000.0,
        executable=True,
        passes_return_hurdle=True,
        gross_edge_bps_per_hour=2.0,
        static_modeled_cost_bps=0.0,
        total_modeled_cost_bps=1.0,
        net_edge_bps_per_hour=1.0,
        net_annualized_return=0.50,
        capital_required_usd=10000.0,
    )
    return OpportunityExecutability(
        opportunity_id="core-btc",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="BTC",
        observed_at=NOW,
        tiers=[tier],
        estimated_capacity_notional_usd=5000.0,
        max_qualified_notional_usd=5000.0,
    )


def test_core_candidate_converts_annualized_return_to_one_holding_period():
    rows = _core_candidates([core_opportunity()], [core_execution()])
    assert len(rows) == 1
    row = rows[0]
    expected_return = 0.50 * 24.0 / (24.0 * 365.0)
    assert row.expected_return_on_reserved_capital == pytest.approx(expected_return)
    assert row.expected_profit_usd_per_deployment == pytest.approx(10000.0 * expected_return)
    assert row.modeled_holding_hours == 24.0
    assert row.source_return_metric == "net_annualized_return"
    assert row.exposure_kind == "market_neutral"
    assert row.executable_eligible is False


class FakeCore:
    def __init__(self):
        self.settings = Settings(
            allocator_max_venue_fraction=0.75,
            allocator_max_asset_fraction=0.75,
            allocator_max_allocations=5,
        )

    async def collect_live_executability(self):
        return SimpleNamespace(
            opportunities=[core_opportunity()],
            executability=[core_execution()],
            market_quotes=[],
            completed_at=NOW,
        )


class FailingCore(FakeCore):
    async def collect_live_executability(self):
        raise RuntimeError("core market provider unavailable")


class FakeCexDexPromotion:
    async def live_qualification(self, *, paper_inventory_usd_per_side: float):
        assert paper_inventory_usd_per_side > 0
        qualified = SimpleNamespace(
            paper_allocation_eligible=True,
            composite_key="eth-edge",
            evidence_id="eth-evidence",
            asset="ETH",
            route_direction="buy_asset",
            target_notional_usd=5000.0,
            paper_capital_required_usd=10000.0,
            conservative_capture_edge_bps=40.0,
            cex_venue="Kraken",
            cex_symbol="ETH-USD",
            dex_venue="DEX:ethereum",
        )
        blocked = SimpleNamespace(paper_allocation_eligible=False)
        return SimpleNamespace(qualifications=[qualified, blocked])


class FailingCexDexPromotion:
    async def live_qualification(self, *, paper_inventory_usd_per_side: float):
        assert paper_inventory_usd_per_side > 0
        raise ConnectionError("DEX qualification provider unavailable")


class FakeAlphaFactory:
    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        assert snapshot.completed_at == NOW
        assert total_capital_usd > 0
        return [SimpleNamespace(
            candidate_id="alpha:sol-trend",
            strategy_id="time_series_momentum_v1",
            asset="SOL",
            direction="long",
            venue="OKX",
            symbol="SOL-USDT",
            market_kind=MarketKind.SPOT,
            observed_at=NOW,
            entry_reference_price=150.0,
            estimated_cost_return=0.001,
            capital_required_usd=5000.0,
            notional_usd=5000.0,
            expected_profit_usd=25.0,
            expected_net_return=0.005,
            horizon_hours=6.0,
            conflict_keys=["alpha-instrument:OKX:SOL-USDT"],
        )]


class FailingAlphaFactory:
    async def promoted_candidates(self, snapshot, *, total_capital_usd: float):
        raise LookupError("alpha evidence unavailable")


@pytest.mark.asyncio
async def test_unified_allocator_compares_only_qualified_current_deployments_and_preserves_cash():
    service = UnifiedPaperAllocatorService(
        FakeCore(),  # type: ignore[arg-type]
        FakeCexDexPromotion(),  # type: ignore[arg-type]
    )

    candidates = await service.candidates(total_capital_usd=30000.0)
    assert [row.family for row in candidates] == ["cex_dex", "core_cex"]
    assert candidates[0].expected_return_on_reserved_capital == pytest.approx(0.002)
    assert candidates[1].expected_return_on_reserved_capital == pytest.approx(0.50 / 365.0)

    plan = await service.allocate(
        total_capital_usd=30000.0,
        max_venue_fraction=0.75,
        max_asset_fraction=0.75,
        max_allocations=5,
    )
    assert len(plan.allocations) == 2
    assert plan.allocations[0].family == "cex_dex"
    assert plan.allocations[1].family == "core_cex"
    assert plan.allocated_capital_usd == 20000.0
    assert plan.unused_cash_usd == 10000.0
    assert plan.expected_profit_usd_current_deployments > 0
    assert plan.portfolio_risk_budget is not None
    assert plan.portfolio_risk_budget.market_neutral_capital_usd == 20000.0
    assert plan.portfolio_risk_budget.directional_capital_usd == 0.0
    assert plan.authorizes_execution is False
    assert plan.live_execution_eligible is False
    assert plan.degraded is False
    assert plan.family_failures == {}
    assert set(plan.available_families) == {"core_cex", "cex_dex"}
    assert all(item.authorizes_execution is False for item in plan.allocations)


@pytest.mark.asyncio
async def test_unified_allocator_accepts_only_promoted_alpha_and_tracks_directional_budget():
    service = UnifiedPaperAllocatorService(
        FakeCore(),  # type: ignore[arg-type]
        FakeCexDexPromotion(),  # type: ignore[arg-type]
        FakeAlphaFactory(),  # type: ignore[arg-type]
    )
    candidates = await service.candidates(total_capital_usd=30000.0)
    assert [row.family for row in candidates] == ["alpha", "cex_dex", "core_cex"]
    assert candidates[0].strategy == "time_series_momentum_v1"
    assert candidates[0].exposure_kind == "directional_long"
    assert candidates[0].instrument_market_kind == "spot"
    assert candidates[0].entry_reference_price == 150.0
    assert candidates[0].modeled_roundtrip_cost_return == 0.001
    assert candidates[0].expected_return_on_reserved_capital == pytest.approx(0.005)
    plan = await service.allocate(total_capital_usd=30000.0)
    alpha = next(row for row in plan.allocations if row.family == "alpha")
    assert alpha.source_observed_at == NOW
    assert alpha.instrument_symbol == "SOL-USDT"
    assert alpha.instrument_market_kind == "spot"
    assert any(row.family == "alpha" for row in plan.allocations)
    assert plan.portfolio_risk_budget is not None
    assert plan.portfolio_risk_budget.directional_long_capital_usd == 5000.0
    assert plan.portfolio_risk_budget.directional_net_capital_usd == 5000.0
    assert plan.degraded is False
    assert all(row.authorizes_execution is False for row in plan.allocations)


@pytest.mark.asyncio
async def test_cex_dex_failure_does_not_erase_core_or_alpha_candidates():
    service = UnifiedPaperAllocatorService(
        FakeCore(),  # type: ignore[arg-type]
        FailingCexDexPromotion(),  # type: ignore[arg-type]
        FakeAlphaFactory(),  # type: ignore[arg-type]
    )

    batch = await service.candidate_batch(total_capital_usd=30000.0)
    assert [row.family for row in batch.candidates] == ["alpha", "core_cex"]
    assert set(batch.available_families) == {"core_cex", "alpha"}
    assert "cex_dex" in batch.family_failures
    assert batch.degraded is True

    plan = await service.allocate(total_capital_usd=30000.0)
    assert {row.family for row in plan.allocations} == {"alpha", "core_cex"}
    assert plan.degraded is True
    assert "cex_dex" in plan.family_failures
    assert plan.unused_cash_usd == 15000.0


@pytest.mark.asyncio
async def test_core_failure_still_allows_cex_dex_and_explicitly_blocks_snapshot_dependent_alpha():
    service = UnifiedPaperAllocatorService(
        FailingCore(),  # type: ignore[arg-type]
        FakeCexDexPromotion(),  # type: ignore[arg-type]
        FakeAlphaFactory(),  # type: ignore[arg-type]
    )

    batch = await service.candidate_batch(total_capital_usd=30000.0)
    assert [row.family for row in batch.candidates] == ["cex_dex"]
    assert batch.available_families == ["cex_dex"]
    assert "core_cex" in batch.family_failures
    assert batch.family_failures["alpha"].startswith("market_snapshot_unavailable")
    assert batch.degraded is True

    plan = await service.allocate(total_capital_usd=30000.0)
    assert [row.family for row in plan.allocations] == ["cex_dex"]
    assert plan.allocated_capital_usd == 10000.0
    assert plan.unused_cash_usd == 20000.0
    assert plan.degraded is True


@pytest.mark.asyncio
async def test_all_candidate_families_can_fail_closed_to_cash_without_allocator_crash():
    service = UnifiedPaperAllocatorService(
        FailingCore(),  # type: ignore[arg-type]
        FailingCexDexPromotion(),  # type: ignore[arg-type]
        FailingAlphaFactory(),  # type: ignore[arg-type]
    )

    plan = await service.allocate(total_capital_usd=250000.0)
    assert plan.candidate_count == 0
    assert plan.allocations == []
    assert plan.allocated_capital_usd == 0.0
    assert plan.unused_cash_usd == 250000.0
    assert plan.degraded is True
    assert set(plan.family_failures) == {"core_cex", "cex_dex", "alpha"}
    assert plan.authorizes_execution is False
    assert plan.live_execution_eligible is False
