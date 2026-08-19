from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.cex_dex_composite_statistics import (
    CompositeEdgeStatisticalQualification,
    ProbabilityEstimate as CompositeProbability,
)
from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_operations import CexDexOperationalQualification
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_statistics import DexStatisticalQualification, ProbabilityEstimate
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote


NOW = datetime.now(timezone.utc)


def evidence(edge_bps: float = 100.0) -> CexDexCompositeEvidence:
    return CexDexCompositeEvidence(
        evidence_id="edge-current",
        frontier_id="frontier",
        asset="ETH",
        route_direction="buy_asset",
        target_notional_usd=1000.0,
        route_contiguous_acceptable=True,
        cex_venue="Coinbase",
        cex_symbol="ETH-USD",
        cex_quote_currency="USD",
        cex_reference_price=4000.0,
        route_quote_currency="USD",
        route_effective_asset_price=3960.0,
        route_quote_notional_usd_proxy=1000.0,
        conversion_depth_quote=None,
        conversion_risk_haircut_bps=0.0,
        cex_taker_fee_bps=10.0,
        gas_cost_bps=5.0,
        gross_edge_after_conversion_depth_bps=edge_bps + 15.0,
        net_research_edge_bps=edge_bps,
        observed_at=NOW,
        evidence_complete=True,
        blocked_reason="research-only",
    )


def route_model() -> DexStatisticalQualification:
    probability = ProbabilityEstimate(
        successes=100, sample_count=100, probability=1.0, ci_lower=0.95, ci_upper=1.0, ci_width=0.05
    )
    return DexStatisticalQualification(
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=1000.0,
        reference_horizon_seconds=5.0,
        notional_tolerance_fraction=0.10,
        confidence_level=0.95,
        shadow_effective_sample_count=100,
        frontier_effective_sample_count=100,
        adverse_tail_sample_count=100,
        survival=probability,
        frontier_acceptance=probability,
        p95_adverse_deterioration_bps=5.0,
        route_change_rate=0.01,
        statistically_qualified=True,
    )


def composite_model(current: CexDexCompositeEvidence) -> CompositeEdgeStatisticalQualification:
    probability = CompositeProbability(
        successes=100, sample_count=100, probability=1.0, ci_lower=0.90, ci_upper=1.0, ci_width=0.10
    )
    from inefficiency_engine.cex_dex_shadow import composite_edge_key

    return CompositeEdgeStatisticalQualification(
        composite_key=composite_edge_key(current),
        evidence_id=current.evidence_id,
        asset=current.asset,
        route_direction=current.route_direction,
        target_notional_usd=current.target_notional_usd,
        cex_venue=current.cex_venue,
        cex_symbol=current.cex_symbol,
        reference_horizon_seconds=5.0,
        effective_sample_count=100,
        adverse_tail_sample_count=100,
        retained_edge_sample_count=100,
        hurdle_survival=probability,
        p95_adverse_deterioration_bps=10.0,
        p10_retained_edge_fraction=0.80,
        statistically_qualified=True,
    )


class FakeCompositeService:
    def __init__(self, current):
        self.current = current

    async def probe(self):
        return SimpleNamespace(evidence=[self.current])


class FakeCore:
    def __init__(self):
        self.settings = Settings(dex_statistical_min_net_edge_bps=12.0)

    async def collect_live_evidence(self):
        return SimpleNamespace(market_quotes=[
            MarketQuote(
                venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT,
                symbol="ETH-USD", quote_currency="USD", bid=4000, ask=4001, mid=4000.5,
                observed_at=NOW, source="test",
            ),
            MarketQuote(
                venue="Kraken", asset="ETH", market_kind=MarketKind.SPOT,
                symbol="ETH-USD", quote_currency="USD", bid=3999, ask=4002, mid=4000.5,
                observed_at=NOW, source="test",
            ),
        ])


@pytest.mark.asyncio
async def test_final_gate_promotes_only_after_all_independent_layers_pass(tmp_path):
    current = evidence()
    store = EvidenceStore(tmp_path / "promotion.sqlite3")
    service = CexDexPaperPromotionService(
        FakeCore(),  # type: ignore[arg-type]
        FakeCompositeService(current),  # type: ignore[arg-type]
        store,
    )
    service.route_statistics.model = lambda **_: route_model()  # type: ignore[method-assign]
    service.composite_statistics.model = lambda _: composite_model(current)  # type: ignore[method-assign]

    probe = await service.live_qualification(paper_inventory_usd_per_side=5000.0)

    assert probe.evidence_count == 1
    assert probe.paper_allocation_eligible_count == 1
    row = probe.qualifications[0]
    assert row.stablecoin_depth_required is False
    assert row.stablecoin_depth_qualified is True
    assert row.operations.paper_operationally_qualified is True
    assert row.conservative_capture_edge_bps == pytest.approx(72.0)
    assert row.paper_capital_required_usd == 2000.0
    assert row.paper_allocation_eligible is True
    assert row.executable_eligible is False
    assert row.live_execution_eligible is False


@pytest.mark.asyncio
async def test_paper_allocator_reserves_both_legs_and_never_authorizes_execution(tmp_path):
    current = evidence()
    store = EvidenceStore(tmp_path / "allocator.sqlite3")
    service = CexDexPaperPromotionService(
        FakeCore(),  # type: ignore[arg-type]
        FakeCompositeService(current),  # type: ignore[arg-type]
        store,
    )
    service.route_statistics.model = lambda **_: route_model()  # type: ignore[method-assign]
    service.composite_statistics.model = lambda _: composite_model(current)  # type: ignore[method-assign]

    plan = await service.paper_allocation(
        total_capital_usd=10000.0,
        max_venue_fraction=0.50,
        max_asset_fraction=0.50,
        max_allocations=2,
    )

    assert len(plan.allocations) == 1
    allocation = plan.allocations[0]
    assert allocation.capital_required_usd == 2000.0
    assert allocation.notional_usd_per_leg == 1000.0
    assert allocation.conservative_expected_profit_usd == pytest.approx(7.2)
    assert allocation.expected_return_on_reserved_capital == pytest.approx(0.0036)
    assert allocation.authorizes_execution is False
    assert plan.allocated_capital_usd == 2000.0
    assert plan.unused_cash_usd == 8000.0
    assert plan.authorizes_execution is False
