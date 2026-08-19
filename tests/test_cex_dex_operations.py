from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_operations import (
    CexDexOperationalQualificationService,
    HedgeRecoveryPolicy,
    PaperInventoryPolicy,
    qualify_cex_dex_operations,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind, MarketQuote


NOW = datetime.now(timezone.utc)


def evidence(direction: str = "buy_asset", *, edge_bps: float = 60.0, notional: float = 1000.0):
    return CexDexCompositeEvidence(
        evidence_id=f"edge-{direction}-{notional}",
        frontier_id="frontier",
        asset="ETH",
        route_direction=direction,
        target_notional_usd=notional,
        route_contiguous_acceptable=True,
        cex_venue="Coinbase",
        cex_symbol="ETH-USD",
        cex_quote_currency="USD",
        cex_reference_price=4000.0,
        route_quote_currency="USDC",
        route_effective_asset_price=3990.0,
        route_quote_notional_usd_proxy=notional,
        conversion_depth_quote=None,
        conversion_risk_haircut_bps=2.0,
        cex_taker_fee_bps=10.0,
        gas_cost_bps=5.0,
        gross_edge_after_conversion_depth_bps=edge_bps + 17.0,
        net_research_edge_bps=edge_bps,
        observed_at=NOW,
        evidence_complete=True,
        blocked_reason="research-only",
    )


def inventory(limit: float = 5000.0) -> PaperInventoryPolicy:
    return PaperInventoryPolicy(
        cex_asset_inventory_usd_per_venue=limit,
        cex_quote_inventory_usd_per_venue=limit,
        dex_asset_inventory_usd=limit,
        dex_quote_inventory_usd=limit,
    )


def hedge(buffer: float = 20.0) -> HedgeRecoveryPolicy:
    return HedgeRecoveryPolicy(
        max_unhedged_seconds=2.0,
        reserve_buffer_bps=buffer,
        minimum_alternate_cex_venues=1,
    )


def test_buy_route_requires_cex_asset_and_dex_quote_inventory():
    result = qualify_cex_dex_operations(
        evidence("buy_asset"),
        alternate_cex_venues=["Coinbase", "Kraken"],
        inventory=inventory(),
        hedge_policy=hedge(),
        settings=Settings(dex_statistical_min_net_edge_bps=12.0),
    )

    assert result.required_cex_asset_inventory_usd == 1000.0
    assert result.required_cex_quote_inventory_usd == 0.0
    assert result.required_dex_asset_inventory_usd == 0.0
    assert result.required_dex_quote_inventory_usd == 1000.0
    assert result.inventory_prefunded is True
    assert result.settlement_dependency_during_trade is False
    assert result.hedge_recovery_qualified is True
    assert result.paper_operationally_qualified is True
    assert result.allocation_eligible is False
    assert result.live_balance_verified is False
    assert result.live_execution_eligible is False


def test_sell_route_requires_cex_quote_and_dex_asset_inventory():
    result = qualify_cex_dex_operations(
        evidence("sell_asset"),
        alternate_cex_venues=["Kraken"],
        inventory=inventory(),
        hedge_policy=hedge(),
        settings=Settings(dex_statistical_min_net_edge_bps=12.0),
    )

    assert result.required_cex_asset_inventory_usd == 0.0
    assert result.required_cex_quote_inventory_usd == 1000.0
    assert result.required_dex_asset_inventory_usd == 1000.0
    assert result.required_dex_quote_inventory_usd == 0.0
    assert result.paper_operationally_qualified is True


def test_operational_qualification_fails_closed_on_inventory_recovery_or_edge():
    result = qualify_cex_dex_operations(
        evidence(edge_bps=25.0, notional=5000.0),
        alternate_cex_venues=["Coinbase"],
        inventory=inventory(limit=1000.0),
        hedge_policy=hedge(buffer=20.0),
        settings=Settings(dex_statistical_min_net_edge_bps=12.0),
    )

    assert result.inventory_prefunded is False
    assert result.settlement_qualified is False
    assert result.hedge_recovery_qualified is False
    assert result.paper_operationally_qualified is False
    assert "paper pre-funded inventory limit is insufficient for both legs" in result.blockers
    assert "insufficient independent CEX venues for modeled hedge recovery" in result.blockers
    assert "net edge does not survive the configured hedge-recovery reserve" in result.blockers


class FakeCompositeService:
    settings = Settings(dex_statistical_min_net_edge_bps=12.0)

    async def probe(self):
        return SimpleNamespace(evidence=[evidence(edge_bps=60.0)])


class FakeCore:
    def __init__(self):
        self.settings = Settings(dex_statistical_min_net_edge_bps=12.0)

    async def collect_live_evidence(self):
        return SimpleNamespace(
            market_quotes=[
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
            ]
        )


@pytest.mark.asyncio
async def test_service_discovers_independent_recovery_venue_without_claiming_live_balance():
    service = CexDexOperationalQualificationService(
        FakeCore(),  # type: ignore[arg-type]
        FakeCompositeService(),  # type: ignore[arg-type]
        inventory_policy=inventory(),
        hedge_policy=hedge(),
    )
    probe = await service.live_qualification()

    assert probe.evidence_count == 1
    assert probe.paper_operationally_qualified_count == 1
    assert probe.qualifications[0].alternate_cex_venues == ["Kraken"]
    assert probe.live_balance_verified is False
    assert probe.live_execution_eligible is False
