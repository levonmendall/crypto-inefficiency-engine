from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.memory_bounded_qualified_opportunity import (
    MemoryBoundedQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityExecutability,
    OpportunityLeg,
    Side,
    Strategy,
)


def _quote(at: datetime, *, asset: str = "BTC", mid: float = 100.0) -> MarketQuote:
    return MarketQuote(
        venue="Coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot",
        mid=mid,
        observed_at=at,
        source="test",
    )


def _opportunity(opportunity_id: str, at: datetime) -> Opportunity:
    return Opportunity(
        id=opportunity_id,
        strategy=Strategy.FUNDING_DISPERSION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="Bybit",
                asset="BTC",
                market_kind=MarketKind.PERPETUAL,
                side=Side.LONG,
                symbol="BTCUSDT",
                quote_currency="USDT",
                contract_key="continuous",
            ),
            OpportunityLeg(
                venue="OKX",
                asset="BTC",
                market_kind=MarketKind.PERPETUAL,
                side=Side.SHORT,
                symbol="BTC-USDT-SWAP",
                quote_currency="USDT",
                contract_key="continuous",
            ),
        ],
        gross_edge_bps_per_hour=1.0,
        modeled_cost_bps=1.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=0.1,
        net_edge_bps_per_hour=0.5,
        net_annualized_return=0.10,
        observed_at=at,
        expires_at=at + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_bridge_never_materializes_full_scan_and_only_loads_executable_opportunities(
    tmp_path, monkeypatch
):
    now = datetime.now(timezone.utc)
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "bridge-projection.sqlite3")
    used = _opportunity("used-opportunity", now)
    unused = _opportunity("unused-opportunity", now)
    store.record_scan(
        funding_quotes=[],
        market_quotes=[_quote(now)],
        opportunities=[used, unused],
        providers=[],
        started_at=now,
        completed_at=now,
        order_books=[],
        executability=[
            OpportunityExecutability(
                opportunity_id=used.id,
                strategy=used.strategy,
                asset=used.asset,
                observed_at=now,
                tiers=[],
            )
        ],
    )

    def forbidden_load_scan(*args, **kwargs):
        raise AssertionError("qualified-opportunity bridge must never call EvidenceStore.load_scan")

    monkeypatch.setattr(store, "load_scan", forbidden_load_scan)
    publisher = MemoryBoundedQualifiedOpportunityBridgePublisher(
        SimpleNamespace(settings=settings),
        store,
        SimpleNamespace(alpha_factory=None),  # type: ignore[arg-type]
    )

    projection = publisher._latest_scan()
    assert projection is not None
    assert projection.scan_id
    assert [item.id for item in projection.opportunities] == [used.id]
    assert [item.opportunity_id for item in projection.executability] == [used.id]
    assert projection.funding_quotes == []
    assert projection.providers == []

    published = await publisher.publish_latest(total_capital_usd=250_000.0)
    assert published is not None
    assert published.source_scan_id == projection.scan_id


def test_alpha_history_stream_is_exactly_scoped_to_active_snapshot_and_strategy_lookbacks(tmp_path):
    now = datetime.now(timezone.utc)
    settings = Settings(alpha_history_hours=72.0)
    store = EvidenceStore(tmp_path / "alpha-history.sqlite3")

    samples = [
        _quote(now - timedelta(hours=60), mid=90.0),
        _quote(now - timedelta(hours=47), mid=91.0),
        _quote(now - timedelta(hours=24), asset="ETH", mid=3_000.0),
        _quote(now, mid=100.0),
    ]
    for index, quote in enumerate(samples):
        store.record_scan(
            funding_quotes=[],
            market_quotes=[quote],
            opportunities=[],
            providers=[],
            started_at=quote.observed_at,
            completed_at=quote.observed_at,
            scan_id=f"history-{index}",
        )

    factory = MemoryBoundedExpandedAlphaFactoryService(
        SimpleNamespace(settings=settings),  # type: ignore[arg-type]
        store,
    )
    current = samples[-1]
    snapshot = ScanSnapshot(
        scan_id="current",
        started_at=now,
        completed_at=now,
        providers=[],
        funding_quotes=[],
        market_quotes=[current],
        opportunities=[],
        order_books=[],
        executability=[],
        analysis_config={},
    )

    history = factory._history_for_snapshot(snapshot)
    key = ("Coinbase", "BTC", MarketKind.SPOT)
    assert set(history) == {key}
    assert [row.mid for row in history[key]] == [91.0, 100.0]
    assert all(row.observed_at >= now - timedelta(hours=48) for row in history[key])
    assert factory._effective_history_hours() == pytest.approx(48.0)
