from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.memory_bounded_qualified_opportunity import (
    MemoryBoundedQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.models import (
    FundingQuote,
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityExecutability,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
)
from inefficiency_engine.permanent_source_plane import PERMANENT_SOURCE_WORKER_ID


NOW = datetime(2026, 8, 23, 15, 30, tzinfo=timezone.utc)


def _market_quote() -> MarketQuote:
    return MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot",
        bid=60_000.0,
        ask=60_001.0,
        mid=60_000.5,
        observed_at=NOW,
        source="coinbase-exchange:ticker",
    )


def _funding_quote() -> FundingQuote:
    return FundingQuote(
        venue="Hyperliquid",
        asset="BTC",
        rate=0.0001,
        interval_hours=1.0,
        symbol="BTC",
        quote_currency="USD",
        contract_key="continuous",
        observed_at=NOW,
        source="hyperliquid:funding",
    )


def _book() -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot",
        bids=[OrderBookLevel(price=60_000.0, size=2.0)],
        asks=[OrderBookLevel(price=60_001.0, size=2.0)],
        observed_at=NOW,
        source="coinbase-exchange:book-level2",
    )


def _opportunity() -> Opportunity:
    return Opportunity(
        id="bridge-opportunity",
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.LONG,
                symbol="BTC-USD",
                quote_currency="USD",
                contract_key="spot",
                reference_price=60_000.5,
            ),
            OpportunityLeg(
                venue="Kraken",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.SHORT,
                symbol="XBT/USD",
                quote_currency="USD",
                contract_key="spot",
                reference_price=60_050.0,
            ),
        ],
        gross_edge_bps_per_hour=8.0,
        modeled_cost_bps=2.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=1.0,
        net_edge_bps_per_hour=5.0,
        net_annualized_return=0.10,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        confidence="medium",
    )


def _record_full_source_scan(store: EvidenceStore) -> str:
    return store.record_scan(
        funding_quotes=[_funding_quote()],
        market_quotes=[_market_quote()],
        opportunities=[],
        providers=[],
        started_at=NOW,
        completed_at=NOW,
        analysis_config={"permanent_source_plane": True, "paper_only": True},
        order_books=[_book()],
    )


def test_bridge_ignores_newer_l2_only_scan(tmp_path):
    store = EvidenceStore(tmp_path / "bridge-source.sqlite")
    source_scan_id = _record_full_source_scan(store)
    store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        providers=[],
        started_at=NOW + timedelta(seconds=1),
        completed_at=NOW + timedelta(seconds=1),
        analysis_config={"alpha_l2_sampling": True, "paper_only": True},
        order_books=[_book().model_copy(update={"observed_at": NOW + timedelta(seconds=1)})],
    )

    publisher = object.__new__(MemoryBoundedQualifiedOpportunityBridgePublisher)
    publisher.store = store
    with store.engine.connect() as db:
        row, config = publisher._select_full_scan(db)

    assert row is not None
    assert str(row["scan_id"]) == source_scan_id
    assert config["permanent_source_plane"] is True


def test_bridge_synthesizes_core_executability_from_durable_source(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "bridge-projection.sqlite")
    source_scan_id = _record_full_source_scan(store)
    opportunity = _opportunity()
    calls = {"analyze": 0, "qualify": 0}

    class FakeCore:
        settings = SimpleNamespace(capital_tiers_usd=(1_000.0,))

        def analyze(self, funding_quotes, market_quotes):
            calls["analyze"] += 1
            assert len(funding_quotes) == 1
            assert len(market_quotes) == 1
            return [opportunity]

        def empirical_latency_resolver(self):
            return SimpleNamespace(resolve=lambda *args, **kwargs: None)

    def fake_qualify(item, books, settings, **kwargs):
        calls["qualify"] += 1
        assert item.id == opportunity.id
        assert settings.capital_tiers_usd == (1_000.0,)
        return OpportunityExecutability(
            opportunity_id=item.id,
            strategy=item.strategy,
            asset=item.asset,
            observed_at=NOW,
            tiers=[],
        )

    monkeypatch.setattr(
        "inefficiency_engine.memory_bounded_qualified_opportunity.qualify_opportunity",
        fake_qualify,
    )
    publisher = object.__new__(MemoryBoundedQualifiedOpportunityBridgePublisher)
    publisher.store = store
    publisher.core = FakeCore()

    snapshot = publisher._latest_scan()

    assert snapshot is not None
    assert snapshot.scan_id == source_scan_id
    assert snapshot.funding_quotes
    assert snapshot.opportunities == [opportunity]
    assert len(snapshot.executability) == 1
    assert snapshot.analysis_config["bridge_projection_synthesized_executability"] is True
    assert snapshot.analysis_config["bridge_projection_provider_requests"] == 0
    assert calls == {"analyze": 1, "qualify": 1}


@pytest.mark.asyncio
async def test_alpha_factory_reuses_current_permanent_source_without_network(tmp_path):
    store = EvidenceStore(tmp_path / "alpha-source-reuse.sqlite")
    source_scan_id = _record_full_source_scan(store)
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        scan_id=source_scan_id,
        detail={"market_refresh_complete": True},
    )

    service = object.__new__(DisposableExpandedAlphaFactoryService)
    service.store = store
    service.core = SimpleNamespace(
        settings=SimpleNamespace(worker_heartbeat_stale_seconds=180.0)
    )
    network_called = False

    async def forbidden_collector():
        nonlocal network_called
        network_called = True
        raise AssertionError("permanent source reuse must not launch provider work")

    snapshot = await service.refresh_l2_source_snapshot(forbidden_collector)

    assert snapshot.scan_id == source_scan_id
    assert network_called is False
    assert snapshot.analysis_config["reused_by_alpha_factory"] is True
    assert snapshot.analysis_config["duplicate_provider_requests"] == 0
