from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.asset_universe import MAX_LIQUID_RESEARCH_ASSETS
from inefficiency_engine.disposable_alpha_factory import (
    ALPHA_L2_WORKER_ID,
    DisposableExpandedAlphaFactoryService,
)
from inefficiency_engine.disposable_research_worker import _error_keys
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus, ScanSnapshot
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    OrderBookLevel,
    OrderBookSnapshot,
)
from inefficiency_engine.volume_universe import TOP_VOLUME_ASSET_COUNT


NOW = datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc)


def test_compatibility_universe_matches_authoritative_top_volume_count():
    assert MAX_LIQUID_RESEARCH_ASSETS == TOP_VOLUME_ASSET_COUNT == 25


def test_disposable_alpha_cycle_samples_and_persists_l2_without_structural_opportunity(
    tmp_path,
    monkeypatch,
):
    store = EvidenceStore(tmp_path / "alpha-l2.sqlite3")
    quote = MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        mid=60_000.0,
        bid=59_999.0,
        ask=60_001.0,
        observed_at=NOW,
        source="test",
    )
    snapshot = ScanSnapshot(
        scan_id="quote-scan",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote],
        opportunities=[],
        order_books=[],
        executability=[],
    )
    book = OrderBookSnapshot(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        bids=[OrderBookLevel(price=59_999.0, size=2.0)],
        asks=[OrderBookLevel(price=60_001.0, size=2.0)],
        observed_at=NOW,
        source="test-l2",
    )

    class Registry:
        coinbase = SimpleNamespace(assets=("BTC",))

        async def collect_books_for_opportunities(self, opportunities):
            assert len(opportunities) == 1
            assert opportunities[0].legs[0].asset == "BTC"
            return [book], [ProviderStatus(provider="coinbase-l2:test", ok=True, item_count=1)]

    calls: list[str] = []

    async def quote_only():
        calls.append("quote_only")
        return snapshot

    core = SimpleNamespace(
        collect_live_evidence=quote_only,
        adapter_registry=Registry(),
    )
    service = object.__new__(DisposableExpandedAlphaFactoryService)
    service.core = core
    service.store = store

    async def fake_parent_run(self, *, total_capital_usd=None):
        return await self.core.collect_live_evidence()

    monkeypatch.setattr(
        AllLaneEvidenceFactoryService,
        "run_evidence_cycle",
        fake_parent_run,
    )

    result = asyncio.run(service.run_evidence_cycle())
    assert calls == ["quote_only"]
    assert result.opportunities == []
    assert len(result.order_books) == 1
    assert result.order_books[0].symbol == "BTC-USD"
    assert core.collect_live_evidence is quote_only

    with store.engine.connect() as db:
        assert db.execute(store.order_books.select()).first() is not None
    heartbeat = store.latest_worker_heartbeat(ALPHA_L2_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["structural_opportunity_required"] is False


def test_research_heartbeat_error_inventory_is_explicit():
    detail = {
        "sequence": 7,
        "alpha_forward_evidence_error_type": "RuntimeError",
        "qualified_bridge_error_type": "TimeoutError",
        "alpha_candidate_count": 0,
    }
    assert _error_keys(detail) == [
        "alpha_forward_evidence_error_type",
        "qualified_bridge_error_type",
    ]
