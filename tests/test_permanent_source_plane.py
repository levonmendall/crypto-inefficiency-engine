from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine import lightweight_portfolio_worker, permanent_source_worker
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    OrderBookLevel,
    OrderBookSnapshot,
)
from inefficiency_engine.permanent_source_plane import (
    ALPHA_L2_WORKER_ID,
    PERMANENT_SOURCE_WORKER_ID,
    PermanentSourcePlane,
    permanent_source_plane_current,
    source_executable_deadline_seconds,
    source_market_interval_seconds,
)
from inefficiency_engine.priority_source_collection import SOURCE_REFRESH_WORKER_ID


NOW = datetime(2026, 8, 22, 21, 0, tzinfo=timezone.utc)


class _HeartbeatStore:
    def __init__(self, heartbeat):
        self.heartbeat = heartbeat

    def latest_worker_heartbeat(self, worker_id):
        assert worker_id == PERMANENT_SOURCE_WORKER_ID
        return self.heartbeat


def _heartbeat(*, age_seconds: float, state: str):
    return SimpleNamespace(
        observed_at=NOW - timedelta(seconds=age_seconds),
        state=state,
    )


def test_current_permanent_source_owner_accepts_current_degraded_loop():
    assert permanent_source_plane_current(
        _HeartbeatStore(_heartbeat(age_seconds=45, state="degraded")),
        now=NOW,
        max_age_seconds=120,
    ) is True


def test_permanent_source_owner_rejects_stale_or_error_loop():
    assert permanent_source_plane_current(
        _HeartbeatStore(_heartbeat(age_seconds=121, state="success")),
        now=NOW,
        max_age_seconds=120,
    ) is False
    assert permanent_source_plane_current(
        _HeartbeatStore(_heartbeat(age_seconds=10, state="error")),
        now=NOW,
        max_age_seconds=120,
    ) is False


class _FakeRegistry:
    def __init__(self):
        self.coinbase = SimpleNamespace(assets=("BTC",))

    async def collect_inputs(self):
        quote = MarketQuote(
            venue="Coinbase",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-USD",
            quote_currency="USD",
            contract_key="spot",
            bid=60000.0,
            ask=60001.0,
            mid=60000.5,
            observed_at=NOW,
            source="coinbase-exchange:ticker",
        )
        return [], [quote], [
            ProviderStatus(
                provider="coinbase-exchange:ticker",
                ok=True,
                item_count=1,
                observed_at=NOW,
            )
        ]

    async def collect_books_for_opportunities(self, opportunities):
        assert opportunities
        book = OrderBookSnapshot(
            venue="Coinbase",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-USD",
            quote_currency="USD",
            contract_key="spot",
            bids=[OrderBookLevel(price=60000.0, size=1.0)],
            asks=[OrderBookLevel(price=60001.0, size=1.0)],
            observed_at=NOW,
            source="coinbase-exchange:book-level2",
        )
        return [book], [
            ProviderStatus(
                provider="coinbase-exchange:book-level2",
                ok=True,
                item_count=1,
                observed_at=NOW,
            )
        ]


class _FakeFastMarket:
    def __init__(self, registry):
        self.registry = registry

    async def collect_inputs(self, assets):
        assert assets == ("BTC",)
        return await self.registry.collect_inputs()

    async def collect_books(self, requests):
        return await self.registry.collect_books_for_opportunities(requests)


@pytest.mark.asyncio
async def test_permanent_market_l2_cycle_persists_source_truth_without_research(tmp_path):
    store = EvidenceStore(tmp_path / "permanent-source.sqlite")
    plane = PermanentSourcePlane(store)
    plane.registry = _FakeRegistry()
    plane.fast_market = _FakeFastMarket(plane.registry)

    snapshot = await plane.refresh_market_l2_snapshot()

    assert len(snapshot.market_quotes) == 1
    assert len(snapshot.order_books) == 1
    assert snapshot.analysis_config["permanent_source_plane"] is True
    assert snapshot.analysis_config["executable_hot_path"] is True
    assert snapshot.analysis_config["broad_research_sweep"] is False
    assert snapshot.analysis_config["disposable_research_required"] is False
    heartbeat = store.latest_worker_heartbeat(ALPHA_L2_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["permanent_source_plane"] is True
    assert heartbeat.detail["executable_hot_path"] is True


@pytest.mark.asyncio
async def test_broad_research_market_sweep_never_publishes_executable_authority(tmp_path):
    store = EvidenceStore(tmp_path / "research-market.sqlite")
    plane = PermanentSourcePlane(store)
    plane.registry = _FakeRegistry()

    snapshot = await plane.refresh_research_market_snapshot()

    assert len(snapshot.market_quotes) == 1
    assert snapshot.order_books == []
    assert snapshot.analysis_config["research_market_sweep"] is True
    assert snapshot.analysis_config["broad_research_sweep"] is True
    assert snapshot.analysis_config["permanent_source_plane"] is False
    assert snapshot.analysis_config["executable_hot_path"] is False
    assert snapshot.analysis_config["allocation_authority"] is False


@pytest.mark.asyncio
async def test_source_progress_pulse_does_not_claim_fresh_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "source-progress.sqlite")
    cycle_done = asyncio.Event()
    pulse = asyncio.create_task(
        permanent_source_worker._source_cycle_progress_pulse(
            store,
            cycle_done=cycle_done,
            interval_seconds=0.01,
        )
    )

    await asyncio.sleep(0.035)
    heartbeat = store.latest_worker_heartbeat(PERMANENT_SOURCE_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "running"
    assert heartbeat.detail["progress_pulse"] is True
    assert heartbeat.detail["stage"] == "executable_market_l2_cycle_in_progress"
    assert heartbeat.detail["fresh_evidence_published"] is False
    assert heartbeat.detail["executable_hot_path"] is True
    assert heartbeat.detail["priority_source_tail_decoupled"] is True
    assert heartbeat.detail["broad_research_sweep_decoupled"] is True
    assert heartbeat.detail["separate_python_process"] is True
    assert heartbeat.detail["allocation_authority"] is False

    cycle_done.set()
    await pulse
    store.record_worker_heartbeat(
        worker_id=PERMANENT_SOURCE_WORKER_ID,
        state="success",
        detail={"terminal": True},
    )
    await asyncio.sleep(0.02)
    terminal = store.latest_worker_heartbeat(PERMANENT_SOURCE_WORKER_ID)
    assert terminal is not None
    assert terminal.state == "success"
    assert terminal.detail["terminal"] is True


@pytest.mark.asyncio
async def test_priority_source_progress_pulse_is_independent_from_market_l2(tmp_path):
    store = EvidenceStore(tmp_path / "priority-progress.sqlite")
    cycle_done = asyncio.Event()
    pulse = asyncio.create_task(
        permanent_source_worker._priority_source_progress_pulse(
            store,
            cycle_done=cycle_done,
            interval_seconds=0.01,
        )
    )

    await asyncio.sleep(0.035)
    heartbeat = store.latest_worker_heartbeat(SOURCE_REFRESH_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "running"
    assert heartbeat.detail["progress_pulse"] is True
    assert heartbeat.detail["stage"] == "priority_source_cycle_in_progress"
    assert heartbeat.detail["market_l2_cadence_independent"] is True
    assert heartbeat.detail["allocation_authority"] is False

    cycle_done.set()
    await pulse


def test_executable_source_cadence_is_start_to_start_not_runtime_plus_sleep():
    assert permanent_source_worker._remaining_cycle_delay(
        interval_seconds=30.0,
        started_monotonic=100.0,
        now_monotonic=105.0,
    ) == 25.0
    assert permanent_source_worker._remaining_cycle_delay(
        interval_seconds=30.0,
        started_monotonic=100.0,
        now_monotonic=145.0,
    ) == 0.0
    assert source_market_interval_seconds() == 30.0
    assert source_executable_deadline_seconds() == 45.0
    assert source_executable_deadline_seconds() < 120.0


def test_source_provider_work_is_not_hosted_on_portfolio_event_loop():
    portfolio_source = inspect.getsource(lightweight_portfolio_worker)
    source_worker = inspect.getsource(permanent_source_worker)

    assert "PermanentSourcePlane" not in portfolio_source
    assert "resolve_top_volume_assets" not in portfolio_source
    assert "_permanent_source_refresh_loop" not in portfolio_source
    assert "_volume_universe_refresh_loop" not in portfolio_source
    assert 'name="research-dashboard-projection-refresh"' in portfolio_source

    assert "PermanentSourcePlane" in source_worker
    assert "resolve_top_volume_assets" in source_worker
    assert 'name="permanent-executable-source-refresh"' in source_worker
    assert 'name="research-market-universe-refresh"' in source_worker
    assert 'name="priority-source-refresh"' in source_worker
    assert 'name="volume-universe-refresh"' in source_worker
    assert "_source_cycle_progress_pulse" in source_worker
    assert "_priority_source_progress_pulse" in source_worker
    assert "allocation_authority" in source_worker


def test_market_l2_loop_does_not_wait_for_research_or_priority_source_tail():
    market_loop = inspect.getsource(permanent_source_worker._permanent_source_refresh_loop)
    research_loop = inspect.getsource(permanent_source_worker._research_market_refresh_loop)
    priority_loop = inspect.getsource(permanent_source_worker._priority_source_refresh_loop)

    assert "refresh_market_l2_snapshot" in market_loop
    assert "refresh_research_market_snapshot" not in market_loop
    assert ".priority.run_cycle" not in market_loop

    assert "refresh_research_market_snapshot" in research_loop
    assert "refresh_market_l2_snapshot" not in research_loop
    assert ".priority.run_cycle" not in research_loop

    assert ".priority.run_cycle" in priority_loop
    assert "refresh_market_l2_snapshot" not in priority_loop
    assert "refresh_research_market_snapshot" not in priority_loop
