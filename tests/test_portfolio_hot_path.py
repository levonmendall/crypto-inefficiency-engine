from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_factory import AlphaCandidate
from inefficiency_engine.bounded_alpha_factory import BoundedExpandedAlphaFactoryService
from inefficiency_engine.canonical_allocator import CanonicalPortfolioAllocatorService
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    OrderBookLevel,
    OrderBookSnapshot,
)


NOW = datetime.now(timezone.utc)


def _candidate() -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id="alpha:test",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=60000.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.0005,
        expected_net_return=0.0095,
        expected_profit_usd=95.0,
        notional_usd=10000.0,
        capital_required_usd=10000.0,
        confidence_score=0.8,
        regime="normal",
    )


def _book() -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot",
        bids=[OrderBookLevel(price=59999.0, size=10.0)],
        asks=[OrderBookLevel(price=60001.0, size=10.0)],
        observed_at=NOW,
        source="test",
        request_latency_ms=5.0,
    )


@pytest.mark.asyncio
async def test_alpha_promotion_reuses_scan_l2_without_second_provider_call(tmp_path, monkeypatch):
    settings = Settings(
        coinbase_spot_taker_fee_bps=1.0,
        alpha_execution_risk_floor_bps=1.0,
        alpha_min_current_net_return=0.0001,
        max_order_book_age_seconds=30.0,
    )

    class NoRefetchRegistry:
        order_book_timeout_seconds = 0.05

        def book_request(self, leg):
            raise AssertionError("snapshot L2 should prevent a second provider request")

    core = SimpleNamespace(settings=settings, adapter_registry=NoRefetchRegistry())
    store = EvidenceStore(tmp_path / "bounded-alpha.sqlite3")
    factory = BoundedExpandedAlphaFactoryService(core, store)  # type: ignore[arg-type]
    item = _candidate()
    snapshot = ScanSnapshot(
        scan_id="scan",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        order_books=[_book()],
        executability=[],
    )

    monkeypatch.setattr(factory, "discover", lambda snapshot, total_capital_usd: [item])
    monkeypatch.setattr(
        factory,
        "qualification",
        lambda candidate: SimpleNamespace(
            statistically_qualified=True,
            mean_realized_net_return_ci_lower=0.005,
        ),
    )
    monkeypatch.setattr(
        factory,
        "strategy_health",
        lambda candidate: SimpleNamespace(
            healthy_for_paper_allocation=True,
            capital_multiplier=1.0,
            health_score=1.0,
            recent_mean_net_return=0.005,
            recent_hit_rate=0.75,
            forecast_capture_ratio_median=0.8,
            recent_to_long_run_ratio=1.0,
            max_compounded_drawdown=0.01,
            trailing_loss_streak=0,
        ),
    )

    promoted = await factory.promoted_candidates(snapshot, total_capital_usd=250000.0)

    assert len(promoted) == 1
    assert promoted[0].paper_allocation_eligible is True
    assert promoted[0].estimated_cost_return > 0


@pytest.mark.asyncio
async def test_alpha_fallback_l2_request_is_bounded(tmp_path):
    settings = Settings()

    async def never_returns():
        await asyncio.sleep(10.0)
        return _book()

    class SlowRegistry:
        order_book_timeout_seconds = 0.01

        def book_request(self, leg):
            return SimpleNamespace(awaitable=never_returns())

    core = SimpleNamespace(settings=settings, adapter_registry=SlowRegistry())
    store = EvidenceStore(tmp_path / "bounded-fallback.sqlite3")
    factory = BoundedExpandedAlphaFactoryService(core, store)  # type: ignore[arg-type]

    assert await factory._bounded_current_l2_cost(_candidate()) is None


@pytest.mark.asyncio
async def test_canonical_allocator_consumes_latest_persisted_executable_scan(tmp_path):
    settings = Settings(max_quote_age_seconds=120.0, max_order_book_age_seconds=30.0)
    store = EvidenceStore(tmp_path / "canonical-hot-path.sqlite3")
    quote = MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot",
        bid=59999.0,
        ask=60001.0,
        mid=60000.0,
        observed_at=NOW,
        source="test",
    )
    scan_id = store.record_scan(
        funding_quotes=[],
        market_quotes=[quote],
        opportunities=[],
        providers=[],
        started_at=NOW,
        completed_at=NOW,
        order_books=[_book()],
        executability=[],
    )

    class Core:
        def __init__(self):
            self.settings = settings

        async def collect_live_executability(self):
            raise AssertionError("canonical allocator must consume the persisted scan")

    class Alpha:
        def __init__(self):
            self.store = store
            self.seen_scan_id = None

        async def promoted_candidates(self, snapshot, *, total_capital_usd):
            self.seen_scan_id = snapshot.scan_id
            return []

    alpha = Alpha()
    allocator = CanonicalPortfolioAllocatorService(
        Core(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        alpha,  # type: ignore[arg-type]
    )

    plan = await allocator.allocate(total_capital_usd=250000.0)

    assert alpha.seen_scan_id == scan_id
    assert plan.candidate_count == 0
    assert plan.family_failures == []
    assert plan.unused_cash_usd == pytest.approx(250000.0)
