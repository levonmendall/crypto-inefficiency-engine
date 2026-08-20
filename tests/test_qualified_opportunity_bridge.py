from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunityAllocatorService,
    QualifiedOpportunityBridgePublisher,
    QualifiedOpportunityLedger,
    QualifiedOpportunitySnapshot,
)
from inefficiency_engine.unified_allocation import (
    PaperSettlementLeg,
    UnifiedPaperAllocation,
    UnifiedPaperAllocationPlan,
    UnifiedPaperCandidate,
)
from inefficiency_engine.universal_paper_portfolio import (
    UniversalOperationallyResilientPaperPortfolioService,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _alpha_candidate(at: datetime) -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id="alpha:bridge:btc",
        family="alpha",
        strategy="time_series_momentum_v1",
        asset="BTC",
        venues=["Coinbase"],
        capital_required_usd=25_000.0,
        notional_usd_per_leg=25_000.0,
        expected_profit_usd_per_deployment=250.0,
        expected_return_on_reserved_capital=0.01,
        modeled_holding_hours=6.0,
        source_return_metric="forward_ci_health_haircut_net_return",
        source_return_value=0.01,
        exposure_kind="directional_long",
        source_observed_at=at,
        instrument_symbol="BTC-USD",
        instrument_market_kind="spot",
        entry_reference_price=60_000.0,
        modeled_roundtrip_cost_return=0.001,
        conflict_keys=["venue-symbol:Coinbase:BTC-USD"],
    )


def _core_candidate(at: datetime) -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id="core:bridge:funding",
        family="core_cex",
        strategy="funding_dispersion",
        asset="BTC",
        venues=["Bybit", "OKX"],
        capital_required_usd=20_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=100.0,
        expected_return_on_reserved_capital=0.005,
        modeled_holding_hours=1.0,
        source_return_metric="net_annualized_return",
        source_return_value=0.20,
        exposure_kind="market_neutral",
        source_observed_at=at,
        settlement_legs=[
            PaperSettlementLeg(
                venue="Bybit",
                asset="BTC",
                market_kind="perpetual",
                side="long",
                symbol="BTCUSDT",
                base_quantity=100.0,
                entry_price=100.0,
                entry_notional_usd=10_000.0,
                quote_currency="USDT",
                contract_key="continuous",
            ),
            PaperSettlementLeg(
                venue="OKX",
                asset="BTC",
                market_kind="perpetual",
                side="short",
                symbol="BTC-USDT-SWAP",
                base_quantity=100.0,
                entry_price=100.0,
                entry_notional_usd=10_000.0,
                quote_currency="USDT",
                contract_key="continuous",
            ),
        ],
        modeled_non_slippage_cost_bps=10.0,
        modeled_safety_buffer_bps=2.0,
        capital_multiple=2.0,
        conflict_keys=[
            "venue-symbol:Bybit:BTCUSDT",
            "venue-symbol:OKX:BTC-USDT-SWAP",
        ],
    )


@pytest.mark.asyncio
async def test_canonical_allocator_reads_fresh_bridge_without_provider_work(tmp_path):
    at = _now()
    store = EvidenceStore(tmp_path / "qualified-bridge.sqlite3")
    ledger = QualifiedOpportunityLedger(store)
    ledger.record(
        QualifiedOpportunitySnapshot(
            observed_at=at,
            expires_at=at + timedelta(minutes=2),
            source_scan_id="research-scan",
            total_capital_usd=250_000.0,
            candidates=[_alpha_candidate(at), _core_candidate(at)],
        )
    )

    core = SimpleNamespace(settings=Settings())
    alpha = SimpleNamespace(store=store)
    allocator = QualifiedOpportunityAllocatorService(core, None, alpha)  # type: ignore[arg-type]

    plan = await allocator.allocate(total_capital_usd=250_000.0)

    assert plan.candidate_count == 2
    assert {item.family for item in plan.allocations} == {"alpha", "core_cex"}
    assert plan.family_failures == []
    assert plan.allocated_capital_usd == pytest.approx(45_000.0)


@pytest.mark.asyncio
async def test_stale_bridge_fails_closed_to_cash(tmp_path):
    now = _now()
    store = EvidenceStore(tmp_path / "stale-bridge.sqlite3")
    QualifiedOpportunityLedger(store).record(
        QualifiedOpportunitySnapshot(
            observed_at=now - timedelta(minutes=5),
            expires_at=now - timedelta(minutes=1),
            source_scan_id="stale-scan",
            total_capital_usd=250_000.0,
            candidates=[_alpha_candidate(now - timedelta(minutes=5))],
        )
    )
    allocator = QualifiedOpportunityAllocatorService(
        SimpleNamespace(settings=Settings()),  # type: ignore[arg-type]
        None,
        SimpleNamespace(store=store),  # type: ignore[arg-type]
    )

    plan = await allocator.allocate(total_capital_usd=250_000.0)

    assert plan.allocations == []
    assert plan.unused_cash_usd == pytest.approx(250_000.0)
    assert plan.family_failures[0]["error_type"] == "QualifiedOpportunitySnapshotUnavailableOrStale"


@pytest.mark.asyncio
async def test_bridge_publisher_uses_already_persisted_scan(tmp_path):
    now = _now()
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "publisher.sqlite3")
    store.record_scan(
        funding_quotes=[],
        market_quotes=[
            MarketQuote(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                symbol="BTC-USD",
                mid=60_000.0,
                observed_at=now,
                source="test",
            )
        ],
        opportunities=[],
        providers=[],
        started_at=now,
        completed_at=now,
        order_books=[],
        executability=[],
    )

    class NoProviderCore:
        def __init__(self):
            self.settings = settings

        async def collect_live_executability(self):
            raise AssertionError("bridge publisher must reuse persisted research")

    publisher = QualifiedOpportunityBridgePublisher(
        NoProviderCore(),
        store,
        SimpleNamespace(alpha_factory=None),  # type: ignore[arg-type]
    )
    snapshot = await publisher.publish_latest(total_capital_usd=250_000.0)

    assert snapshot is not None
    assert snapshot.candidates == []
    assert QualifiedOpportunityLedger(store).latest_active() is not None


class _FakeCore:
    def __init__(self, settings: Settings, snapshots: list[ScanSnapshot]):
        self.settings = settings
        self.snapshots = list(snapshots)

    async def collect_live_evidence(self) -> ScanSnapshot:
        return self.snapshots.pop(0)


class _FakeAllocator:
    def __init__(self, plans: list[UnifiedPaperAllocationPlan]):
        self.plans = list(plans)

    async def allocate(self, *, total_capital_usd: float):
        return self.plans.pop(0)


def _perp_snapshot(at: datetime, price: float) -> ScanSnapshot:
    return ScanSnapshot(
        scan_id=f"perp-{int(at.timestamp())}",
        started_at=at,
        completed_at=at,
        providers=[],
        funding_quotes=[],
        market_quotes=[
            MarketQuote(
                venue="HlPerp",
                asset="BTC",
                market_kind=MarketKind.PERPETUAL,
                symbol="BTC",
                quote_currency="USD",
                contract_key="continuous",
                mid=price,
                observed_at=at,
                source="test",
            )
        ],
        opportunities=[],
        order_books=[],
        executability=[],
        analysis_config={},
    )


def _short_allocation(at: datetime) -> UnifiedPaperAllocation:
    return UnifiedPaperAllocation(
        candidate_id="alpha:short:btc",
        family="alpha",
        strategy="time_series_momentum_v1",
        asset="BTC",
        venues=["HlPerp"],
        capital_required_usd=10_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=500.0,
        expected_return_on_reserved_capital=0.05,
        modeled_holding_hours=1.0,
        source_return_metric="forward_ci_health_haircut_net_return",
        source_return_value=0.05,
        exposure_kind="directional_short",
        source_observed_at=at,
        instrument_symbol="BTC",
        instrument_market_kind="perpetual",
        entry_reference_price=100.0,
        modeled_roundtrip_cost_return=0.001,
    )


def _plan(at: datetime, allocations: list[UnifiedPaperAllocation], total: float) -> UnifiedPaperAllocationPlan:
    allocated = sum(item.capital_required_usd for item in allocations)
    return UnifiedPaperAllocationPlan(
        observed_at=at,
        total_capital_usd=total,
        allocated_capital_usd=allocated,
        unused_cash_usd=max(0.0, total - allocated),
        expected_profit_usd_current_deployments=sum(
            item.expected_profit_usd_per_deployment for item in allocations
        ),
        weighted_expected_return_on_reserved_capital=0.0,
        candidate_count=len(allocations),
        allocations=allocations,
    )


@pytest.mark.asyncio
async def test_canonical_portfolio_can_compound_settlement_supported_perp_short(tmp_path):
    t0 = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    funding_at = t0 + timedelta(minutes=30)
    t2 = t0 + timedelta(hours=2)
    settings = Settings()
    store = EvidenceStore(tmp_path / "short-portfolio.sqlite3")

    # Exact funding schedule and a point-in-time mid at the event make the short
    # settlement independently reconstructible. A zero funding rate is deliberate.
    store.record_scan(
        funding_quotes=[
            FundingQuote(
                venue="HlPerp",
                asset="BTC",
                symbol="BTC",
                quote_currency="USD",
                contract_key="continuous",
                rate=0.0,
                interval_hours=1.0,
                next_funding_time=funding_at,
                observed_at=t0 + timedelta(minutes=15),
                source="test",
            )
        ],
        market_quotes=[
            MarketQuote(
                venue="HlPerp",
                asset="BTC",
                market_kind=MarketKind.PERPETUAL,
                symbol="BTC",
                quote_currency="USD",
                contract_key="continuous",
                mid=99.0,
                observed_at=funding_at,
                source="test",
            )
        ],
        opportunities=[],
        providers=[],
        started_at=funding_at,
        completed_at=funding_at,
    )

    allocator = _FakeAllocator(
        [
            _plan(t0, [_short_allocation(t0)], 250_000.0),
            _plan(t2, [], 250_990.0),
        ]
    )
    portfolio = UniversalOperationallyResilientPaperPortfolioService(
        _FakeCore(settings, [_perp_snapshot(t0, 100.0), _perp_snapshot(t2, 90.0)]),
        allocator,  # type: ignore[arg-type]
        store,
    )

    first = await portfolio.run_cycle()
    assert first.opened_position_count == 1
    opened = portfolio.ledger.latest_snapshot()
    assert opened is not None
    assert opened.open_position_count == 1
    assert opened.cash_usd == pytest.approx(240_000.0)

    second = await portfolio.run_cycle()
    closed = portfolio.ledger.latest_snapshot()
    assert second.closed_position_count == 1
    assert closed is not None
    assert closed.open_position_count == 0
    assert closed.cash_usd == pytest.approx(250_990.0)
    assert closed.realized_pnl_usd == pytest.approx(990.0)
    assert closed.pnl_by_mechanism_usd["alpha"] == pytest.approx(990.0)
