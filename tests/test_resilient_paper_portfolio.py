from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.canonical_paper_portfolio import CANONICAL_INITIAL_CAPITAL_USD
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperAllocationPlan


class FakeCore:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)

    async def collect_live_executability(self):
        next_item = self.snapshots.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class FakeAllocator:
    def __init__(self, plans):
        self.plans = list(plans)
        self.capital_requests: list[float] = []

    async def allocate(self, *, total_capital_usd: float):
        self.capital_requests.append(total_capital_usd)
        next_item = self.plans.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


def snapshot(
    at: datetime,
    price: float = 100.0,
    *,
    include_quote: bool = True,
    quote_at: datetime | None = None,
) -> ScanSnapshot:
    quotes = []
    if include_quote:
        quotes.append(MarketQuote(
            venue="Coinbase",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-USD",
            mid=price,
            observed_at=quote_at or at,
            source="test",
        ))
    return ScanSnapshot(
        scan_id=f"scan-{int(at.timestamp())}",
        started_at=at,
        completed_at=at,
        providers=[],
        funding_quotes=[],
        market_quotes=quotes,
        opportunities=[],
        order_books=[],
        executability=[],
        analysis_config={},
    )


def allocation(at: datetime) -> UnifiedPaperAllocation:
    return UnifiedPaperAllocation(
        candidate_id="alpha:test:btc",
        family="alpha",
        strategy="time_series_momentum_v1",
        asset="BTC",
        venues=["Coinbase"],
        capital_required_usd=10_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=100.0,
        expected_return_on_reserved_capital=0.01,
        modeled_holding_hours=1.0,
        source_return_metric="forward_ci_health_haircut_net_return",
        source_return_value=0.01,
        exposure_kind="directional_long",
        source_observed_at=at,
        instrument_symbol="BTC-USD",
        instrument_market_kind="spot",
        entry_reference_price=100.0,
        modeled_roundtrip_cost_return=0.001,
        capacity_claimed=False,
        authorizes_execution=False,
        paper_only=True,
    )


def plan(at: datetime, allocations, *, family_failures=None) -> UnifiedPaperAllocationPlan:
    allocated = sum(item.capital_required_usd for item in allocations)
    return UnifiedPaperAllocationPlan(
        observed_at=at,
        total_capital_usd=CANONICAL_INITIAL_CAPITAL_USD,
        allocated_capital_usd=allocated,
        unused_cash_usd=CANONICAL_INITIAL_CAPITAL_USD - allocated,
        expected_profit_usd_current_deployments=sum(
            item.expected_profit_usd_per_deployment for item in allocations
        ),
        weighted_expected_return_on_reserved_capital=0.0,
        candidate_count=len(allocations),
        allocations=list(allocations),
        skipped=[],
        family_failures=list(family_failures or []),
        authorizes_execution=False,
        live_execution_eligible=False,
        paper_only=True,
    )


@pytest.mark.asyncio
async def test_healthy_cycle_records_fresh_integrity_and_opens_supported_position(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    service = OperationallyResilientPaperPortfolioService(
        FakeCore([snapshot(t0)]),
        FakeAllocator([plan(t0, [allocation(t0)])]),
        store,
    )

    cycle = await service.run_cycle()
    account = service.ledger.latest_snapshot()
    integrity = service.integrity.latest()

    assert cycle.opened_position_count == 1
    assert account is not None and account.open_position_count == 1
    assert integrity is not None
    assert integrity.valuation_status == "fresh"
    assert integrity.cycle_status == "success"
    assert integrity.market_evidence_at == t0
    assert integrity.stale_position_count == 0
    assert integrity.settlement_evidence_blocked_count == 0
    assert integrity.allocation_family_failures == []


@pytest.mark.asyncio
async def test_failed_family_does_not_block_surviving_supported_allocation(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    failures = [{
        "family": "cex_dex",
        "error_type": "ConnectionError",
        "reason": "CEX↔DEX candidate family failed closed",
    }]
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    service = OperationallyResilientPaperPortfolioService(
        FakeCore([snapshot(t0)]),
        FakeAllocator([plan(t0, [allocation(t0)], family_failures=failures)]),
        store,
    )

    cycle = await service.run_cycle()
    integrity = service.integrity.latest()

    assert cycle.opened_position_count == 1
    assert integrity is not None
    assert integrity.valuation_status == "fresh"
    assert integrity.cycle_status == "degraded"
    assert integrity.allocation_family_failures == failures


@pytest.mark.asyncio
async def test_stale_open_position_blocks_new_deployment_and_is_explicit(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=30)
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    allocator = FakeAllocator([plan(t0, [allocation(t0)])])
    service = OperationallyResilientPaperPortfolioService(
        FakeCore([snapshot(t0), snapshot(t1, include_quote=False)]),
        allocator,
        store,
    )

    await service.run_cycle()
    await service.run_cycle()
    account = service.ledger.latest_snapshot()
    integrity = service.integrity.latest()

    assert len(allocator.capital_requests) == 1
    assert account is not None and account.open_position_count == 1
    assert integrity is not None
    assert integrity.valuation_status == "stale"
    assert integrity.cycle_status == "degraded"
    assert integrity.cycle_error_type == "StaleOpenPositionValuation"
    assert integrity.stale_position_count == 1
    assert integrity.settlement_evidence_blocked_count == 0


@pytest.mark.asyncio
async def test_scan_completion_after_horizon_cannot_settle_with_pre_horizon_quote(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    due = t0 + timedelta(hours=1)
    scan_completed = due + timedelta(minutes=5)
    pre_horizon_quote = due - timedelta(seconds=1)
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    allocator = FakeAllocator([plan(t0, [allocation(t0)])])
    service = OperationallyResilientPaperPortfolioService(
        FakeCore([
            snapshot(t0, price=100.0),
            snapshot(scan_completed, price=110.0, quote_at=pre_horizon_quote),
        ]),
        allocator,
        store,
    )

    await service.run_cycle()
    second = await service.run_cycle()
    account = service.ledger.latest_snapshot()
    integrity = service.integrity.latest()

    assert len(allocator.capital_requests) == 1
    assert second.closed_position_count == 0
    assert account is not None and account.open_position_count == 1
    assert account.closed_trade_count == 0
    assert service.ledger.trade_history() == []
    assert integrity is not None
    assert integrity.valuation_status == "stale"
    assert integrity.cycle_status == "degraded"
    assert integrity.cycle_error_type == "SettlementEvidencePreHorizon"
    assert integrity.stale_position_count == 1
    assert integrity.settlement_evidence_blocked_count == 1
    assert integrity.market_evidence_at == t0


@pytest.mark.asyncio
async def test_allocator_exception_preserves_fresh_cash_account_snapshot(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    service = OperationallyResilientPaperPortfolioService(
        FakeCore([snapshot(t0)]),
        FakeAllocator([RuntimeError("allocator unavailable")]),
        store,
    )

    await service.run_cycle()
    account = service.ledger.latest_snapshot()
    integrity = service.integrity.latest()

    assert account is not None
    assert account.nav_usd == 250000.0
    assert account.observed_at == t0
    assert integrity is not None
    assert integrity.valuation_status == "cash_only"
    assert integrity.cycle_status == "degraded"
    assert integrity.cycle_error_type == "RuntimeError"
