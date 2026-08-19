from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CanonicalPaperPortfolioLedger,
    CanonicalPaperPortfolioService,
)
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperAllocationPlan


class FakeCore:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)

    async def collect_live_executability(self):
        return self.snapshots.pop(0)


class FakeAllocator:
    def __init__(self, plans):
        self.plans = list(plans)
        self.capital_requests = []

    async def allocate(self, *, total_capital_usd: float):
        self.capital_requests.append(total_capital_usd)
        return self.plans.pop(0)


def _snapshot(at: datetime, price: float) -> ScanSnapshot:
    quote = MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        mid=price,
        observed_at=at,
        source="test",
    )
    return ScanSnapshot(
        scan_id=f"scan-{int(at.timestamp())}",
        started_at=at,
        completed_at=at,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote],
        opportunities=[],
        order_books=[],
        executability=[],
        analysis_config={},
    )


def _allocation(at: datetime) -> UnifiedPaperAllocation:
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


def _plan(at: datetime, allocations) -> UnifiedPaperAllocationPlan:
    allocated = sum(item.capital_required_usd for item in allocations)
    return UnifiedPaperAllocationPlan(
        observed_at=at,
        total_capital_usd=CANONICAL_INITIAL_CAPITAL_USD,
        allocated_capital_usd=allocated,
        unused_cash_usd=CANONICAL_INITIAL_CAPITAL_USD - allocated,
        expected_profit_usd_current_deployments=sum(item.expected_profit_usd_per_deployment for item in allocations),
        weighted_expected_return_on_reserved_capital=0.0,
        candidate_count=len(allocations),
        allocations=list(allocations),
        skipped=[],
        authorizes_execution=False,
        live_execution_eligible=False,
        paper_only=True,
    )


def test_genesis_is_exactly_250k_and_is_not_reset(tmp_path):
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    ledger = CanonicalPaperPortfolioLedger(store)
    first = ledger.ensure_genesis()
    second = ledger.ensure_genesis()

    assert first.event_id == second.event_id
    assert len(ledger.events_all()) == 1
    state = ledger.current_state()
    assert state.initial_capital_usd == 250_000.0
    assert state.cash_usd == 250_000.0
    assert state.nav_usd == 250_000.0
    assert state.total_return == 0.0


@pytest.mark.asyncio
async def test_supported_spot_alpha_position_compounds_back_into_cash(tmp_path):
    t0 = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    t2 = t0 + timedelta(hours=2)
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    allocator = FakeAllocator([
        _plan(t0, [_allocation(t0)]),
        _plan(t2, []),
    ])
    service = CanonicalPaperPortfolioService(
        FakeCore([_snapshot(t0, 100.0), _snapshot(t2, 110.0)]),
        allocator,
        store,
    )

    first = await service.run_cycle()
    opened = service.ledger.latest_snapshot()
    assert first.opened_position_count == 1
    assert opened is not None
    assert opened.cash_usd == 240_000.0
    assert opened.reserved_capital_usd == 10_000.0
    assert opened.unrealized_pnl_usd == -10.0
    assert opened.nav_usd == 249_990.0
    assert opened.open_position_count == 1

    second = await service.run_cycle()
    closed = service.ledger.latest_snapshot()
    assert second.closed_position_count == 1
    assert closed is not None
    assert closed.open_position_count == 0
    assert closed.closed_trade_count == 1
    assert closed.realized_pnl_usd == pytest.approx(990.0)
    assert closed.cumulative_modeled_cost_usd == pytest.approx(10.0)
    assert closed.cash_usd == pytest.approx(250_990.0)
    assert closed.nav_usd == pytest.approx(250_990.0)
    assert closed.total_return == pytest.approx(990.0 / 250_000.0)
    assert closed.pnl_by_mechanism_usd["alpha"] == pytest.approx(990.0)
    assert closed.pnl_by_strategy_usd["time_series_momentum_v1"] == pytest.approx(990.0)
    assert len(service.ledger.trade_history()) == 1


def test_unsupported_multi_leg_allocation_stays_fail_closed():
    allocation = UnifiedPaperAllocation(
        candidate_id="core:test",
        family="core_cex",
        strategy="funding_dispersion",
        asset="BTC",
        venues=["Bybit", "OKX"],
        capital_required_usd=20_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=50.0,
        expected_return_on_reserved_capital=0.0025,
        source_return_metric="net_annualized_return",
        source_return_value=0.15,
        exposure_kind="market_neutral",
        capacity_claimed=False,
        authorizes_execution=False,
        paper_only=True,
    )
    supported, reason = CanonicalPaperPortfolioService._support_reason(allocation)
    assert supported is False
    assert "multi-leg structural" in (reason or "")
