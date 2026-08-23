from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.canonical_control_plane_runtime import (
    refresh_canonical_control_plane,
)
from inefficiency_engine.dashboard_projection import DashboardProjectionLedger
from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.lightweight_portfolio_worker import (
    CanonicalPaperPortfolioService,
    CanonicalPortfolioAllocatorService,
    _DurableQualifiedStateHandle,
)
from inefficiency_engine.models import (
    CapitalTierQualification,
    LegExecutionEstimate,
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityExecutability,
    OpportunityLeg,
    OrderBookLevel,
    OrderBookSnapshot,
    Side,
    Strategy,
    TradeSide,
)
from inefficiency_engine.operating_certification import (
    MechanismOperatingStatus,
    OperatingCertificationSnapshot,
)
from inefficiency_engine.permanent_control_worker import _build_control_services
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunityLedger,
    QualifiedOpportunitySnapshot,
)
from inefficiency_engine.production_dashboard_fastpath import (
    build_production_dashboard_snapshot,
)
from inefficiency_engine.service import OpportunityService


def test_combined_entrypoint_bootstraps_schema_before_starting_children():
    from inefficiency_engine import render_combined

    source = inspect.getsource(render_combined.main)
    assert source.index("bootstrap_permanent_runtime_schema") < source.index(
        "control_guard.start"
    )


def _quote(
    venue: str,
    symbol: str,
    *,
    mid: float,
    observed_at: datetime,
) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol=symbol,
        quote_currency="USD",
        contract_key="spot",
        bid=mid - 0.05,
        ask=mid + 0.05,
        mid=mid,
        observed_at=observed_at,
        source="seeded-permanent-source",
    )


def _book(
    venue: str,
    symbol: str,
    *,
    bid: float,
    ask: float,
    observed_at: datetime,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol=symbol,
        quote_currency="USD",
        contract_key="spot",
        bids=[OrderBookLevel(price=bid, size=500.0)],
        asks=[OrderBookLevel(price=ask, size=500.0)],
        observed_at=observed_at,
        source="seeded-permanent-source",
    )


def _opportunity(
    observed_at: datetime,
    *,
    passes_return_hurdle: bool,
) -> tuple[Opportunity, OpportunityExecutability]:
    opportunity = Opportunity(
        id=f"seeded-{'qualifying' if passes_return_hurdle else 'rejected'}-cex",
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
                reference_price=100.0,
            ),
            OpportunityLeg(
                venue="Kraken",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                side=Side.SHORT,
                symbol="BTC/USD",
                quote_currency="USD",
                contract_key="spot",
                reference_price=102.0,
            ),
        ],
        gross_edge_bps_per_hour=30.0,
        modeled_cost_bps=10.0,
        holding_hours=1.0 / 3600.0,
        safety_buffer_bps_per_hour=2.0,
        net_edge_bps_per_hour=18.0,
        net_annualized_return=0.20,
        observed_at=observed_at,
        expires_at=observed_at + timedelta(minutes=5),
        confidence="high",
    )
    tier = CapitalTierQualification(
        opportunity_id=opportunity.id,
        notional_usd_per_leg=10_000.0,
        executable=True,
        passes_return_hurdle=passes_return_hurdle,
        gross_edge_bps_per_hour=30.0,
        static_modeled_cost_bps=10.0,
        total_modeled_cost_bps=12.0,
        net_edge_bps_per_hour=18.0,
        net_annualized_return=0.20,
        capital_required_usd=20_000.0,
        capital_multiple=2.0,
        observed_entry_slippage_bps=1.0,
        assumed_exit_slippage_bps=1.0,
        leg_estimates=[
            LegExecutionEstimate(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                trade_side=TradeSide.BUY,
                symbol="BTC-USD",
                requested_base_quantity=100.0,
                filled_base_quantity=100.0,
                filled_notional_usd=10_000.0,
                average_price=100.0,
                best_price=99.99,
                slippage_bps=1.0,
                levels_consumed=1,
            ),
            LegExecutionEstimate(
                venue="Kraken",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                trade_side=TradeSide.SELL,
                symbol="BTC/USD",
                requested_base_quantity=100.0,
                filled_base_quantity=100.0,
                filled_notional_usd=10_200.0,
                average_price=102.0,
                best_price=102.01,
                slippage_bps=1.0,
                levels_consumed=1,
            ),
        ],
    )
    return opportunity, OpportunityExecutability(
        opportunity_id=opportunity.id,
        strategy=opportunity.strategy,
        asset=opportunity.asset,
        observed_at=observed_at,
        tiers=[tier],
    )


def _record_source_scan(
    store: EvidenceStore,
    *,
    observed_at: datetime,
    opportunity: Opportunity | None = None,
    executability: OpportunityExecutability | None = None,
    exit_books: bool = False,
) -> str:
    return store.record_scan(
        funding_quotes=[],
        market_quotes=[
            _quote("Coinbase", "BTC-USD", mid=101.0 if exit_books else 100.0, observed_at=observed_at),
            _quote("Kraken", "BTC/USD", mid=101.0 if exit_books else 102.0, observed_at=observed_at),
        ],
        opportunities=[opportunity] if opportunity is not None else [],
        providers=[],
        started_at=observed_at,
        completed_at=observed_at,
        analysis_config={
            "permanent_source_plane": True,
            "executable_hot_path": True,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
        order_books=[
            _book(
                "Coinbase",
                "BTC-USD",
                bid=101.0 if exit_books else 99.99,
                ask=101.1 if exit_books else 100.0,
                observed_at=observed_at,
            ),
            _book(
                "Kraken",
                "BTC/USD",
                bid=100.9 if exit_books else 102.0,
                ask=101.0 if exit_books else 102.01,
                observed_at=observed_at,
            ),
        ],
        executability=[executability] if executability is not None else [],
    )


class _ForbiddenProviderRegistry:
    order_book_timeout_seconds = 0.01

    async def collect_inputs(self):
        raise AssertionError(
            "canonical portfolio must consume the persisted permanent-source snapshot"
        )

    def book_request(self, leg):
        raise AssertionError(
            "canonical settlement must consume persisted permanent-source L2"
        )


@pytest.mark.asyncio
async def test_seeded_production_pipeline_allocates_and_settles_without_provider_calls(tmp_path):
    """Permanent source -> bridge -> allocation -> trial -> settlement -> dashboard."""

    entry_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    exit_at = datetime.now(timezone.utc)
    settings = Settings(max_quote_age_seconds=120.0, max_order_book_age_seconds=30.0)
    database_path = tmp_path / "production-e2e.sqlite3"
    source_store = EvidenceStore(database_path)
    control_store = EvidenceStore(database_path)
    portfolio_store = EvidenceStore(database_path)
    opportunity, executability = _opportunity(
        entry_at,
        passes_return_hurdle=True,
    )
    source_scan_id = _record_source_scan(
        source_store,
        observed_at=entry_at,
        opportunity=opportunity,
        executability=executability,
    )

    operating, bridge, research_projection = _build_control_services(
        settings,
        control_store,
    )
    bridge.core.adapter_registry = _ForbiddenProviderRegistry()  # type: ignore[assignment]
    baseline = OperatingCertificationSnapshot(
        observed_at=entry_at,
        version="seeded-production-invariant",
        public_market_provider_healthy=True,
        public_market_surface_count=2,
        public_market_surface_ok_count=2,
        public_order_book_probe_count=2,
        public_order_book_probe_ok_count=2,
        market_quote_count=2,
        funding_quote_count=0,
        mechanism_count=1,
        provider_gap_count=0,
        collecting_count=1,
        poor_economics_count=0,
        blocked_count=0,
        certifying_count=0,
        certified_count=0,
        mechanisms=[
            MechanismOperatingStatus(
                mechanism_id="price_discrepancy",
                name="Price discrepancy",
                state="collecting",
                stage="profitability_certifiable",
                provider_ready=True,
                primary_reason="seeded operating baseline",
                next_action="settle the seeded paper trial",
            )
        ],
    )
    operating.ledger.record(baseline)
    qualified = await bridge.publish_latest(total_capital_usd=250_000.0)
    assert qualified is not None
    assert qualified.source_scan_id == source_scan_id
    assert len(qualified.candidates) == 1

    service = OpportunityService(settings=settings, evidence_store=portfolio_store)
    service.adapter_registry = _ForbiddenProviderRegistry()  # type: ignore[assignment]
    allocator = CanonicalPortfolioAllocatorService(
        service,
        None,
        _DurableQualifiedStateHandle(portfolio_store),
    )
    plan = allocator.allocate_sync(total_capital_usd=250_000.0)
    assert plan.allocated_capital_usd == pytest.approx(20_000.0)

    portfolio = CanonicalPaperPortfolioService(service, allocator, portfolio_store)
    opened = await portfolio.run_cycle()
    assert opened.opened_position_count == 1
    assert portfolio.ledger.latest_snapshot().reserved_capital_usd == pytest.approx(20_000.0)
    assert len(portfolio.settlement.ledger.trials_all()) == 1
    older_projection = DashboardProjectionLedger(portfolio_store).publish()
    assert older_projection["performance"]["closed_trade_count"] == 0

    exit_scan_id = _record_source_scan(
        source_store,
        observed_at=exit_at,
        exit_books=True,
    )
    QualifiedOpportunityLedger(control_store).record(
        QualifiedOpportunitySnapshot(
            observed_at=exit_at,
            expires_at=exit_at + timedelta(minutes=10),
            source_scan_id=exit_scan_id,
            total_capital_usd=250_000.0,
            candidates=[],
        )
    )
    settled = await portfolio.run_cycle()
    assert settled.closed_position_count == 1
    final = portfolio.ledger.latest_snapshot()
    assert final.open_position_count == 0
    assert final.closed_trade_count == 1
    assert final.realized_pnl_usd == pytest.approx(190.0)
    outcomes = portfolio.settlement.ledger.outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].settlement_evidence_complete is True

    control = await refresh_canonical_control_plane(
        store=control_store,
        operating_certification=operating,
        qualified_bridge=bridge,
        research_projection=research_projection,
        settings=settings,
    )
    assert control["operating_reconciliation_complete"] is True
    assert control["qualified_bridge_publication_complete"] is True
    assert control["research_projection_publication_complete"] is True
    reconciled = operating.ledger.latest()
    assert reconciled is not None
    assert reconciled.observed_at > baseline.observed_at
    price_discrepancy = next(
        item
        for item in reconciled.mechanisms
        if item.mechanism_id == "price_discrepancy"
    )
    assert price_discrepancy.settled_allocator_outcome_count == 1

    projection = DashboardProjectionLedger(portfolio_store).publish()
    projected_portfolio_at = datetime.fromisoformat(
        projection["source_portfolio_observed_at"].replace("Z", "+00:00")
    )
    assert projected_portfolio_at == final.observed_at
    assert projection["performance"]["closed_trade_count"] == 1
    assert projection["performance"]["realized_pnl_usd"] == pytest.approx(190.0)
    api_store = EvidenceStore(database_path)
    assert {
        source_store.safe_database_url,
        control_store.safe_database_url,
        portfolio_store.safe_database_url,
        api_store.safe_database_url,
    } == {source_store.safe_database_url}
    api_snapshot = build_production_dashboard_snapshot(api_store)
    assert api_snapshot["performance"]["closed_trade_count"] == 1
    assert api_snapshot["performance"]["realized_pnl_usd"] == pytest.approx(190.0)


@pytest.mark.asyncio
async def test_nonqualifying_source_is_an_explicit_economic_rejection(tmp_path):
    observed_at = datetime.now(timezone.utc)
    store = EvidenceStore(tmp_path / "explicit-rejection.sqlite3")
    opportunity, executability = _opportunity(
        observed_at,
        passes_return_hurdle=False,
    )
    source_scan_id = _record_source_scan(
        store,
        observed_at=observed_at,
        opportunity=opportunity,
        executability=executability,
    )
    service = OpportunityService(
        settings=Settings(max_quote_age_seconds=120.0),
        evidence_store=store,
    )
    service.adapter_registry = _ForbiddenProviderRegistry()  # type: ignore[assignment]
    bridge = DurableControlQualifiedOpportunityBridgePublisher(
        service,
        store,
        SimpleNamespace(alpha_factory=None),  # type: ignore[arg-type]
    )

    rejected = await bridge.publish_latest(total_capital_usd=250_000.0)

    assert rejected is not None
    assert rejected.source_scan_id == source_scan_id
    assert rejected.candidates == []
    assert rejected.family_failures == [
        {
            "family": "core_cex",
            "error_type": "EconomicQualificationRejected",
            "reason": "executable core CEX evidence failed the unchanged return hurdle",
            "opportunity_id": opportunity.id,
            "executable_tier_count": 1,
            "return_hurdle_pass_count": 0,
        }
    ]
