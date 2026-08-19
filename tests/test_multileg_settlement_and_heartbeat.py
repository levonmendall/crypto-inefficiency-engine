from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.alpha_factory import AlphaEvidenceLedger
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import (
    CapitalTierQualification,
    FundingQuote,
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
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.unified_allocation import (
    PaperSettlementLeg,
    UnifiedPaperAllocation,
    _core_candidates,
)


NOW = datetime(2026, 8, 19, 20, 0, tzinfo=timezone.utc)


def book(venue: str, kind: MarketKind, symbol: str, *, bid: float, ask: float, observed_at: datetime):
    return OrderBookSnapshot(
        venue=venue,
        asset="BTC",
        market_kind=kind,
        symbol=symbol,
        bids=[OrderBookLevel(price=bid, size=500.0)],
        asks=[OrderBookLevel(price=ask, size=500.0)],
        observed_at=observed_at,
        source="test",
    )


def test_core_candidates_preserve_exact_two_leg_entry_metadata():
    opportunity = Opportunity(
        id="op-1",
        strategy=Strategy.CEX_SPOT_DISLOCATION,
        asset="BTC",
        legs=[
            OpportunityLeg(
                venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT,
                side=Side.LONG, symbol="BTC-USD",
            ),
            OpportunityLeg(
                venue="Kraken", asset="BTC", market_kind=MarketKind.SPOT,
                side=Side.SHORT, symbol="BTC/USD",
            ),
        ],
        gross_edge_bps_per_hour=30.0,
        modeled_cost_bps=10.0,
        holding_hours=1.0,
        safety_buffer_bps_per_hour=2.0,
        net_edge_bps_per_hour=18.0,
        net_annualized_return=0.20,
        observed_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    tier = CapitalTierQualification(
        opportunity_id=opportunity.id,
        notional_usd_per_leg=10000.0,
        executable=True,
        passes_return_hurdle=True,
        gross_edge_bps_per_hour=30.0,
        static_modeled_cost_bps=10.0,
        total_modeled_cost_bps=12.0,
        net_edge_bps_per_hour=18.0,
        net_annualized_return=0.20,
        capital_required_usd=20000.0,
        capital_multiple=2.0,
        observed_entry_slippage_bps=1.0,
        assumed_exit_slippage_bps=1.0,
        leg_estimates=[
            LegExecutionEstimate(
                venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT,
                trade_side=TradeSide.BUY, symbol="BTC-USD",
                requested_base_quantity=100.0, filled_base_quantity=100.0,
                filled_notional_usd=10000.0, average_price=100.0, best_price=99.99,
                slippage_bps=1.0, levels_consumed=1,
            ),
            LegExecutionEstimate(
                venue="Kraken", asset="BTC", market_kind=MarketKind.SPOT,
                trade_side=TradeSide.SELL, symbol="BTC/USD",
                requested_base_quantity=100.0, filled_base_quantity=100.0,
                filled_notional_usd=10200.0, average_price=102.0, best_price=102.01,
                slippage_bps=1.0, levels_consumed=1,
            ),
        ],
    )
    execution = OpportunityExecutability(
        opportunity_id=opportunity.id,
        strategy=opportunity.strategy,
        asset="BTC",
        observed_at=NOW,
        tiers=[tier],
    )

    candidate = _core_candidates([opportunity], [execution])[0]
    assert len(candidate.settlement_legs) == 2
    assert candidate.settlement_legs[0].entry_price == 100.0
    assert candidate.settlement_legs[1].side == "short"
    assert candidate.modeled_non_slippage_cost_bps == pytest.approx(10.0)
    assert candidate.modeled_safety_buffer_bps == pytest.approx(2.0)
    assert candidate.capital_multiple == pytest.approx(2.0)


def multileg_allocation(*, strategy: str, second_kind: str = "spot") -> UnifiedPaperAllocation:
    return UnifiedPaperAllocation(
        candidate_id=f"core:{strategy}",
        family="core_cex",
        strategy=strategy,
        asset="BTC",
        venues=["Coinbase", "Kraken"],
        capital_required_usd=20000.0,
        notional_usd_per_leg=10000.0,
        expected_profit_usd_per_deployment=100.0,
        expected_return_on_reserved_capital=0.005,
        modeled_holding_hours=1.0 if second_kind == "spot" else 8.0,
        source_return_metric="net_annualized_return",
        source_return_value=0.20,
        exposure_kind="market_neutral",
        source_observed_at=NOW,
        settlement_legs=[
            PaperSettlementLeg(
                venue="Coinbase", asset="BTC", market_kind="spot", side="long",
                symbol="BTC-USD", base_quantity=100.0, entry_price=100.0,
                entry_notional_usd=10000.0,
            ),
            PaperSettlementLeg(
                venue="Kraken", asset="BTC", market_kind=second_kind, side="short",
                symbol="BTC/USD" if second_kind == "spot" else "PF_XBTUSD",
                base_quantity=100.0, entry_price=102.0 if second_kind == "spot" else 100.0,
                entry_notional_usd=10200.0 if second_kind == "spot" else 10000.0,
            ),
        ],
        modeled_non_slippage_cost_bps=10.0 if second_kind == "spot" else 0.0,
        modeled_safety_buffer_bps=2.0,
        capital_multiple=2.0,
    )


def test_shared_multileg_engine_settles_both_legs_from_visible_l2(tmp_path):
    store = EvidenceStore(tmp_path / "settlement.sqlite3")
    service = AllocationForwardCertificationService(
        SimpleNamespace(), SimpleNamespace(), store  # type: ignore[arg-type]
    )
    allocation = multileg_allocation(strategy=Strategy.CEX_SPOT_DISLOCATION.value)
    trial = service.trial_from_allocation(allocation, plan_observed_at=NOW)
    assert trial.settlement_supported is True
    assert trial.settlement_method == service.MULTI_LEG_SETTLEMENT_METHOD

    due = NOW + timedelta(hours=1)
    snapshot = ScanSnapshot(
        scan_id="exit",
        started_at=due,
        completed_at=due,
        providers=[],
        funding_quotes=[],
        market_quotes=[],
        order_books=[
            book("Coinbase", MarketKind.SPOT, "BTC-USD", bid=101.0, ask=101.1, observed_at=due),
            book("Kraken", MarketKind.SPOT, "BTC/USD", bid=100.9, ask=101.0, observed_at=due),
        ],
        opportunities=[],
    )
    outcome = service._settle_trial(trial, snapshot)
    assert outcome is not None
    assert len(outcome.leg_outcomes) == 2
    assert outcome.realized_price_pnl_usd == pytest.approx(200.0)
    assert outcome.modeled_non_slippage_cost_usd == pytest.approx(10.0)
    assert outcome.realized_profit_usd == pytest.approx(190.0)
    assert outcome.realized_gross_return == pytest.approx(0.01)
    assert outcome.realized_net_return == pytest.approx(0.0095)
    assert outcome.settlement_evidence_complete is True


def test_multileg_carry_accrues_observed_funding_event(tmp_path):
    store = EvidenceStore(tmp_path / "funding.sqlite3")
    service = AllocationForwardCertificationService(
        SimpleNamespace(), SimpleNamespace(), store  # type: ignore[arg-type]
    )
    allocation = multileg_allocation(strategy=Strategy.SPOT_PERP_BASIS.value, second_kind="perpetual")
    trial = service.trial_from_allocation(allocation, plan_observed_at=NOW)
    assert trial.settlement_supported is True

    event_at = NOW + timedelta(hours=4)
    evidence_at = event_at - timedelta(minutes=1)
    store.record_scan(
        scan_id="funding-evidence",
        started_at=evidence_at,
        completed_at=evidence_at,
        providers=[],
        opportunities=[],
        funding_quotes=[FundingQuote(
            venue="Kraken",
            asset="BTC",
            rate=0.001,
            interval_hours=8.0,
            symbol="PF_XBTUSD",
            next_funding_time=event_at,
            observed_at=evidence_at,
            source="test",
        )],
        market_quotes=[MarketQuote(
            venue="Kraken",
            asset="BTC",
            market_kind=MarketKind.PERPETUAL,
            symbol="PF_XBTUSD",
            mid=100.0,
            observed_at=evidence_at,
            source="test",
        )],
    )

    due = NOW + timedelta(hours=8)
    snapshot = ScanSnapshot(
        scan_id="carry-exit",
        started_at=due,
        completed_at=due,
        providers=[],
        funding_quotes=[],
        market_quotes=[],
        order_books=[
            book("Coinbase", MarketKind.SPOT, "BTC-USD", bid=100.0, ask=100.1, observed_at=due),
            book("Kraken", MarketKind.PERPETUAL, "PF_XBTUSD", bid=99.9, ask=100.0, observed_at=due),
        ],
        opportunities=[],
    )
    outcome = service._settle_trial(trial, snapshot)
    assert outcome is not None
    assert outcome.realized_price_pnl_usd == pytest.approx(0.0)
    assert outcome.realized_funding_pnl_usd == pytest.approx(10.0)
    assert outcome.realized_profit_usd == pytest.approx(10.0)
    assert outcome.leg_outcomes[1].funding_event_count == 1


def test_forward_evidence_heartbeat_exposes_worker_persistence_and_next_expected_cycle(tmp_path):
    store = EvidenceStore(tmp_path / "heartbeat.sqlite3")
    ledger = AlphaEvidenceLedger(store)
    store.record_worker_heartbeat(
        worker_id="shadow-test",
        state="success",
        observed_at=NOW,
        detail={"alpha_forward_evidence_cycle_id": "alpha-cycle"},
    )
    service = OperatingCertificationService.__new__(OperatingCertificationService)
    service.store = store
    service.alpha_factory = SimpleNamespace(ledger=ledger)
    service.core = SimpleNamespace(settings=Settings(
        alpha_evidence_every_cycles=10,
        shadow_cycle_interval_seconds=30.0,
        worker_heartbeat_stale_seconds=180.0,
    ))

    heartbeat = service._forward_evidence_heartbeat("directional_time_series", now=NOW)
    assert heartbeat["worker_healthy"] is True
    assert heartbeat["persistence_healthy"] is True
    assert heartbeat["expected_interval_seconds"] == pytest.approx(300.0)
    assert heartbeat["next_expected_at"] == NOW + timedelta(seconds=300)
    assert heartbeat["last_signal_at"] is None
    assert "next expected" in service._heartbeat_note(heartbeat)
