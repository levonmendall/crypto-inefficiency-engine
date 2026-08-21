from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.executable_lane_runtime import (
    AllLaneAllocationForwardCertificationService,
    ExecutableMechanismExecutionService,
)
from inefficiency_engine.mechanism_execution import (
    MechanismForwardTrial,
    MechanismPaperCandidate,
)
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation
from inefficiency_engine.trade_flow import TradeFlowImbalanceStrategy, TradeFlowLedger
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation


NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def core():
    return SimpleNamespace(settings=Settings())


def market_quote(at, price=60_000.0):
    return MarketQuote(
        venue="Coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        mid=price,
        bid=price - 5,
        ask=price + 5,
        observed_at=at,
        source="test",
    )


def test_public_trade_flow_generates_real_forward_testable_alpha_without_maker_fill_assumption(tmp_path):
    store = EvidenceStore(tmp_path / "trade-flow.sqlite3")
    plane = SourceCoveragePlane(store)
    for index, side in enumerate(["buy", "buy", "buy", "sell"]):
        plane.record_event(SourceEventObservation(
            event_id=f"trade-{index}",
            lane_id="microstructure",
            source_id="public-trade-flow",
            event_type="public_trade",
            event_at=NOW - timedelta(minutes=4 - index),
            observed_at=NOW,
            asset="BTC",
            payload={
                "venue": "Coinbase",
                "symbol": "BTC-USD",
                "aggressor_side": side,
                "price": 60_000.0,
                "size": 1.0 if side == "buy" else 0.1,
            },
        ))
    strategy = TradeFlowImbalanceStrategy(TradeFlowLedger(store))
    q = market_quote(NOW)
    snapshot = ScanSnapshot(
        scan_id="trade-flow",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[q],
        opportunities=[],
    )
    # Microstructure controls live in the expanded settings view rather than the
    # base Settings dataclass. The strategy intentionally uses getattr defaults, so
    # this fixture supplies only the controls under test.
    settings = SimpleNamespace(
        alpha_microstructure_min_abs_imbalance=0.20,
        alpha_microstructure_return_scale=0.012,
        alpha_microstructure_max_expected_return=0.006,
        alpha_research_cost_floor_bps=1.0,
        alpha_min_current_net_return=0.0001,
        alpha_microstructure_lookback_hours=6.0,
        alpha_microstructure_horizon_hours=0.25,
        alpha_microstructure_max_candidates=6,
        alpha_min_notional_usd=100.0,
        alpha_candidate_capital_fraction=0.02,
        spot_collateral_fraction=1.0,
        perp_collateral_fraction=0.25,
        alpha_min_history_points=8,
    )
    rows = strategy.discover(
        snapshot,
        {(q.venue, "BTC", q.market_kind): [q]},
        settings,
        total_capital_usd=100_000,
    )
    assert rows
    assert rows[0].features["trade_count"] == 4
    assert rows[0].features["maker_fill_assumed"] is False
    assert rows[0].paper_allocation_eligible is False


def test_market_making_settlement_uses_observed_trade_through_and_future_inventory_mark(tmp_path):
    store = EvidenceStore(tmp_path / "maker.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    plane = SourceCoveragePlane(store)
    for index, (side, price) in enumerate([("sell", 99.0), ("buy", 101.0)]):
        plane.record_event(SourceEventObservation(
            event_id=f"maker-trade-{index}",
            lane_id="liquidity_provision",
            source_id="public-trade-flow",
            event_type="public_trade",
            event_at=NOW + timedelta(minutes=2 + index),
            observed_at=NOW + timedelta(minutes=2 + index),
            asset="BTC",
            payload={
                "venue": "Coinbase",
                "symbol": "BTC-USD",
                "aggressor_side": side,
                "price": price,
                "size": 5.0,
            },
        ))
    store.record_scan(
        funding_quotes=[],
        market_quotes=[MarketQuote(
            venue="Coinbase",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-USD",
            mid=100.5,
            bid=100.4,
            ask=100.6,
            observed_at=NOW + timedelta(hours=1),
            source="test",
        )],
        opportunities=[],
        providers=[],
        started_at=NOW,
        completed_at=NOW + timedelta(hours=1),
    )
    trial = MechanismForwardTrial(
        mechanism_id="liquidity_provision",
        cohort_key="maker|Coinbase|BTC|BTC-USD",
        asset="BTC",
        venues=["Coinbase"],
        source_observed_at=NOW,
        due_at=NOW + timedelta(minutes=15),
        capital_usd=1000,
        predicted_net_return=0.005,
        predicted_profit_usd=5,
        settlement_payload={
            "venue": "Coinbase",
            "asset": "BTC",
            "symbol": "BTC-USD",
            "market_kind": "spot",
            "bid": 99.0,
            "ask": 101.0,
            "mid": 100.0,
            "quantity": 10.0,
        },
    )
    result = service.settle_trial(trial)
    assert result is not None
    assert result.detail["bid_filled"] is True
    assert result.detail["ask_filled"] is True
    assert result.detail["empirical_fill_observed"] is True
    assert result.settlement_method == "shadow_post_only_fill_plus_inventory_mark"


def test_mechanism_candidate_is_accepted_by_canonical_paper_settlement_contract(tmp_path):
    store = EvidenceStore(tmp_path / "canonical-mechanism.sqlite3")
    mechanism = ExecutableMechanismExecutionService(core(), store)
    candidate = MechanismPaperCandidate(
        candidate_id="mechanism:yield:test",
        mechanism_id="yield",
        cohort_key="yield|Morpho|USDC|lending",
        asset="USDC",
        venues=["Morpho"],
        observed_at=NOW,
        holding_hours=24.0,
        capital_usd=1000.0,
        expected_net_return=0.002,
        expected_profit_usd=2.0,
        evidence_sample_count=3,
        evidence_allocation_fraction=0.10,
        settlement_payload={"protocol": "Morpho", "asset": "USDC", "entry_net_apy": 0.10},
        conflict_keys=["yield:Morpho:USDC"],
    )
    mechanism.ledger.record_candidate(candidate)
    allocation = UnifiedPaperAllocation(
        candidate_id=candidate.candidate_id,
        family="mechanism",
        strategy="mechanism:yield:test",
        asset="USDC",
        venues=["Morpho"],
        capital_required_usd=1000.0,
        notional_usd_per_leg=1000.0,
        expected_profit_usd_per_deployment=2.0,
        expected_return_on_reserved_capital=0.002,
        modeled_holding_hours=24.0,
        source_return_metric="mechanism_forward_evidence_net_return",
        source_return_value=0.002,
        source_observed_at=NOW,
        instrument_symbol="USDC",
        instrument_market_kind="mechanism",
        entry_reference_price=1.0,
        modeled_roundtrip_cost_return=0.0,
        opportunity_id="yield",
    )
    settlement = AllLaneAllocationForwardCertificationService(core(), SimpleNamespace(), store)
    trial = settlement.trial_from_allocation(allocation, plan_observed_at=NOW)
    assert trial.settlement_supported is True
    assert trial.settlement_method == "mechanism:yield"
    assert trial.family == "mechanism"
    assert trial.live_execution_authority is False
