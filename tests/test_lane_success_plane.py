from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.lane_success import LaneSuccessController
from inefficiency_engine.trade_flow_integrity import _integrity_summary
from inefficiency_engine.unified_allocation import UnifiedPaperCandidate


NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def candidate(
    candidate_id: str,
    strategy: str,
    *,
    expected_return: float = 0.01,
    holding_hours: float = 1.0,
    asset: str = "BTC",
):
    capital = 1000.0
    return UnifiedPaperCandidate(
        candidate_id=candidate_id,
        family="alpha",
        strategy=strategy,
        asset=asset,
        venues=["Bybit"],
        capital_required_usd=capital,
        notional_usd_per_leg=capital,
        expected_profit_usd_per_deployment=capital * expected_return,
        expected_return_on_reserved_capital=expected_return,
        modeled_holding_hours=holding_hours,
        source_return_metric="test",
        source_return_value=expected_return,
        exposure_kind="directional_long",
        instrument_symbol=f"{asset}USDT",
        instrument_market_kind="spot",
        entry_reference_price=100.0,
        modeled_roundtrip_cost_return=0.001,
        paper_only=True,
    )


def record_outcome(
    controller: LaneSuccessController,
    *,
    strategy: str,
    index: int,
    predicted: float,
    realized: float,
    regime: str = "normal",
):
    at = NOW + timedelta(hours=index * 6)
    controller.ledger.record_outcome(
        outcome_key=f"{strategy}-{index}",
        strategy=strategy,
        asset="BTC",
        regime=regime,
        observed_at=at,
        predicted_return=predicted,
        realized_return=realized,
        predicted_profit_usd=predicted * 1000.0,
        realized_profit_usd=realized * 1000.0,
        capital_usd=1000.0,
        holding_hours=1.0,
        venues=["Bybit"],
        candidate_id=f"{strategy}-{index}",
        settlement_method="test",
        failure_attribution=[],
    )


def test_universal_calibration_is_subtractive_and_regime_conditioned(tmp_path):
    controller = LaneSuccessController(EvidenceStore(tmp_path / "lane-success.sqlite3"))
    strategy = "time_series_momentum_v1"
    for index in range(5):
        record_outcome(
            controller,
            strategy=strategy,
            index=index,
            predicted=0.01,
            realized=0.004,
        )

    adjusted, profile = controller.adjust_return(
        strategy=strategy,
        raw_expected_return=0.02,
        regime="normal",
    )
    assert profile.sample_count == 5
    assert profile.calibration_multiplier == pytest.approx(0.4)
    assert profile.regime_multiplier == pytest.approx(0.4)
    assert 0.0 <= profile.combined_multiplier <= 1.0
    assert adjusted == pytest.approx(0.008)
    assert adjusted <= 0.02


def test_negative_regime_evidence_suspends_without_weakening_upstream_gates(tmp_path):
    controller = LaneSuccessController(EvidenceStore(tmp_path / "regime.sqlite3"))
    strategy = "event_driven_v1"
    for index in range(5):
        record_outcome(
            controller,
            strategy=strategy,
            index=index,
            predicted=0.01,
            realized=-0.003,
            regime="stress_high_vol",
        )

    adjusted, profile = controller.adjust_return(
        strategy=strategy,
        raw_expected_return=0.01,
        regime="stress_high_vol",
    )
    assert adjusted == 0.0
    assert profile.regime_multiplier == 0.0
    assert profile.health_multiplier == 0.0
    assert profile.state == "suspended"


def test_empirical_correlation_suppresses_hidden_duplicate_risk(tmp_path):
    controller = LaneSuccessController(EvidenceStore(tmp_path / "correlation.sqlite3"))
    left = "time_series_momentum_v1"
    right = "mean_reversion_btc_relative_v1"
    values = [0.002, 0.005, 0.003, 0.006, 0.004, 0.007, 0.005, 0.008]
    for index, value in enumerate(values):
        record_outcome(controller, strategy=left, index=index, predicted=0.01, realized=value)
        record_outcome(controller, strategy=right, index=index, predicted=0.01, realized=value)

    first = candidate("fast", left, expected_return=0.01, holding_hours=1.0)
    second = candidate("slow", right, expected_return=0.02, holding_hours=8.0)
    selected, skipped, _ = controller.adjust_and_diversify(
        [second, first],
        total_capital_usd=250_000.0,
        regime="normal",
    )
    assert [row.candidate_id for row in selected] == ["fast"]
    assert any(row.get("candidate_id") == "slow" for row in skipped)
    rejection = next(row for row in skipped if row.get("candidate_id") == "slow")
    assert float(rejection["correlation"]) >= 0.80
    assert "asset:BTC" in rejection["shared_risk_factors"]


def test_failure_attribution_identifies_execution_and_model_causes():
    reasons = LaneSuccessController.failure_attribution(
        predicted_return=0.02,
        realized_return=-0.01,
        settlement_method="option_mark_forward_with_dynamic_delta_hedge_and_greek_penalties",
        detail={
            "observation_latency_seconds": 2.5,
            "bid_crossed_without_fill": True,
            "adverse_selection_penalty": 0.002,
            "hedge_cost_return": 0.003,
            "exit_liquidity_sufficient": False,
        },
    )
    assert "forecast_error" in reasons
    assert "source_latency" in reasons
    assert "timing_decay" in reasons
    assert "queue_non_fill" in reasons
    assert "adverse_selection" in reasons
    assert "hedge_error" in reasons
    assert "liquidity_loss" in reasons
    assert "model_overconfidence" in reasons


def test_event_integrity_reports_sequence_gaps_duplicates_and_latency():
    events = [
        {
            "exchange_event_id": "1",
            "sequence": 10,
            "symbol": "BTCUSDT",
            "event_at": NOW,
            "received_at": NOW + timedelta(milliseconds=10),
        },
        {
            "exchange_event_id": "1",
            "sequence": 12,
            "symbol": "BTCUSDT",
            "event_at": NOW + timedelta(milliseconds=5),
            "received_at": NOW + timedelta(milliseconds=30),
        },
    ]
    summary = _integrity_summary(events)
    assert summary["duplicate_event_count"] == 1
    assert summary["sequence_supported"] is True
    assert summary["sequence_gap_count"] == 1
    assert summary["max_receive_latency_ms"] == pytest.approx(25.0)
    assert summary["integrity_degraded"] is True


def test_production_all_lane_runtime_installs_lane_success_services():
    from inefficiency_engine import worker_children as base
    from inefficiency_engine.lane_success_runtime import (
        LaneSuccessAllocationForwardCertificationService,
        LaneSuccessOperationallyResilientPaperPortfolioService,
        LaneSuccessQualifiedOpportunityAllocatorService,
    )
    from inefficiency_engine.worker_children_all_lanes import _install_all_lane_runtime

    _install_all_lane_runtime()
    assert base.UnifiedPaperAllocatorService is LaneSuccessQualifiedOpportunityAllocatorService
    assert base.AllocationForwardCertificationService is LaneSuccessAllocationForwardCertificationService
    assert (
        base.OperationallyResilientPaperPortfolioService
        is LaneSuccessOperationallyResilientPaperPortfolioService
    )
