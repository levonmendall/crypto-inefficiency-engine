from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.allocation_certification import (
    AllocationCertificationLedger,
    AllocationForwardCertificationService,
)
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def allocation(
    *,
    exposure: str = "directional_long",
    kind: str = "spot",
    family: str = "alpha",
) -> UnifiedPaperAllocation:
    return UnifiedPaperAllocation(
        candidate_id=f"candidate-{family}-{exposure}",
        family=family,
        strategy="time_series_momentum_v1" if family == "alpha" else "spot_perp_basis",
        asset="BTC",
        venues=["Coinbase"] if family == "alpha" else ["Coinbase", "HlPerp"],
        capital_required_usd=10000.0,
        notional_usd_per_leg=10000.0,
        expected_profit_usd_per_deployment=50.0,
        expected_return_on_reserved_capital=0.005,
        modeled_holding_hours=6.0,
        source_return_metric="forward_ci_health_haircut_net_return",
        source_return_value=0.005,
        exposure_kind=exposure,
        source_observed_at=NOW,
        instrument_symbol="BTC-USD" if family == "alpha" else None,
        instrument_market_kind=kind if family == "alpha" else None,
        entry_reference_price=60000.0 if family == "alpha" else None,
        modeled_roundtrip_cost_return=0.002 if family == "alpha" else None,
    )


def test_spot_long_allocation_is_forward_settleable_but_perp_short_and_neutral_are_not():
    spot = AllocationForwardCertificationService.trial_from_allocation(
        allocation(),
        plan_observed_at=NOW,
    )
    assert spot.settlement_supported is True
    assert spot.due_at == NOW + timedelta(hours=6)
    assert spot.settlement_method == AllocationForwardCertificationService.SETTLEMENT_METHOD

    short = AllocationForwardCertificationService.trial_from_allocation(
        allocation(exposure="directional_short", kind="perpetual"),
        plan_observed_at=NOW,
    )
    assert short.settlement_supported is False
    assert "funding" in (short.settlement_blocker or "")

    neutral = AllocationForwardCertificationService.trial_from_allocation(
        allocation(exposure="market_neutral", family="core_cex"),
        plan_observed_at=NOW,
    )
    assert neutral.settlement_supported is False
    assert "multi-leg" in (neutral.settlement_blocker or "")


def test_supported_trial_settles_forward_price_move_net_of_precommitted_cost(tmp_path):
    store = EvidenceStore(tmp_path / "allocation.sqlite3")
    service = AllocationForwardCertificationService(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        store,
    )
    trial = service.trial_from_allocation(allocation(), plan_observed_at=NOW)
    snapshot_time = NOW + timedelta(hours=6)
    snapshot = ScanSnapshot(
        scan_id="settlement",
        started_at=snapshot_time,
        completed_at=snapshot_time,
        providers=[],
        funding_quotes=[],
        market_quotes=[MarketQuote(
            venue="Coinbase",
            asset="BTC",
            market_kind=MarketKind.SPOT,
            symbol="BTC-USD",
            mid=60600.0,
            bid=60590.0,
            ask=60610.0,
            observed_at=snapshot_time,
            source="test",
        )],
        opportunities=[],
    )
    outcome = service._settle_trial(trial, snapshot)
    assert outcome is not None
    assert outcome.realized_gross_return == pytest.approx(0.01)
    assert outcome.realized_net_return == pytest.approx(0.008)
    assert outcome.realized_profit_usd == pytest.approx(80.0)
    assert outcome.prediction_error_usd == pytest.approx(30.0)
    assert outcome.profit_capture_ratio == pytest.approx(1.6)
    assert outcome.profitable is True


def test_allocation_ledger_separates_supported_settlements_from_unsupported_decisions(tmp_path):
    store = EvidenceStore(tmp_path / "ledger.sqlite3")
    ledger = AllocationCertificationLedger(store)
    supported = AllocationForwardCertificationService.trial_from_allocation(allocation(), plan_observed_at=NOW)
    unsupported = AllocationForwardCertificationService.trial_from_allocation(
        allocation(exposure="market_neutral", family="core_cex"),
        plan_observed_at=NOW,
    )
    ledger.record_trial(supported)
    ledger.record_trial(supported)
    ledger.record_trial(unsupported)

    summary = ledger.summary()
    assert summary["trial_count"] == 2
    assert summary["supported_trial_count"] == 1
    assert summary["unsupported_trial_count"] == 1
    assert summary["settled_outcome_count"] == 0
    assert summary["realized_profit_usd_settled_trials"] == 0


def test_matured_but_unsettled_supported_trial_still_blocks_overlapping_cohort(tmp_path):
    store = EvidenceStore(tmp_path / "overlap.sqlite3")
    ledger = AllocationCertificationLedger(store)
    trial = AllocationForwardCertificationService.trial_from_allocation(allocation(), plan_observed_at=NOW)
    trial.due_at = NOW - timedelta(minutes=1)
    ledger.record_trial(trial)

    assert ledger.has_unsettled_supported_cohort(trial.cohort_key) is True
    pending = ledger.pending_supported_trials(now=NOW)
    assert [row.trial_id for row in pending] == [trial.trial_id]
