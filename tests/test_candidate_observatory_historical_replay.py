from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from inefficiency_engine.alpha_factory import (
    AlphaCandidate,
    AlphaEvidenceLedger,
    AlphaForwardSignal,
)
from inefficiency_engine.candidate_observatory import (
    CandidateObservatoryLedger,
    CandidateObservatorySnapshot,
)
from inefficiency_engine.candidate_observatory_backfill_supervisor import BACKFILL_COMMAND
from inefficiency_engine.candidate_observatory_historical_replay import (
    HistoricalCandidateReplayLedger,
    read_historical_candidate_replay,
    run_historical_candidate_replay_batch,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind
from inefficiency_engine.research_closure_worker import (
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)


START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
LIVE_START = datetime(2026, 8, 25, 16, 30, tzinfo=timezone.utc)


def _candidate(*, observed_at: datetime, candidate_id: str) -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id=candidate_id,
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=observed_at,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=100.0,
        expected_gross_return=0.012,
        estimated_cost_return=0.002,
        expected_net_return=0.010,
        expected_profit_usd=100.0,
        notional_usd=10_000.0,
        capital_required_usd=10_000.0,
        confidence_score=0.8,
        regime="normal",
        conflict_keys=["alpha-instrument:Coinbase:BTC-USD"],
    )


def _record_live_boundary(store: EvidenceStore) -> None:
    ledger = CandidateObservatoryLedger(store)
    ledger.record_snapshot(
        CandidateObservatorySnapshot(
            cycle_id="live-cycle",
            observed_at=LIVE_START,
            source_scan_id="live-scan",
        )
    )


def _record_structural_summary(store: EvidenceStore) -> None:
    ResearchClosureSummaryLedger(store).record(
        ResearchClosureCycleSummary(
            observed_at=START + timedelta(hours=12),
            source_scan_id="closure-scan",
            source_order_book_count=4,
            usable_order_book_count=4,
            rejection_funnels={
                "price_discrepancy": {
                    "raw_candidate_count": 66,
                    "emitted_candidate_count": 0,
                    "dominant_rejection_gate": "net_return_hurdle",
                },
                "carry": {
                    "raw_candidate_count": 1204,
                    "emitted_candidate_count": 0,
                    "dominant_rejection_gate": "detector_output_mismatch",
                },
                # Legacy closure microstructure was a cross-cycle join and must not
                # be promoted into historical truth by the replay.
                "microstructure": {
                    "raw_candidate_count": 0,
                    "emitted_candidate_count": 5,
                    "dominant_rejection_gate": "no_usable_order_book",
                },
            },
            capital_location_forward={},
            maker_shadow={},
            canonical_capabilities={},
            provider_admission={},
            diagnostic_errors={},
        )
    )


def test_replay_recovers_exact_selected_candidates_and_aggregate_funnels(tmp_path):
    store = EvidenceStore(tmp_path / "historical-observatory.sqlite3")
    alpha = AlphaEvidenceLedger(store)
    before = _candidate(
        observed_at=START - timedelta(minutes=1),
        candidate_id="before-window",
    )
    selected = _candidate(
        observed_at=START + timedelta(hours=2),
        candidate_id="selected-in-window",
    )
    after_live = _candidate(
        observed_at=LIVE_START + timedelta(minutes=1),
        candidate_id="after-live-boundary",
    )
    for candidate in (before, selected, after_live):
        alpha.record_signal(
            AlphaForwardSignal(
                signal_id=candidate.candidate_id,
                candidate=candidate,
                due_at=candidate.observed_at + timedelta(hours=candidate.horizon_hours),
            )
        )

    store.record_worker_heartbeat(
        worker_id="shadow-research-auxiliary",
        state="running",
        cycle_id="alpha-cycle-1",
        observed_at=START + timedelta(hours=3),
        detail={
            "alpha_discovery_funnel": {
                "trend_momentum": {
                    "raw_candidate_count": 8,
                    "emitted_candidate_count": 1,
                    "dominant_rejection_gate": "candidate_emitted",
                },
                "microstructure": {
                    "raw_candidate_count": 5,
                    "emitted_candidate_count": 0,
                    "dominant_rejection_gate": "net_return_hurdle",
                },
            }
        },
    )
    _record_structural_summary(store)
    _record_live_boundary(store)

    original_alpha_count = alpha.summary()["signal_count"]
    result = run_historical_candidate_replay_batch(
        store,
        start=START,
        now=LIVE_START + timedelta(hours=1),
        batch_size=100,
    )

    assert result["complete"] is True
    replay = read_historical_candidate_replay(store, limit=100)
    assert replay["available"] is True
    assert replay["complete"] is True
    assert len(replay["selected_candidates"]) == 1
    selected_row = replay["selected_candidates"][0]
    assert selected_row["candidate"]["candidate_id"] == "selected-in-window"
    assert selected_row["candidate"]["expected_net_return"] == 0.010
    assert selected_row["exact_persisted_evidence"] is True
    assert selected_row["historical_replay"] is True
    assert selected_row["historical_counts_as_forward"] is False
    assert selected_row["qualification_authority"] is False
    assert selected_row["allocation_authority"] is False
    assert selected_row["live_execution_authority"] is False

    assert len(replay["alpha_funnels"]) == 1
    assert replay["alpha_funnels"][0]["funnels"]["trend_momentum"]["raw_candidate_count"] == 8
    assert replay["alpha_funnels"][0]["funnels"]["microstructure"]["emitted_candidate_count"] == 0

    assert len(replay["structural_funnels"]) == 1
    structural = replay["structural_funnels"][0]
    assert structural["funnels"]["price_discrepancy"]["raw_candidate_count"] == 66
    assert structural["funnels"]["carry"]["raw_candidate_count"] == 1204
    assert "microstructure" not in structural["funnels"]
    assert structural["omitted_untrusted_legacy_funnels"] == ["microstructure"]

    # The replay is a read/index operation only. It never manufactures forward data.
    assert alpha.summary()["signal_count"] == original_alpha_count


def test_replay_is_idempotent_and_does_not_cross_live_boundary(tmp_path):
    store = EvidenceStore(tmp_path / "historical-observatory-idempotent.sqlite3")
    alpha = AlphaEvidenceLedger(store)
    selected = _candidate(
        observed_at=START + timedelta(hours=2),
        candidate_id="selected-in-window",
    )
    alpha.record_signal(
        AlphaForwardSignal(
            signal_id=selected.candidate_id,
            candidate=selected,
            due_at=selected.observed_at + timedelta(hours=selected.horizon_hours),
        )
    )
    _record_live_boundary(store)

    first = run_historical_candidate_replay_batch(
        store, start=START, now=LIVE_START + timedelta(hours=1), batch_size=100
    )
    second = run_historical_candidate_replay_batch(
        store, start=START, now=LIVE_START + timedelta(hours=1), batch_size=100
    )
    assert first["complete"] is True
    assert second["complete"] is True

    ledger = HistoricalCandidateReplayLedger(store, create_schema=False)
    with store.engine.connect() as db:
        count = db.execute(select(func.count()).select_from(ledger.records)).scalar_one()
    assert count == 1


def test_replay_waits_for_a_genuine_live_boundary_before_declaring_complete(tmp_path):
    store = EvidenceStore(tmp_path / "historical-observatory-wait.sqlite3")
    alpha = AlphaEvidenceLedger(store)
    selected = _candidate(
        observed_at=START + timedelta(hours=2),
        candidate_id="selected-in-window",
    )
    alpha.record_signal(
        AlphaForwardSignal(
            signal_id=selected.candidate_id,
            candidate=selected,
            due_at=selected.observed_at + timedelta(hours=selected.horizon_hours),
        )
    )

    result = run_historical_candidate_replay_batch(
        store,
        start=START,
        now=START + timedelta(days=4),
        batch_size=100,
    )
    assert result["complete"] is False
    assert result["waiting_for_live_observatory_boundary"] is True


def test_render_backfill_command_uses_disposable_heavy_lease_job():
    assert BACKFILL_COMMAND[-2:] == [
        "inefficiency_engine.disposable_heavy_job",
        "observatory_backfill",
    ]
