from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import LaneSourceCoverage, SourceCoverageSnapshot
from inefficiency_engine.source_coverage_history import (
    SOURCE_COVERAGE_HISTORY_TABLE,
    SourceCoverageHistoryLedger,
    backfill_source_coverage_history_from_heartbeats,
)


START = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def _snapshot(observed_at: datetime, *, lane_id: str = "yield") -> SourceCoverageSnapshot:
    classes = ["yield_rate", "capacity", "exit_liquidity"]
    lane = LaneSourceCoverage(
        lane_id=lane_id,
        name="Yield",
        required_evidence_classes=classes,
        covered_evidence_classes=classes,
        missing_evidence_classes=[],
        healthy_source_count=1,
        independent_authoritative_source_count=1,
        admitted_authoritative_source_groups=["morpho"],
        source_redundancy_satisfied=False,
        evidence_class_coverage_satisfied=True,
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=False,
        source_layer_sufficient=False,
        source_state="concentration_risk",
        sources=[
            {
                "source_id": "morpho-markets",
                "classes": classes,
                "group": "morpho",
                "authoritative": True,
                "healthy": True,
                "fresh": True,
                "admitted": True,
                "observed_at": observed_at.isoformat(),
            }
        ],
    )
    return SourceCoverageSnapshot(
        observed_at=observed_at,
        lane_count=1,
        sufficient_lane_count=0,
        insufficient_lane_count=1,
        research_eligible_lane_count=1,
        forward_test_eligible_lane_count=1,
        allocation_source_qualified_lane_count=0,
        priority_order=[lane_id],
        lanes=[lane],
    )


def _archive_snapshot(store: EvidenceStore, snapshot: SourceCoverageSnapshot) -> None:
    store.record_worker_heartbeat(
        worker_id="canonical-source-coverage-snapshot",
        state="success",
        observed_at=snapshot.observed_at + timedelta(seconds=1),
        detail={
            "snapshot": snapshot.model_dump(mode="json"),
            "snapshot_observed_at": snapshot.observed_at.isoformat(),
            "persisted_complete_snapshot": True,
        },
    )


def test_history_ledger_records_lane_snapshot_idempotently(tmp_path):
    store = EvidenceStore(tmp_path / "history.sqlite3")
    ledger = SourceCoverageHistoryLedger(store)
    snapshot = _snapshot(START)

    assert ledger.record_snapshot(snapshot, published_at=START + timedelta(seconds=1)) == 1
    assert ledger.record_snapshot(snapshot, published_at=START + timedelta(seconds=2)) == 0

    summary = ledger.summary(start=START - timedelta(minutes=1), end=START + timedelta(minutes=1))
    row = summary["yield"]
    assert row["canonical_snapshot_count"] == 1
    assert row["source_count"] == 1
    assert row["evidence_classes"] == {"yield_rate", "capacity", "exit_liquidity"}
    assert row["source_ids"] == {"morpho-markets"}
    assert row["source_ledgers"] == {SOURCE_COVERAGE_HISTORY_TABLE}


def test_heartbeat_archive_migration_is_checkpointed_and_resumable(tmp_path):
    store = EvidenceStore(tmp_path / "migration.sqlite3")
    first = _snapshot(START)
    second = _snapshot(START + timedelta(minutes=1))
    _archive_snapshot(store, first)
    _archive_snapshot(store, second)

    first_pass = backfill_source_coverage_history_from_heartbeats(
        store,
        max_heartbeats=1,
    )
    assert first_pass["complete"] is False
    assert first_pass["migrated_heartbeats"] == 1
    assert first_pass["inserted_lane_snapshots"] == 1

    ledger = SourceCoverageHistoryLedger(store)
    status = ledger.migration_status()
    assert status["complete"] is False
    assert status["checkpoint_heartbeat_id"] > 0

    second_pass = backfill_source_coverage_history_from_heartbeats(
        store,
        max_heartbeats=1,
    )
    assert second_pass["complete"] is True
    assert second_pass["migrated_heartbeats"] == 1
    assert second_pass["inserted_lane_snapshots"] == 1
    assert ledger.migration_status()["complete"] is True

    no_op = backfill_source_coverage_history_from_heartbeats(store, max_heartbeats=10)
    assert no_op["complete"] is True
    assert no_op["migrated_heartbeats"] == 0
    assert no_op["inserted_lane_snapshots"] == 0

    summary = ledger.summary(
        start=START - timedelta(minutes=1),
        end=START + timedelta(minutes=2),
    )
    assert summary["yield"]["canonical_snapshot_count"] == 2


def test_live_snapshot_can_precede_archive_migration_without_duplication(tmp_path):
    store = EvidenceStore(tmp_path / "live-first.sqlite3")
    snapshot = _snapshot(START)
    _archive_snapshot(store, snapshot)
    ledger = SourceCoverageHistoryLedger(store)

    assert ledger.record_snapshot(snapshot, published_at=START + timedelta(seconds=1)) == 1
    result = backfill_source_coverage_history_from_heartbeats(store, max_heartbeats=10)

    assert result["complete"] is True
    assert result["inserted_lane_snapshots"] == 0
    summary = ledger.summary(
        start=START - timedelta(minutes=1),
        end=START + timedelta(minutes=1),
    )
    assert summary["yield"]["canonical_snapshot_count"] == 1
