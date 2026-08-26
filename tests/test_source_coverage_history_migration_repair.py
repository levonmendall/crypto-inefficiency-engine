from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import LaneSourceCoverage, SourceCoverageSnapshot
from inefficiency_engine import source_coverage_history_migration_child as child
from inefficiency_engine import source_coverage_history_migration_supervisor as supervisor


START = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def _snapshot(observed_at: datetime) -> SourceCoverageSnapshot:
    classes = ["yield_rate", "capacity", "exit_liquidity"]
    lane = LaneSourceCoverage(
        lane_id="yield",
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
        priority_order=["yield"],
        lanes=[lane],
    )


def _archive(store: EvidenceStore, snapshot: SourceCoverageSnapshot) -> None:
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


def test_independent_child_advances_checkpoint_until_complete(tmp_path):
    store = EvidenceStore(tmp_path / "history-repair.sqlite3")
    _archive(store, _snapshot(START))
    _archive(store, _snapshot(START + timedelta(minutes=1)))

    first = child.advance_one_history_migration_batch(store, max_heartbeats=1)
    assert first["complete"] is False
    assert first["migrated_heartbeats"] == 1

    second = child.advance_one_history_migration_batch(store, max_heartbeats=1)
    assert second["complete"] is True
    assert second["migrated_heartbeats"] == 1

    heartbeat = store.latest_worker_heartbeat(child.SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["stage"] == "canonical_history_ready"
    assert heartbeat.detail["provider_requests_allowed"] is False
    assert heartbeat.detail["candidate_level_history_synthesized"] is False
    assert heartbeat.detail["historical_counts_as_forward"] is False
    assert heartbeat.detail["allocation_authority"] is False
    assert heartbeat.detail["live_execution_authority"] is False


class _NoSleepStop:
    def is_set(self) -> bool:
        return False

    def wait(self, _seconds: float) -> bool:
        return False


def test_supervisor_retries_progress_batches_until_complete(monkeypatch):
    calls: list[int] = []
    results = [
        (child.MIGRATION_INCOMPLETE_EXIT_CODE, False),
        (child.MIGRATION_INCOMPLETE_EXIT_CODE, False),
        (0, False),
    ]

    monkeypatch.setattr(supervisor, "_api_is_bound", lambda _port: True)

    def fake_run(_stop_event, *, deadline_seconds=supervisor.MIGRATION_EXECUTOR_DEADLINE_SECONDS):
        calls.append(int(deadline_seconds))
        return results.pop(0)

    monkeypatch.setattr(supervisor, "_run_bounded_child", fake_run)

    supervisor.run_source_coverage_history_migration_supervisor(_NoSleepStop())

    assert len(calls) == 3
    assert not results


def test_live_source_executor_no_longer_owns_archive_backfill():
    from inefficiency_engine import source_coverage_snapshot_executor

    source = Path(source_coverage_snapshot_executor.__file__).read_text()
    assert "backfill_source_coverage_history_from_heartbeats" not in source
    assert "archive_migration_owner" in source
    assert "persist_source_coverage_history_snapshot" in source


def test_production_runtime_starts_independent_history_migration_supervisor():
    from inefficiency_engine import render_combined_postbind_lane_repair

    source = Path(render_combined_postbind_lane_repair.__file__).read_text()
    assert "run_source_coverage_history_migration_supervisor" in source
    assert 'name="source-coverage-history-migration-supervisor"' in source
    assert "source_history_guard.start()" in source
