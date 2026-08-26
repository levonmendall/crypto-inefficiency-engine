from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from inefficiency_engine import read_api_certification_fast_readiness as fast_readiness
from inefficiency_engine import read_api_cycle_history_truth_repair as truth_repair
from inefficiency_engine import read_api_end_to_end_certification_deploy as certification
from inefficiency_engine import source_coverage_history_migration_child as history_child
from inefficiency_engine.evidence import EvidenceStore


def test_certification_batch_includes_every_post_readiness_truth_worker():
    mapping = fast_readiness._certification_heartbeat_mapping()

    assert mapping["source_coverage_snapshot"] == "canonical-source-coverage-snapshot"
    assert mapping["research_projection"] == "dashboard-research-projection-publisher"
    assert mapping["runtime_index_maintenance"] == "source-coverage-runtime-index-maintenance"
    assert mapping["source_history_migration"] == "canonical-source-coverage-history-migration"
    assert mapping["cycle_history_backfill"] == "cycle-history-background-backfill"
    assert mapping["cycle_history_index_maintenance"] == "cycle-history-index-maintenance"


def test_alpha_forward_is_reaged_from_compact_research_publication():
    now = datetime(2026, 8, 26, 20, 0, tzinfo=timezone.utc)
    observed = now - timedelta(seconds=125)
    research = {
        "critical_evidence_recovery": {
            "workers": {
                "alpha_forward": {
                    "worker_id": "shadow-research-auxiliary",
                    "signal": "alpha_forward_evidence_cycle_id",
                    "available": True,
                    "observed_at": observed.isoformat(),
                    "state": "running",
                    "cycle_id": "alpha-cycle-1",
                    "recovery_after_seconds": 1200.0,
                }
            }
        }
    }

    status = certification._alpha_forward_status_from_research_worker(
        research,
        now=now,
    )

    assert status["available"] is True
    assert status["recovery_required"] is False
    assert status["age_seconds"] == 125.0
    assert status["cycle_id"] == "alpha-cycle-1"
    assert status["source"] == "research_worker_compact_recovery_snapshot"


def test_completed_source_history_status_uses_published_counts_only():
    status = certification._source_history_status_from_worker(
        {
            "available": True,
            "state": "success",
            "stage": "canonical_history_ready",
            "complete": True,
            "compact_certification_summary": True,
            "checkpoint_heartbeat_id": 1234,
            "lane_count": 13,
            "snapshot_count": 70000,
            "observed_at": "2026-08-26T20:00:00+00:00",
        }
    )

    assert status["migration_complete"] is True
    assert status["lane_count"] == 13
    assert status["snapshot_count"] == 70000
    assert status["request_time_archive_count_queries"] == 0


def test_migration_child_publishes_final_counts_off_request_path(tmp_path):
    store = EvidenceStore(tmp_path / "history-summary.sqlite")
    with store.engine.begin() as db:
        db.execute(
            text(
                "CREATE TABLE canonical_source_coverage_history ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, lane_id TEXT NOT NULL)"
            )
        )
        db.execute(
            text(
                "INSERT INTO canonical_source_coverage_history(lane_id) "
                "VALUES ('price_discrepancy'),('carry'),('carry')"
            )
        )

    summary = history_child._completed_history_summary(store)

    assert summary == {"lane_count": 2, "snapshot_count": 3}


def test_e2e_request_has_no_post_readiness_database_audit_calls():
    source = inspect.getsource(certification.end_to_end_certification_payload)

    assert "active._store" not in source
    assert "_alpha_forward_status(" not in source
    assert "inspect(" not in source
    assert "latest_worker_heartbeat" not in source
    assert '"certification_post_readiness_database_reads": 0' in source


def test_truth_repair_reuses_worker_snapshot_without_database_lookup():
    source = inspect.getsource(truth_repair.repaired_end_to_end_certification_payload)

    assert "include_worker_truth=True" in source
    assert "latest_worker_heartbeat" not in source
    assert "deployment_readiness" not in source
    assert '"truth_repair_additional_database_reads": 0' in source
