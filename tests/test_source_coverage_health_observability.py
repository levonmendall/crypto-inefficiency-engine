from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.runtime_index_health_observability import (
    SOURCE_COVERAGE_REFRESH_LABEL,
    SOURCE_COVERAGE_REFRESH_WORKER_ID,
    SOURCE_COVERAGE_SNAPSHOT_LABEL,
    SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    install_runtime_index_health_observability,
)


NOW = datetime(2026, 8, 24, 5, 45, tzinfo=timezone.utc)


class _Store:
    def __init__(self):
        self.rows = {
            SOURCE_COVERAGE_REFRESH_WORKER_ID: SimpleNamespace(
                state="degraded",
                error_type="SourceCoverageSnapshotRefreshDeadlineExceeded",
                observed_at=NOW,
                detail={
                    "sequence": 7,
                    "stage": "source_coverage_snapshot_refresh_failed",
                    "ok": False,
                    "return_code": -9,
                    "executor_pid": 321,
                    "executor_runtime_seconds": 47.1,
                    "executor_deadline_seconds": 45.0,
                    "executor_terminated": True,
                    "executor_killed": True,
                    "independent_publication_cadence": True,
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                },
            ),
            SOURCE_COVERAGE_SNAPSHOT_WORKER_ID: SimpleNamespace(
                state="success",
                error_type=None,
                observed_at=NOW,
                detail={
                    "snapshot_observed_at": "2026-08-24T05:43:00+00:00",
                    "publication_owner": "source-coverage-reconciliation",
                    "persisted_complete_snapshot": True,
                    "lane_count": 13,
                    "sufficient_lane_count": 8,
                    "forward_test_eligible_lane_count": 10,
                    "allocation_source_qualified_lane_count": 8,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                },
            ),
        }

    def latest_worker_heartbeat(self, worker_id):
        return self.rows.get(worker_id)


def test_source_coverage_workers_are_exposed_with_diagnostic_details():
    store = _Store()
    base = SimpleNamespace(
        _RUNTIME_HEARTBEATS={},
        _RUNTIME_STALE_AFTER_SECONDS={},
        _store=lambda: store,
    )

    def original_runtime_heartbeats():
        return {
            "available": True,
            "workers": {
                SOURCE_COVERAGE_REFRESH_LABEL: {
                    "worker_id": SOURCE_COVERAGE_REFRESH_WORKER_ID,
                    "available": True,
                    "state": "degraded",
                    "error_type": "SourceCoverageSnapshotRefreshDeadlineExceeded",
                    "observed_at": NOW.isoformat(),
                    "age_seconds": 2.0,
                    "stale_after_seconds": 180.0,
                    "stale": False,
                    "sequence": 7,
                    "stage": "source_coverage_snapshot_refresh_failed",
                },
                SOURCE_COVERAGE_SNAPSHOT_LABEL: {
                    "worker_id": SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
                    "available": True,
                    "state": "success",
                    "error_type": None,
                    "observed_at": NOW.isoformat(),
                    "age_seconds": 100.0,
                    "stale_after_seconds": 180.0,
                    "stale": False,
                    "sequence": None,
                    "stage": None,
                },
            },
        }

    base._runtime_heartbeats = original_runtime_heartbeats
    install_runtime_index_health_observability(base)

    assert base._RUNTIME_HEARTBEATS[SOURCE_COVERAGE_REFRESH_LABEL] == (
        SOURCE_COVERAGE_REFRESH_WORKER_ID
    )
    assert base._RUNTIME_HEARTBEATS[SOURCE_COVERAGE_SNAPSHOT_LABEL] == (
        SOURCE_COVERAGE_SNAPSHOT_WORKER_ID
    )

    payload = base._runtime_heartbeats()
    refresh = payload["workers"][SOURCE_COVERAGE_REFRESH_LABEL]
    assert refresh["executor_deadline_seconds"] == 45.0
    assert refresh["executor_runtime_seconds"] == 47.1
    assert refresh["executor_terminated"] is True
    assert refresh["executor_killed"] is True
    assert refresh["provider_requests_allowed"] is False
    assert refresh["provider_requests_used"] == 0

    snapshot = payload["workers"][SOURCE_COVERAGE_SNAPSHOT_LABEL]
    assert snapshot["persisted_complete_snapshot"] is True
    assert snapshot["lane_count"] == 13
    assert snapshot["publication_age_seconds"] == 100.0
    assert snapshot["handoff_stale_after_seconds"] == 90.0
    assert snapshot["handoff_stale"] is True

    assert payload["source_coverage_refresh_observability"] is True
    assert payload["source_coverage_snapshot_observability"] is True
