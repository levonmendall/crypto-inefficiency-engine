from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.runtime_index_health_observability import (
    SOURCE_COVERAGE_EXECUTOR_WORKER_ID,
    SOURCE_COVERAGE_REFRESH_LABEL,
    SOURCE_COVERAGE_REFRESH_WORKER_ID,
    SOURCE_COVERAGE_SNAPSHOT_LABEL,
    SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    install_runtime_index_health_observability,
)
from inefficiency_engine.source_coverage_snapshot_executor import (
    _record_executor_stage,
    main as snapshot_executor_main,
)


NOW = datetime(2026, 8, 24, 6, 0, tzinfo=timezone.utc)


def test_health_preserves_previous_refresh_result_while_next_attempt_runs(tmp_path):
    store = EvidenceStore(tmp_path / "source-refresh-terminal.sqlite")
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_EXECUTOR_WORKER_ID,
        state="running",
        observed_at=NOW,
        detail={
            "executor_pid": 321,
            "stage": "snapshot_compute_and_persist",
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        },
    )
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_REFRESH_WORKER_ID,
        state="degraded",
        error_type="SourceCoverageSnapshotRefreshDeadlineExceeded",
        observed_at=NOW,
        detail={
            "sequence": 7,
            "stage": "source_coverage_snapshot_refresh_failed",
            "ok": False,
            "return_code": -9,
            "executor_pid": 321,
            "executor_runtime_seconds": 45.2,
            "executor_deadline_seconds": 45.0,
            "executor_terminated": True,
            "executor_killed": True,
            "independent_publication_cadence": True,
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
    )
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_EXECUTOR_WORKER_ID,
        state="running",
        observed_at=NOW,
        detail={
            "executor_pid": 654,
            "stage": "store_open",
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        },
    )
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_REFRESH_WORKER_ID,
        state="running",
        observed_at=NOW,
        detail={
            "sequence": 8,
            "stage": "source_coverage_snapshot_executor",
            "executor_pid": 654,
            "executor_deadline_seconds": 45.0,
            "independent_publication_cadence": True,
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
    )
    store.record_worker_heartbeat(
        worker_id=SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
        state="success",
        observed_at=NOW,
        detail={
            "snapshot_observed_at": NOW.isoformat(),
            "persisted_complete_snapshot": True,
            "lane_count": 13,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
    )

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
                    "state": "running",
                    "error_type": None,
                    "observed_at": NOW.isoformat(),
                    "age_seconds": 1.0,
                    "stale_after_seconds": 180.0,
                    "stale": False,
                    "sequence": 8,
                    "stage": "source_coverage_snapshot_executor",
                },
                SOURCE_COVERAGE_SNAPSHOT_LABEL: {
                    "worker_id": SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
                    "available": True,
                    "state": "success",
                    "error_type": None,
                    "observed_at": NOW.isoformat(),
                    "age_seconds": 10.0,
                    "stale_after_seconds": 180.0,
                    "stale": False,
                    "sequence": None,
                    "stage": None,
                },
            },
        }

    base._runtime_heartbeats = original_runtime_heartbeats
    install_runtime_index_health_observability(base)
    payload = base._runtime_heartbeats()
    refresh = payload["workers"][SOURCE_COVERAGE_REFRESH_LABEL]

    assert refresh["executor_current_stage"] == "store_open"
    assert refresh["last_refresh_result"] == "failed"
    assert refresh["last_refresh_error_type"] == (
        "SourceCoverageSnapshotRefreshDeadlineExceeded"
    )
    assert refresh["last_refresh_runtime_seconds"] == 45.2
    assert refresh["last_refresh_return_code"] == -9
    assert refresh["last_refresh_executor_pid"] == 321
    assert refresh["last_refresh_executor_stage"] == "snapshot_compute_and_persist"
    assert refresh["last_successful_publication_at"] == NOW.isoformat()
    assert payload["source_coverage_executor_stage_observability"] is True


def test_executor_stage_heartbeat_is_durable(tmp_path):
    store = EvidenceStore(tmp_path / "source-refresh-executor-stage.sqlite")
    _record_executor_stage(
        store,
        stage="snapshot_compute_and_persist_failed",
        state="degraded",
        error_type="ProgrammingError",
        message="bad durable query",
    )

    heartbeat = store.latest_worker_heartbeat(SOURCE_COVERAGE_EXECUTOR_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "degraded"
    assert heartbeat.error_type == "ProgrammingError"
    assert heartbeat.detail["stage"] == "snapshot_compute_and_persist_failed"
    assert heartbeat.detail["message"] == "bad durable query"
    assert heartbeat.detail["provider_requests_allowed"] is False
    assert heartbeat.detail["provider_requests_used"] == 0


def test_snapshot_executor_reports_decisive_internal_stages():
    source = inspect.getsource(snapshot_executor_main)
    assert "store_open" in source
    assert "snapshot_compute_and_persist" in source
    assert "publication_verify" in source
    assert "executor_complete" in source
