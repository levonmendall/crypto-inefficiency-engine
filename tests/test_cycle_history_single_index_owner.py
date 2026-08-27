from __future__ import annotations

import inspect
from datetime import datetime, timezone

from inefficiency_engine import read_api_cycle_history_truth_repair as api_truth
from inefficiency_engine import render_combined_postbind as postbind


def test_generic_postbind_maintainer_does_not_own_exact_cycle_history_btree():
    module_source = inspect.getsource(postbind)
    guard_source = inspect.getsource(postbind._runtime_index_guard)

    assert "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS" not in module_source
    assert '"post_control_cycle_history"' not in guard_source
    assert "post_control_source_strategy" in guard_source
    assert "ensure_cycle_history_brin_after_api_bind" in guard_source
    assert "cycle-history-index-maintenance" in guard_source
    assert "cycle_history_exact_index_maintained_here" in guard_source


def test_dedicated_index_heartbeat_is_exposed_as_single_owner_from_compact_row():
    now = datetime.now(timezone.utc)
    status = api_truth._cycle_history_index_maintenance_status(
        {
            "worker_id": api_truth.CYCLE_HISTORY_INDEX_WORKER_ID,
            "available": True,
            "observed_at": now.isoformat(),
            "age_seconds": 0.0,
            "stale": False,
            "state": "success",
            "error_type": None,
            "stage": "cycle_history_index_ready",
            "index_status": {
                "ready": True,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": "ix_cycle_history_v2",
                "planner_usable_verified": True,
                "reason": "replacement_index_ready",
            },
            "maintenance_result": {"complete": True},
        }
    )

    assert status["available"] is True
    assert status["ready"] is True
    assert status["stale"] is False
    assert status["single_owner"] is True
    assert status["effective_index_name"] == "ix_cycle_history_v2"
    assert status["generic_runtime_exact_index_maintenance_disabled"] is True
    assert status["certification_authority"] is False
    assert status["additional_database_reads"] == 0


def test_supervisor_observed_ready_stage_is_accepted_as_exact_index_truth():
    status = api_truth._cycle_history_index_maintenance_status(
        {
            "worker_id": api_truth.CYCLE_HISTORY_INDEX_WORKER_ID,
            "available": True,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "age_seconds": 0.0,
            "stale": False,
            "state": "success",
            "error_type": None,
            "stage": "cycle_history_index_ready_observed_after_child_exit",
            "index_status": {
                "ready": True,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": "ix_cycle_history_v8",
                "planner_usable_verified": True,
                "reason": "replacement_index_ready",
            },
            "child_return_code": -9,
            "child_exit_error_type": "IndexChildTerminatedBySignal",
            "termination_signal": "SIGKILL",
            "possible_oom_or_external_kill": True,
            "oom_kill_proven": False,
            "ddl_retry_skipped": True,
        }
    )

    assert status["ready"] is True
    assert status["stage"] in api_truth.CYCLE_HISTORY_INDEX_READY_STAGES
    assert status["child_return_code"] == -9
    assert status["child_exit_error_type"] == "IndexChildTerminatedBySignal"
    assert status["termination_signal"] == "SIGKILL"
    assert status["possible_oom_or_external_kill"] is True
    assert status["oom_kill_proven"] is False
    assert status["ddl_retry_skipped"] is True


def test_dedicated_index_supervisor_progress_is_exposed_without_request_time_db_read():
    status = api_truth._cycle_history_index_maintenance_status(
        {
            "worker_id": api_truth.CYCLE_HISTORY_INDEX_WORKER_ID,
            "available": True,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "age_seconds": 1.0,
            "stale": False,
            "state": "running",
            "error_type": None,
            "stage": "cycle_history_index_supervisor_observing",
            "index_status": {
                "ready": False,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": None,
                "planner_usable_verified": True,
                "reason": "planner_usable_index_unavailable",
            },
            "supervisor_observation": True,
            "supervisor_executes_ddl": False,
            "child_pid": 4321,
            "child_runtime_seconds": 47.5,
            "executor_deadline_seconds": 630.0,
            "postgres_progress_available": True,
            "postgres_index_progress": {
                "pid": 998,
                "command": "CREATE INDEX CONCURRENTLY",
                "phase": "building index: scanning table",
                "blocks_total": 1000,
                "blocks_done": 325,
                "index_name": "ix_cycle_history_v9",
            },
            "attempt_number": 107,
        }
    )

    assert status["ready"] is False
    assert status["supervisor_observation"] is True
    assert status["supervisor_executes_ddl"] is False
    assert status["child_pid"] == 4321
    assert status["child_runtime_seconds"] == 47.5
    assert status["executor_deadline_seconds"] == 630.0
    assert status["postgres_progress_available"] is True
    assert status["postgres_index_progress"]["blocks_done"] == 325
    assert status["postgres_index_progress"]["phase"] == (
        "building index: scanning table"
    )
    assert status["attempt_number"] == 107
    assert status["diagnostic_source"] == "batched_latest_worker_heartbeat"
    assert status["additional_database_reads"] == 0


def test_e2e_payload_surfaces_dedicated_index_owner_without_extra_db_read(monkeypatch):
    worker_truth = {
        "canonical_control": {
            "available": True,
            "cycle_history_cache_complete": False,
            "cycle_history_cache_progress": {},
            "historical_cache_progress": {},
        },
        "cycle_history_index_maintenance": {
            "worker_id": api_truth.CYCLE_HISTORY_INDEX_WORKER_ID,
            "available": True,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "age_seconds": 0.0,
            "stale": False,
            "state": "running",
            "error_type": None,
            "stage": "cycle_history_index_supervisor_observing",
            "index_status": {
                "ready": False,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": None,
                "planner_usable_verified": True,
                "reason": "planner_usable_index_unavailable",
            },
            "supervisor_observation": True,
            "child_pid": 4321,
            "child_runtime_seconds": 15.0,
            "executor_deadline_seconds": 630.0,
            "postgres_progress_available": True,
            "postgres_index_progress": {
                "phase": "waiting for writers before build",
            },
        },
    }

    def fake_base(*, include_worker_truth: bool = False):
        payload = {
            "certified": False,
            "operationally_certified": False,
            "status": "blocked",
            "checks": {"cycle_history_serving_target_certified": False},
            "blockers": ["cycle_history_serving_target_certified"],
            "cycle_history_backfill": {
                "available": True,
                "stale": True,
                "cache_complete": False,
                "first_certified_target_pending": True,
                "serving_scan_id": None,
                "progress": {},
            },
            "control": {},
        }
        if include_worker_truth:
            payload["_certification_workers"] = worker_truth
        return payload

    monkeypatch.setattr(api_truth.base, "end_to_end_certification_payload", fake_base)

    payload = api_truth.repaired_end_to_end_certification_payload()

    assert payload["cycle_history_exact_index_single_owner"] is True
    assert payload["cycle_history_exact_index_owner"] == (
        "cycle-history-index-maintenance"
    )
    exact = payload["cycle_history_index_maintenance"]
    assert exact["state"] == "running"
    assert exact["ready"] is False
    assert exact["child_pid"] == 4321
    assert exact["postgres_progress_available"] is True
    assert exact["postgres_index_progress"]["phase"] == (
        "waiting for writers before build"
    )
    assert exact["additional_database_reads"] == 0
    assert payload["cycle_history_backfill"]["waiting_on_exact_index"] is True
    assert payload["cycle_history_backfill"]["exact_index_worker_id"] == (
        "cycle-history-index-maintenance"
    )
    assert payload["truth_repair_additional_database_reads"] == 0
    assert "_certification_workers" not in payload
