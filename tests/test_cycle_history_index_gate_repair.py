from __future__ import annotations

import inspect
from contextlib import contextmanager
from types import SimpleNamespace

from inefficiency_engine import control_cycle_executor_truth_repair as control_truth
from inefficiency_engine import cycle_history_background_backfill_repair as backfill_repair
from inefficiency_engine import cycle_history_background_supervisor_repair as supervisor_repair
from inefficiency_engine import cycle_history_index_gate as index_gate
from inefficiency_engine import cycle_history_index_maintenance_child as index_child
from inefficiency_engine import read_api_cycle_history_truth_repair as api_truth
from inefficiency_engine import render_combined_postbind_lane_repair as entrypoint
from inefficiency_engine import runtime_index_maintenance


def test_non_postgres_cycle_history_index_gate_is_not_a_production_ddl_requirement():
    store = SimpleNamespace(engine=SimpleNamespace(dialect=SimpleNamespace(name="sqlite")))

    status = index_gate.cycle_history_exact_index_status(store)

    assert status["ready"] is True
    assert status["reason"] == "postgres_runtime_index_not_required"
    assert status["planner_usable_verified"] is False
    assert status["allocation_authority"] is False
    assert status["live_execution_authority"] is False


def test_postgres_cycle_history_index_gate_accepts_only_planner_usable_index(monkeypatch):
    @contextmanager
    def connect():
        yield object()

    store = SimpleNamespace(
        engine=SimpleNamespace(
            dialect=SimpleNamespace(name="postgresql"),
            connect=connect,
        )
    )
    monkeypatch.setattr(
        index_gate,
        "_postgres_index_state",
        lambda _db, *, index_name: {"valid": True, "ready": True},
    )

    status = index_gate.cycle_history_exact_index_status(store)

    assert status["ready"] is True
    assert status["planner_usable_verified"] is True
    assert status["effective_index_name"] == status["canonical_index_name"]


def test_backfill_start_heartbeat_preserves_last_durable_progress(monkeypatch):
    progress = {
        "complete": False,
        "working_target_scan_id": "scan-working",
        "checkpoint_writes": 17,
    }
    heartbeat = SimpleNamespace(
        detail={"stage": "backfill_batch_checkpointed", "progress": progress},
        error_type=None,
    )
    store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: heartbeat,
    )
    recorded = {}

    def capture(_store, **kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(backfill_repair, "_ORIGINAL_RECORD_HEARTBEAT", capture)
    backfill_repair._record_heartbeat_preserving_progress(
        store,
        state="running",
        stage="backfill_batch_starting",
        sequence=18,
        detail={"owner": "test"},
    )

    detail = recorded["detail"]
    assert detail["progress"] == progress
    assert detail["last_progress"] == progress
    assert detail["progress_is_previous_durable_checkpoint"] is True
    assert detail["previous_backfill_stage"] == "backfill_batch_checkpointed"


def test_supervisor_gives_dedicated_exact_index_a_realistic_bounded_window():
    source = inspect.getsource(
        supervisor_repair.run_cycle_history_background_supervisor
    )

    assert index_child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS == 600_000
    assert supervisor_repair.INDEX_EXECUTOR_DEADLINE_SECONDS == 630.0
    assert supervisor_repair.INDEX_COMMAND[-1] == (
        "inefficiency_engine.cycle_history_index_maintenance_child"
    )
    assert supervisor_repair.BACKFILL_COMMAND[-1] == (
        "inefficiency_engine.cycle_history_background_backfill_repair"
    )
    assert source.index("INDEX_COMMAND") < source.index("BACKFILL_COMMAND")
    assert "exact_index_ready = False" in source
    assert "return_code == INDEX_NOT_READY_EXIT_CODE" in source


def test_dedicated_index_child_restores_shared_timeout_and_preserves_attempt_context(
    monkeypatch,
):
    previous = SimpleNamespace(
        detail={
            "stage": "cycle_history_index_failed",
            "attempt_number": 4,
            "message": "canceling statement due to statement timeout",
            "current_index": "ix_runtime_market_quotes_venue_asset_observed_at_id_v4",
        },
        error_type="OperationalError",
    )
    recorded: list[dict[str, object]] = []
    store = SimpleNamespace(
        latest_worker_heartbeat=lambda _worker_id: previous,
        record_worker_heartbeat=lambda **kwargs: recorded.append(kwargs),
    )
    statuses = iter(
        [
            {"ready": False, "effective_index_name": None},
            {
                "ready": True,
                "effective_index_name": (
                    "ix_runtime_market_quotes_venue_asset_observed_at_id_v5"
                ),
            },
        ]
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        index_child,
        "Settings",
        SimpleNamespace(
            from_env=lambda: SimpleNamespace(evidence_db_path="test-durable-store")
        ),
    )
    monkeypatch.setattr(index_child, "build_evidence_store", lambda _path: store)
    monkeypatch.setattr(
        index_child,
        "cycle_history_exact_index_status",
        lambda _store: next(statuses),
    )

    def fake_ensure(_store, *, index_specs, progress):
        captured["timeout_ms"] = (
            runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        )
        captured["index_specs"] = index_specs
        progress(
            {
                "phase": "complete",
                "index": "ix_runtime_market_quotes_venue_asset_observed_at_id_v5",
                "effective_index_name": (
                    "ix_runtime_market_quotes_venue_asset_observed_at_id_v5"
                ),
                "table": "market_quotes",
                "runtime_seconds": 420.0,
                "ok": True,
                "concurrent": True,
            }
        )
        return {"complete": True}

    monkeypatch.setattr(
        runtime_index_maintenance,
        "ensure_runtime_indexes_after_api_bind",
        fake_ensure,
    )
    original_timeout = (
        runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )

    assert index_child.run_index_maintenance() == 0

    assert captured["timeout_ms"] == 600_000
    assert captured["index_specs"] == (
        runtime_index_maintenance.CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS
    )
    assert (
        runtime_index_maintenance.CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        == original_timeout
    )
    assert recorded
    first_detail = recorded[0]["detail"]
    assert first_detail["attempt_number"] == 5
    assert first_detail["previous_attempt_number"] == 4
    assert first_detail["previous_stage"] == "cycle_history_index_failed"
    assert first_detail["previous_error_type"] == "OperationalError"
    assert first_detail["previous_message"] == (
        "canceling statement due to statement timeout"
    )
    assert first_detail["previous_effective_index_name"].endswith("_v4")
    assert first_detail["statement_timeout_ms"] == 600_000


def test_e2e_truth_never_promotes_generic_history_to_cycle_history(monkeypatch):
    monkeypatch.setattr(
        api_truth.base,
        "end_to_end_certification_payload",
        lambda: {
            "certified": True,
            "operationally_certified": True,
            "status": "certified",
            "checks": {
                "database_ready": True,
                "cycle_history_serving_target_certified": True,
            },
            "blockers": [],
            "cycle_history_backfill": {
                "available": True,
                "stale": False,
                "cache_complete": False,
                "serving_scan_id": None,
                "progress": {"working_target_scan_id": "working-scan"},
            },
            "control": {
                "cycle_history_cache_complete": True,
                "cycle_history_cache_progress": {
                    "strategy": {"cache_count": 0}
                },
            },
        },
    )
    monkeypatch.setattr(
        api_truth,
        "_raw_canonical_control_status",
        lambda: {
            "cycle_history_cache_complete": False,
            "historical_cache_progress": {
                "strategy": {
                    "cache_count": 0,
                    "cache_initialized": False,
                    "completion_state": "not_initialized_in_this_executor",
                }
            },
        },
    )
    monkeypatch.setattr(
        api_truth,
        "_cycle_history_index_maintenance_status",
        lambda: {"ready": False},
    )

    payload = api_truth.repaired_end_to_end_certification_payload()

    assert payload["checks"]["cycle_history_serving_target_certified"] is False
    assert "cycle_history_serving_target_certified" in payload["blockers"]
    assert payload["certified"] is False
    assert payload["control"]["cycle_history_cache_complete"] is False
    assert payload["control"]["cycle_history_cache_progress"] == {
        "working_target_scan_id": "working-scan"
    }
    assert payload["control"]["cycle_history_cache_progress_source"] == (
        "background_backfill"
    )
    assert payload["control"]["historical_cache_progress"]["strategy"][
        "cache_count"
    ] == 0
    assert payload["duplicate_readiness_read_disabled"] is True


def test_e2e_truth_wrapper_does_not_repeat_full_deployment_readiness():
    source = inspect.getsource(api_truth.repaired_end_to_end_certification_payload)

    assert "deployment_readiness" not in source
    assert "_raw_canonical_control_status" in source
    assert api_truth.CANONICAL_CONTROL_WORKER_ID == "canonical-control-operating-loop"


def test_disposable_strategy_cache_reports_uninitialized_not_failed(monkeypatch):
    monkeypatch.setattr(
        control_truth,
        "_ORIGINAL_CACHE_STATUS",
        lambda: {
            "complete": False,
            "strategy": {
                "cache_count": 0,
                "all_caches_complete": False,
                "caches": [],
            },
            "outcomes": {"all_caches_complete": True},
        },
    )

    status = control_truth._truthful_cache_status()

    assert status["complete"] is False
    assert status["strategy_cache_initialized"] is False
    assert status["strategy"]["all_caches_complete"] is None
    assert status["strategy"]["completion_state"] == (
        "not_initialized_in_this_executor"
    )
    assert status["strategy"]["uninitialized_is_not_durable_cache_failure"] is True


def test_production_entrypoint_wires_repaired_history_and_control_executor():
    assert entrypoint.BOUNDED_HEARTBEAT_API_APP == (
        "inefficiency_engine.read_api_liveness_deploy:app"
    )
    assert entrypoint.CONTROL_TRUTH_COMMAND[-1] == (
        "inefficiency_engine.permanent_control_worker_truth_repair"
    )
    source = inspect.getsource(entrypoint.main)
    assert "install_control_truth_command()" in source
    assert "run_cycle_history_background_supervisor" in source
