from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

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


def test_dedicated_index_heartbeat_is_exposed_as_single_owner(monkeypatch):
    now = datetime.now(timezone.utc)
    heartbeat = SimpleNamespace(
        observed_at=now,
        state="success",
        error_type=None,
        detail={
            "stage": "cycle_history_index_ready",
            "index_status": {
                "ready": True,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": "ix_cycle_history_v2",
                "planner_usable_verified": True,
                "reason": "replacement_index_ready",
            },
            "maintenance_result": {"complete": True},
        },
    )
    store = SimpleNamespace(
        latest_worker_heartbeat=lambda worker_id: (
            heartbeat
            if worker_id == api_truth.CYCLE_HISTORY_INDEX_WORKER_ID
            else None
        )
    )
    monkeypatch.setattr(api_truth.base.active, "_store", lambda: store)

    status = api_truth._cycle_history_index_maintenance_status()

    assert status["available"] is True
    assert status["ready"] is True
    assert status["stale"] is False
    assert status["single_owner"] is True
    assert status["effective_index_name"] == "ix_cycle_history_v2"
    assert status["generic_runtime_exact_index_maintenance_disabled"] is True
    assert status["certification_authority"] is False


def test_e2e_payload_surfaces_dedicated_index_owner(monkeypatch):
    heartbeat = SimpleNamespace(
        observed_at=datetime.now(timezone.utc),
        state="running",
        error_type=None,
        detail={
            "stage": "cycle_history_index_maintenance_starting",
            "index_status": {
                "ready": False,
                "canonical_index_name": "ix_cycle_history",
                "effective_index_name": None,
                "planner_usable_verified": True,
                "reason": "planner_usable_index_unavailable",
            },
        },
    )
    store = SimpleNamespace(latest_worker_heartbeat=lambda _worker_id: heartbeat)
    monkeypatch.setattr(api_truth.base.active, "_store", lambda: store)
    monkeypatch.setattr(
        api_truth.base.active,
        "deployment_readiness",
        lambda: {"runtime_heartbeats": {"workers": {"canonical_control": {}}}},
    )
    monkeypatch.setattr(
        api_truth.base,
        "end_to_end_certification_payload",
        lambda: {
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
        },
    )

    payload = api_truth.repaired_end_to_end_certification_payload()

    assert payload["cycle_history_exact_index_single_owner"] is True
    assert payload["cycle_history_exact_index_owner"] == (
        "cycle-history-index-maintenance"
    )
    assert payload["cycle_history_index_maintenance"]["state"] == "running"
    assert payload["cycle_history_index_maintenance"]["ready"] is False
    assert payload["cycle_history_backfill"]["waiting_on_exact_index"] is True
    assert payload["cycle_history_backfill"]["exact_index_worker_id"] == (
        "cycle-history-index-maintenance"
    )
