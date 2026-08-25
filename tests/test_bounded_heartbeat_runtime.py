from __future__ import annotations

import inspect

from inefficiency_engine import render_combined_postbind_lane_repair
from inefficiency_engine.bounded_heartbeat_runtime import (
    bounded_heartbeat_payloads,
)
from inefficiency_engine.evidence import EvidenceStore, WorkerHeartbeat
from inefficiency_engine.runtime_index_maintenance import (
    BACKGROUND_INDEX_SPECS,
    CONTROL_GATE_INDEX_SPECS,
)


def _heartbeat(store: EvidenceStore, worker_id: str, state: str = "success") -> None:
    store.record_worker_heartbeat(
        worker_id=worker_id,
        state=state,
        detail={"test": True},
    )


def test_bounded_heartbeat_tail_returns_latest_matching_worker(tmp_path):
    store = EvidenceStore(tmp_path / "heartbeat-tail.sqlite")
    _heartbeat(store, "target", "running")
    for index in range(8):
        _heartbeat(store, f"noise-{index}")
    _heartbeat(store, "target", "success")

    with store.engine.connect() as db:
        payloads = bounded_heartbeat_payloads(
            db,
            worker_id="target",
            limit=1,
            window_rows=5,
        )

    assert len(payloads) == 1
    heartbeat = WorkerHeartbeat.model_validate_json(payloads[0])
    assert heartbeat.worker_id == "target"
    assert heartbeat.state == "success"


def test_bounded_heartbeat_tail_fails_closed_when_worker_is_outside_window(tmp_path):
    store = EvidenceStore(tmp_path / "heartbeat-fail-closed.sqlite")
    _heartbeat(store, "target")
    for index in range(6):
        _heartbeat(store, f"noise-{index}")

    with store.engine.connect() as db:
        payloads = bounded_heartbeat_payloads(
            db,
            worker_id="target",
            limit=1,
            window_rows=3,
        )

    assert payloads == []


def test_heartbeat_repair_requires_no_new_runtime_index_ddl():
    assert "worker_heartbeats" not in CONTROL_GATE_INDEX_SPECS
    assert "worker_heartbeats" not in BACKGROUND_INDEX_SPECS


def test_lane_repair_routes_api_and_portfolio_through_bounded_read_wrappers():
    source = inspect.getsource(
        render_combined_postbind_lane_repair.install_source_repair_child_command
    )

    assert 'commands["source"] = list(SOURCE_REPAIR_COMMAND)' in source
    assert 'commands["portfolio"] = list(PORTFOLIO_BOUNDED_HEARTBEAT_COMMAND)' in source
    assert 'BOUNDED_HEARTBEAT_API_APP' in source
    assert "inefficiency_engine.read_api_liveness_deploy:app" == (
        render_combined_postbind_lane_repair.BOUNDED_HEARTBEAT_API_APP
    )
