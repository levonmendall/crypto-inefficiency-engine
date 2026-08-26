from __future__ import annotations

import inspect

from inefficiency_engine import read_api_certification_fast_readiness as fast_readiness
from inefficiency_engine import read_api_end_to_end_certification_deploy as certification
from inefficiency_engine import read_evidence
from inefficiency_engine import render_combined_postbind_lane_repair as entrypoint
from inefficiency_engine.evidence import EvidenceStore


def test_latest_worker_heartbeats_are_batched_and_return_newest_rows(tmp_path):
    store = EvidenceStore(tmp_path / "heartbeat-batch.sqlite")
    store.record_worker_heartbeat(worker_id="worker-a", state="starting")
    store.record_worker_heartbeat(worker_id="worker-b", state="running")
    store.record_worker_heartbeat(worker_id="worker-a", state="success")

    rows = fast_readiness._latest_heartbeats(
        store,
        ["worker-a", "worker-b", "worker-a"],
    )

    assert set(rows) == {"worker-a", "worker-b"}
    assert rows["worker-a"].state == "success"
    assert rows["worker-b"].state == "running"
    source = inspect.getsource(fast_readiness._latest_heartbeats)
    assert ".where(rows.c.worker_id == worker_id)" in source
    assert ".order_by(rows.c.id.desc())" in source
    assert ".limit(1)" in source
    assert "union_all" in source
    assert "func.max" not in source
    assert "group_by" not in source
    assert "latest_worker_heartbeat" not in source


def test_certification_uses_fast_batched_readiness_module():
    assert certification.active is fast_readiness
    source = inspect.getsource(fast_readiness._runtime_heartbeats)
    assert "_latest_heartbeats" in source
    assert '"heartbeat_query_count": 1' in source
    assert "store.latest_worker_heartbeat" not in source
    assert fast_readiness.LATEST_HEARTBEAT_QUERY_STRATEGY == (
        "targeted_latest_per_worker_union"
    )


def test_worker_heartbeat_composite_index_is_priority_post_bind_and_non_authoritative():
    original_priority = dict(entrypoint.base.PRIORITY_READ_INDEX_SPECS)
    original_background = dict(entrypoint.base.BACKGROUND_INDEX_SPECS)
    try:
        entrypoint.install_worker_heartbeat_read_index()
        assert entrypoint.base.PRIORITY_READ_INDEX_SPECS["worker_heartbeats"] == (
            "worker_id",
            "id",
        )
        assert "worker_heartbeats" not in entrypoint.base.BACKGROUND_INDEX_SPECS
        assert entrypoint.WORKER_HEARTBEAT_READ_INDEX_SPEC == {
            "worker_heartbeats": ("worker_id", "id")
        }
        assert (
            entrypoint.base.WORKER_HEARTBEAT_PRIORITY_INDEX_STATEMENT_TIMEOUT_MS
            == 180_000
        )
        guard_source = inspect.getsource(entrypoint.base._runtime_index_guard)
        assert guard_source.index("post_control_priority_worker_heartbeat_read") < (
            guard_source.index("post_control_cycle_history_brin")
        )
        assert guard_source.index("post_control_priority_worker_heartbeat_read") < (
            guard_source.index("post_control_source_strategy")
        )
    finally:
        entrypoint.base.PRIORITY_READ_INDEX_SPECS.clear()
        entrypoint.base.PRIORITY_READ_INDEX_SPECS.update(original_priority)
        entrypoint.base.BACKGROUND_INDEX_SPECS.clear()
        entrypoint.base.BACKGROUND_INDEX_SPECS.update(original_background)


def test_priority_heartbeat_index_restores_shared_runtime_timeout(monkeypatch):
    captured = {}
    original_timeout = entrypoint.base.runtime_indexes.POSTGRES_INDEX_STATEMENT_TIMEOUT_MS

    def fake_ensure(store, *, index_specs, progress):
        captured["store"] = store
        captured["index_specs"] = index_specs
        captured["timeout_ms"] = (
            entrypoint.base.runtime_indexes.POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        )
        captured["progress"] = progress
        return {"complete": True}

    monkeypatch.setattr(
        entrypoint.base,
        "ensure_runtime_indexes_after_api_bind",
        fake_ensure,
    )
    marker = object()
    progress = lambda row: row

    result = entrypoint.base._ensure_priority_worker_heartbeat_index(
        marker,
        progress=progress,
    )

    assert result == {"complete": True}
    assert captured["store"] is marker
    assert captured["index_specs"] == {"worker_heartbeats": ("worker_id", "id")}
    assert captured["timeout_ms"] == 180_000
    assert captured["progress"] is progress
    helper_source = inspect.getsource(entrypoint.base._ensure_priority_worker_heartbeat_index)
    assert "finally:" in helper_source
    assert (
        entrypoint.base.runtime_indexes.POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        == original_timeout
    )


def test_batched_read_failure_preserves_database_error_text(monkeypatch):
    class FakeStore:
        pass

    monkeypatch.setattr(fast_readiness, "_store", lambda: FakeStore())

    def fail(_store, _worker_ids):
        raise RuntimeError("canceling statement due to statement timeout")

    monkeypatch.setattr(fast_readiness, "_latest_heartbeats", fail)

    payload = fast_readiness._runtime_heartbeats()

    assert payload["heartbeat_query_failed"] is True
    assert payload["batch_error"]["error_type"] == "RuntimeError"
    assert "statement timeout" in payload["batch_error"]["message"]
    workers = payload["workers"]
    assert workers
    first = next(iter(workers.values()))
    assert first["available"] is False
    assert "statement timeout" in first["error_message"]
    assert first["heartbeat_query_strategy"] == (
        "targeted_latest_per_worker_union"
    )


def test_read_only_postgres_queries_have_server_side_deadlines():
    assert read_evidence.READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS == 2_500
    assert read_evidence.READ_ONLY_POSTGRES_LOCK_TIMEOUT_MS == 1_000
    source = inspect.getsource(read_evidence.ReadOnlyEvidenceStore.__init__)
    assert '"options"' in source
    assert "statement_timeout=" in source
    assert "lock_timeout=" in source
    assert "connect_timeout" in source


def test_certification_http_deadline_remains_stricter_final_safety_net():
    from inefficiency_engine import read_api_liveness_deploy

    assert read_evidence.READ_ONLY_POSTGRES_STATEMENT_TIMEOUT_MS / 1000.0 < (
        read_api_liveness_deploy.END_TO_END_CERTIFICATION_DEADLINE_SECONDS
    )
