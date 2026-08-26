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
    assert "func.max(rows.c.id)" in source
    assert ".group_by(rows.c.worker_id)" in source
    assert "latest_worker_heartbeat" not in source


def test_certification_uses_fast_batched_readiness_module():
    assert certification.active is fast_readiness
    source = inspect.getsource(fast_readiness._runtime_heartbeats)
    assert "_latest_heartbeats" in source
    assert '"heartbeat_query_count": 1' in source
    assert "store.latest_worker_heartbeat" not in source


def test_worker_heartbeat_composite_index_is_deferred_and_non_authoritative():
    original = dict(entrypoint.base.BACKGROUND_INDEX_SPECS)
    try:
        entrypoint.install_worker_heartbeat_read_index()
        assert entrypoint.base.BACKGROUND_INDEX_SPECS["worker_heartbeats"] == (
            "worker_id",
            "id",
        )
        assert entrypoint.WORKER_HEARTBEAT_READ_INDEX_SPEC == {
            "worker_heartbeats": ("worker_id", "id")
        }
        source = inspect.getsource(entrypoint.main)
        assert source.index("install_worker_heartbeat_read_index()") < source.index(
            "return base.main()"
        )
    finally:
        entrypoint.base.BACKGROUND_INDEX_SPECS.clear()
        entrypoint.base.BACKGROUND_INDEX_SPECS.update(original)


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
