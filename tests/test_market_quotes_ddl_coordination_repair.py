from __future__ import annotations

import inspect
from types import SimpleNamespace

from inefficiency_engine import render_combined_postbind_lane_repair as lane_repair
from inefficiency_engine import runtime_index_ddl_coordination_repair as ddl_repair
from inefficiency_engine import source_coverage_history_migration_child as migration_child
from inefficiency_engine import source_coverage_history_migration_supervisor as migration_supervisor
from inefficiency_engine.worker_heartbeat_index_gate import (
    WORKER_HEARTBEAT_INDEX_COLUMNS,
    WORKER_HEARTBEAT_INDEX_TABLE,
    worker_heartbeat_priority_index_status,
)


def test_market_quotes_is_excluded_from_unrelated_runtime_index_round() -> None:
    unrelated = ddl_repair._non_market_quotes_specs()
    market = ddl_repair._market_quotes_specs()

    assert ddl_repair.MARKET_QUOTES_TABLE not in unrelated
    assert market == {
        "market_quotes": ddl_repair.base.CONTROL_GATE_INDEX_SPECS["market_quotes"]
    }
    assert "worker_heartbeats" not in unrelated


def test_production_installs_ddl_coordination_before_base_main() -> None:
    source = inspect.getsource(lane_repair.main)
    assert "install_runtime_index_ddl_coordination_repair()" in source
    assert source.index("install_runtime_index_ddl_coordination_repair()") < source.index(
        "base.main()"
    )


def test_runtime_index_guard_defers_market_quotes_until_exact_index_ready() -> None:
    source = inspect.getsource(ddl_repair._repaired_runtime_index_guard)
    assert "cycle_history_exact_index_status(store)" in source
    assert "market_quotes_ddl_deferred_for_exact_index" in source
    assert source.index("if not exact_ready:") < source.index(
        "ensure_cycle_history_brin_after_api_bind"
    )
    assert "_dispose_store(store)" in source
    assert "except Exception as exc:" in source


def test_non_postgres_heartbeat_gate_is_ready_without_runtime_ddl() -> None:
    store = SimpleNamespace(
        engine=SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    )
    status = worker_heartbeat_priority_index_status(store)

    assert status["ready"] is True
    assert status["planner_usable_verified"] is False
    assert status["table"] == WORKER_HEARTBEAT_INDEX_TABLE
    assert status["columns"] == list(WORKER_HEARTBEAT_INDEX_COLUMNS)
    assert status["allocation_authority"] is False
    assert status["paper_only"] is True


def test_migration_child_waits_for_heartbeat_index_before_raw_history(monkeypatch) -> None:
    records: list[dict[str, object]] = []
    store = object()

    monkeypatch.setattr(
        migration_child.Settings,
        "from_env",
        staticmethod(lambda: SimpleNamespace(evidence_db_path="test")),
    )
    monkeypatch.setattr(migration_child, "build_evidence_store", lambda _path: store)
    monkeypatch.setattr(
        migration_child,
        "worker_heartbeat_priority_index_status",
        lambda _store: {"ready": False, "reason": "planner_usable_index_unavailable"},
    )
    monkeypatch.setattr(
        migration_child,
        "_record",
        lambda _store, *, state, detail, error_type=None: records.append(
            {"state": state, "detail": detail, "error_type": error_type}
        ),
    )
    monkeypatch.setattr(
        migration_child,
        "advance_one_history_migration_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw history migration must not start")
        ),
    )

    code = migration_child.main()

    assert code == migration_child.MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE
    assert records[-1]["detail"]["stage"] == "canonical_history_waiting_for_heartbeat_index"
    assert records[-1]["detail"]["raw_history_queries_started"] is False


def test_migration_supervisor_recognizes_prerequisite_exit_code() -> None:
    source = inspect.getsource(
        migration_supervisor.run_source_coverage_history_migration_supervisor
    )
    assert "MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE" in source
    assert "waiting for planner-usable" in source


def test_repair_preserves_existing_authority_and_timeout_boundaries() -> None:
    assert migration_supervisor.MIGRATION_EXECUTOR_DEADLINE_SECONDS == 30.0
    assert ddl_repair.base.WORKER_HEARTBEAT_PRIORITY_INDEX_STATEMENT_TIMEOUT_MS == 180_000
    assert migration_child.DEFAULT_CHILD_MIGRATION_BATCH == 50
