from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, create_engine, insert

from inefficiency_engine import cycle_history_exact_index_direct as direct_index
from inefficiency_engine import cycle_history_index_maintenance_child as index_child
from inefficiency_engine import cycle_history_index_runtime_store as runtime_store
from inefficiency_engine import cycle_history_index_supervisor_probe as index_probe
from inefficiency_engine import source_coverage_history_migration_child as source_child
from inefficiency_engine import source_coverage_history_migration_supervisor as source_supervisor


def _lane(index: int) -> dict[str, object]:
    return {
        "lane_id": f"lane-{index}",
        "name": f"Lane {index}",
        "required_evidence_classes": [],
        "covered_evidence_classes": [],
        "missing_evidence_classes": [],
        "healthy_source_count": 1,
        "independent_authoritative_source_count": 1,
        "source_redundancy_satisfied": True,
        "evidence_class_coverage_satisfied": True,
        "source_layer_sufficient": True,
        "source_state": "sufficient",
        "sources": [],
    }


def _source_snapshot_payload() -> str:
    snapshot = {
        "observed_at": "2026-08-27T20:00:00+00:00",
        "lane_count": 13,
        "sufficient_lane_count": 9,
        "insufficient_lane_count": 4,
        "research_eligible_lane_count": 13,
        "forward_test_eligible_lane_count": 9,
        "allocation_source_qualified_lane_count": 9,
        "priority_order": [f"lane-{index}" for index in range(13)],
        "lanes": [_lane(index) for index in range(13)],
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
    return json.dumps({"detail": {"snapshot": snapshot}})


def test_compact_source_history_summary_uses_latest_indexed_snapshot_not_archive_counts() -> None:
    engine = create_engine("sqlite://")
    metadata = MetaData()
    heartbeats = Table(
        "worker_heartbeats",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("worker_id", String, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(engine)
    with engine.begin() as db:
        db.execute(
            insert(heartbeats),
            {
                "id": 101,
                "worker_id": "canonical-source-coverage-snapshot",
                "payload_json": _source_snapshot_payload(),
            },
        )

    summary = source_child._completed_history_summary(
        SimpleNamespace(engine=engine),
        checkpoint_heartbeat_id=101,
    )

    assert summary["compact_certification_summary"] is True
    assert summary["migration_checkpoint_covers_summary"] is True
    assert summary["lane_count"] == 13
    assert summary["summary_heartbeat_id"] == 101
    assert summary["archive_snapshot_count_deferred"] is True
    assert summary["snapshot_count"] == 0

    source = inspect.getsource(source_child._completed_history_summary)
    assert "COUNT(*)" not in source
    assert "COUNT(DISTINCT" not in source
    assert "ORDER BY id DESC LIMIT 1" in source


def test_compact_source_history_summary_fails_closed_when_tail_advances() -> None:
    engine = create_engine("sqlite://")
    metadata = MetaData()
    heartbeats = Table(
        "worker_heartbeats",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("worker_id", String, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(engine)
    with engine.begin() as db:
        db.execute(
            insert(heartbeats),
            {
                "id": 202,
                "worker_id": "canonical-source-coverage-snapshot",
                "payload_json": _source_snapshot_payload(),
            },
        )

    summary = source_child._completed_history_summary(
        SimpleNamespace(engine=engine),
        checkpoint_heartbeat_id=201,
    )

    assert summary["compact_certification_summary"] is False
    assert summary["compact_summary_reason"] == "source_snapshot_tail_advanced_after_batch"
    assert summary["lane_count"] == 13
    assert summary["migration_checkpoint_covers_summary"] is False


def test_exact_index_runtime_store_never_bootstraps_schema() -> None:
    source = inspect.getsource(runtime_store)
    assert "metadata.create_all" not in source
    assert "MetaData(" not in source
    assert runtime_store.EXACT_INDEX_CONNECT_TIMEOUT_SECONDS == 8
    assert runtime_store.EXACT_INDEX_HEARTBEAT_STATEMENT_TIMEOUT_MS == 8_000
    assert runtime_store.EXACT_INDEX_HEARTBEAT_LOCK_TIMEOUT_MS == 3_000

    child_source = inspect.getsource(index_child)
    probe_source = inspect.getsource(index_probe)
    assert "build_cycle_history_index_runtime_store" in child_source
    assert "build_cycle_history_index_runtime_store" in probe_source
    assert "from inefficiency_engine.evidence import build_evidence_store" not in child_source
    assert "from inefficiency_engine.evidence import build_evidence_store" not in probe_source


def test_direct_exact_index_has_bounded_preddl_then_unchanged_long_ddl(monkeypatch) -> None:
    configured_timeouts: list[int] = []
    statements: list[str] = []
    phases: list[str] = []

    class DB:
        dialect = SimpleNamespace(name="postgresql")

        def execute(self, statement, _params=None):
            statements.append(str(statement))
            return SimpleNamespace()

    db = DB()

    class Connection:
        def execution_options(self, **_kwargs):
            return self

        def __enter__(self):
            return db

        def __exit__(self, *_args):
            return False

    store = SimpleNamespace(
        engine=SimpleNamespace(
            dialect=SimpleNamespace(name="postgresql"),
            connect=lambda: Connection(),
        )
    )

    monkeypatch.setattr(
        direct_index,
        "_postgres_table_columns",
        lambda _db, *, table_name: set(direct_index.EXACT_INDEX_COLUMNS),
    )
    states = iter([None, {"valid": True, "ready": True}])
    monkeypatch.setattr(
        direct_index.rim,
        "_postgres_index_state",
        lambda _db, *, index_name: next(states),
    )
    monkeypatch.setattr(
        direct_index.rim,
        "_configure_postgres_index_deadlines",
        lambda _db, *, statement_timeout_ms: configured_timeouts.append(statement_timeout_ms),
    )
    monkeypatch.setattr(
        direct_index.rim,
        "_create_index_sql",
        lambda **_kwargs: "CREATE INDEX CONCURRENTLY exact_test ON market_quotes (venue,asset,observed_at,id)",
    )

    result = direct_index.ensure_exact_cycle_history_index_direct(
        store,
        statement_timeout_ms=index_child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS,
        progress=lambda row: phases.append(str(row.get("phase"))),
    )

    assert result["complete"] is True
    assert result["direct_exact_index_path"] is True
    assert configured_timeouts == [
        direct_index.EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
        3_600_000,
        direct_index.EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
    ]
    assert phases == ["preddl_starting", "preddl_complete", "ddl_starting", "complete"]
    assert any("CREATE INDEX CONCURRENTLY" in statement for statement in statements)

    direct_source = inspect.getsource(direct_index.ensure_exact_cycle_history_index_direct)
    assert "inspect(" not in direct_source
    assert "get_table_names" not in direct_source
    assert "get_columns" not in direct_source


def test_repair_preserves_all_deadline_and_authority_boundaries() -> None:
    assert index_child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS == 3_600_000
    assert direct_index.EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS == 8_000
    assert source_supervisor.MIGRATION_EXECUTOR_DEADLINE_SECONDS == 30.0
    assert source_child.DEFAULT_CHILD_MIGRATION_BATCH == 50

    source = inspect.getsource(source_child)
    assert '"qualification_authority": False' in source
    assert '"allocation_authority": False' in source
    assert '"live_execution_authority": False' in source
    assert '"paper_only": True' in source
