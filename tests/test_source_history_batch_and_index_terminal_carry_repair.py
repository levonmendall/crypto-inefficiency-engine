from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, create_engine, insert

from inefficiency_engine import cycle_history_index_maintenance_child as index_child
from inefficiency_engine import cycle_history_index_supervisor_probe as index_probe
from inefficiency_engine import source_coverage_history_batch_repair as batch_repair
from inefficiency_engine import source_coverage_history_migration_child as migration_child
from inefficiency_engine import source_coverage_history_migration_supervisor as migration_supervisor


def test_migration_child_initializes_schema_once_and_reuses_ledger(monkeypatch) -> None:
    records: list[dict[str, object]] = []
    ledgers: list[object] = []
    captured: dict[str, object] = {}
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
        lambda _store: {"ready": True, "reason": "canonical_index_ready"},
    )

    def fake_ledger(_store):
        ledger = object()
        ledgers.append(ledger)
        return ledger

    monkeypatch.setattr(migration_child, "SourceCoverageHistoryLedger", fake_ledger)
    monkeypatch.setattr(
        migration_child,
        "_record",
        lambda _store, *, state, detail, error_type=None: records.append(
            {"state": state, "detail": detail, "error_type": error_type}
        ),
    )

    def fake_advance(_store, *, ledger=None, max_heartbeats=None):
        captured["ledger"] = ledger
        captured["max_heartbeats"] = max_heartbeats
        return {"complete": False}

    monkeypatch.setattr(
        migration_child,
        "advance_one_history_migration_batch",
        fake_advance,
    )

    code = migration_child.main()

    assert code == migration_child.MIGRATION_INCOMPLETE_EXIT_CODE
    assert len(ledgers) == 1
    assert captured["ledger"] is ledgers[0]
    assert captured["max_heartbeats"] == migration_child.DEFAULT_CHILD_MIGRATION_BATCH
    stages = [str(row["detail"].get("stage")) for row in records]
    assert "canonical_history_schema_initializing" in stages
    assert "canonical_history_schema_ready" in stages
    assert "canonical_history_archive_batch_starting" in stages
    assert stages.index("canonical_history_schema_ready") < stages.index(
        "canonical_history_archive_batch_starting"
    )


def test_preinitialized_batch_reports_where_time_is_spent() -> None:
    engine = create_engine("sqlite://")
    metadata = MetaData()
    worker_heartbeats = Table(
        "worker_heartbeats",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("worker_id", String, nullable=False),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(engine)
    with engine.begin() as db:
        db.execute(
            insert(worker_heartbeats),
            {
                "worker_id": "canonical-source-coverage-snapshot",
                "observed_at": "2026-08-27T19:00:00+00:00",
                "payload_json": json.dumps({"detail": {}}),
            },
        )

    checkpoint: dict[str, object] = {}

    class FakeLedger:
        def migration_status(self):
            return {"checkpoint_heartbeat_id": 0, "complete": False}

        def _snapshot_rows(self, *_args, **_kwargs):
            raise AssertionError("invalid heartbeat must not synthesize history")

        def _insert_missing_rows(self, _db, rows):
            assert rows == []
            return 0

        def _upsert_migration_checkpoint(
            self,
            _db,
            *,
            checkpoint_heartbeat_id,
            complete,
            updated_at,
        ):
            checkpoint.update(
                {
                    "id": checkpoint_heartbeat_id,
                    "complete": complete,
                    "updated_at": updated_at,
                }
            )

    store = SimpleNamespace(engine=engine, worker_heartbeats=worker_heartbeats)
    phases: list[tuple[str, dict[str, object]]] = []
    result = batch_repair.migrate_source_coverage_history_batch_with_ledger(
        store,
        ledger=FakeLedger(),
        max_heartbeats=50,
        progress=lambda stage, detail: phases.append((stage, detail)),
    )

    assert result["complete"] is True
    assert result["checkpoint_heartbeat_id"] == 1
    assert result["invalid_heartbeats"] == 1
    assert checkpoint["id"] == 1
    assert result["schema_initialized_outside_archive_batch"] is True
    assert result["preinitialized_ledger_reused"] is True
    phase_names = [stage for stage, _detail in phases]
    assert phase_names == [
        "canonical_history_checkpoint_read_complete",
        "canonical_history_heartbeat_query_complete",
        "canonical_history_payload_parse_complete",
        "canonical_history_history_write_starting",
        "canonical_history_checkpoint_commit_complete",
    ]
    timings = result["batch_phase_timings_seconds"]
    assert "checkpoint_read" in timings
    assert "heartbeat_query" in timings
    assert "payload_parse" in timings
    assert "history_insert" in timings
    assert "checkpoint_upsert" in timings
    assert "history_transaction_commit" in timings
    assert "batch_total" in timings


def test_batch_helper_never_reconstructs_history_ledger() -> None:
    source = inspect.getsource(
        batch_repair.migrate_source_coverage_history_batch_with_ledger
    )
    assert "SourceCoverageHistoryLedger(" not in source
    assert "metadata.create_all" not in source


def test_next_index_attempt_carries_concrete_prior_child_terminal_truth() -> None:
    previous = SimpleNamespace(
        state="degraded",
        error_type="IndexChildExitedNonZero",
        detail={
            "stage": "cycle_history_index_child_retry_pending",
            "attempt_number": 253,
            "child_terminal_stage": "cycle_history_index_retry_pending",
            "child_sql_error_type": "OperationalError",
            "child_sql_error_message": "canceling statement due to lock timeout",
            "child_return_code": 77,
            "child_timed_out": False,
            "termination_signal": "SIGTERM",
            "termination_signal_number": 15,
            "possible_oom_or_external_kill": False,
            "oom_kill_proven": False,
        },
    )
    store = SimpleNamespace(latest_worker_heartbeat=lambda _worker_id: previous)

    attempt, context = index_child._previous_attempt_context(store)

    assert attempt == 254
    assert context["previous_attempt_number"] == 253
    assert context["previous_error_type"] == "IndexChildExitedNonZero"
    assert context["previous_child_terminal_stage"] == "cycle_history_index_retry_pending"
    assert context["previous_child_sql_error_type"] == "OperationalError"
    assert context["previous_child_sql_error_message"] == (
        "canceling statement due to lock timeout"
    )
    assert context["previous_child_return_code"] == 77
    assert context["previous_child_timed_out"] is False
    assert context["previous_termination_signal"] == "SIGTERM"
    assert context["previous_possible_oom_or_external_kill"] is False
    assert context["previous_oom_kill_proven"] is False

    carried = index_probe._carry({"attempt_number": attempt, **context})
    for key, value in context.items():
        assert carried[key] == value


def test_repair_keeps_existing_deadlines_and_authority_boundaries() -> None:
    assert migration_supervisor.MIGRATION_EXECUTOR_DEADLINE_SECONDS == 30.0
    assert migration_child.DEFAULT_CHILD_MIGRATION_BATCH == 50
    assert index_child.DEDICATED_CYCLE_HISTORY_INDEX_STATEMENT_TIMEOUT_MS == 3_600_000

    batch_source = inspect.getsource(batch_repair)
    assert '"paper_only": True' in batch_source
    assert '"qualification_authority": False' in batch_source
    assert '"allocation_authority": False' in batch_source
    assert '"live_execution_authority": False' in batch_source
