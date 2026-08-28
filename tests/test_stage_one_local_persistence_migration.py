from __future__ import annotations

import json

import pytest
from sqlalchemy import Column, MetaData, Table, Text, create_engine

import inefficiency_engine.postgres_local_migration as base_migration
import inefficiency_engine.stage_one_local_persistence_migration as stage_one
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.local_persistence_migration_supervisor import MIGRATION_COMMAND
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


def _cycle_source(tmp_path):
    source = create_engine(f"sqlite:///{tmp_path / 'source.sqlite3'}")
    metadata = MetaData()
    cycle = Table(
        "cycle_historical_quotes",
        metadata,
        Column("quote_id", Text, primary_key=True),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(source)
    with source.begin() as db:
        db.execute(cycle.insert(), [
            {"quote_id": "b", "payload_json": "payload-b"},
            {"quote_id": "d", "payload_json": "payload-d"},
        ])
    return source, cycle


def test_supervisor_uses_finite_stage_one_snapshot_entrypoint():
    assert MIGRATION_COMMAND[-1] == "inefficiency_engine.stage_one_local_persistence_migration"


def test_stage_one_snapshot_does_not_chase_hash_keys_appended_after_capture(tmp_path, monkeypatch):
    source, cycle = _cycle_source(tmp_path)
    target = EvidenceStore(tmp_path / "target.sqlite3")
    history = PartitionedMarketHistory(tmp_path / "history")
    progress = tmp_path / "progress.json"

    monkeypatch.setattr(
        base_migration,
        "_migrate_resumable_append_only_table",
        stage_one._migrate_captured_append_only_table,
    )

    with pytest.raises(InterruptedError):
        base_migration.migrate_engines(
            source,
            target,
            history,
            progress_path=progress,
            batch_size=1,
            interrupt_after_batches=1,
        )

    interrupted = json.loads(progress.read_text())
    cycle_progress = interrupted["tables"]["cycle_historical_quotes"]
    assert cycle_progress["migration_mode"] == stage_one.SNAPSHOT_MIGRATION_MODE
    assert cycle_progress["snapshot_row_count"] == 2
    assert cycle_progress["snapshot_rows_copied"] == 1
    assert cycle_progress["snapshot_manifest_sha256"]

    # This key sorts behind the durable copy checkpoint. The old implementation
    # repeatedly reset and chased it. The repaired stage-one snapshot must remain the
    # already-captured {b, d}; the later row belongs to the final quiesced catch-up.
    with source.begin() as db:
        db.execute(cycle.insert(), {"quote_id": "a", "payload_json": "payload-a"})

    result = base_migration.migrate_engines(
        source,
        target,
        history,
        progress_path=progress,
        batch_size=1,
    )
    table_result = result["tables"]["cycle_historical_quotes"]
    assert result["state"] == "verified"
    assert table_result["verified"] is True
    assert table_result["verification_scope"] == stage_one.SNAPSHOT_VERIFICATION_SCOPE
    assert table_result["snapshot_row_count"] == 2
    assert table_result["snapshot_rows_copied"] == 2
    assert table_result["snapshot_rows_verified"] == 2
    assert table_result["source_rows"] == table_result["verified_rows"] == 2

    with target.engine.connect() as db:
        rows = db.exec_driver_sql(
            "SELECT quote_id, payload_json FROM cycle_historical_quotes ORDER BY quote_id"
        ).all()
    assert rows == [("b", "payload-b"), ("d", "payload-d")]


def test_stage_one_replaces_legacy_live_reconciliation_progress(tmp_path, monkeypatch):
    source, _cycle = _cycle_source(tmp_path)
    target = EvidenceStore(tmp_path / "target.sqlite3")
    history = PartitionedMarketHistory(tmp_path / "history")
    progress = tmp_path / "progress.json"

    # Bootstrap the reflected target table once, then seed the exact durable state
    # shape production had while stuck at 21/22.
    source_metadata = MetaData()
    source_metadata.reflect(source)
    base_migration.bootstrap_local_schema_from_source(source_metadata, target.engine)
    target_metadata = MetaData()
    target_metadata.reflect(target.engine)
    with target.engine.begin() as db:
        db.execute(
            target_metadata.tables["cycle_historical_quotes"].insert(),
            {"quote_id": "b", "payload_json": "payload-b"},
        )

    progress.write_text(json.dumps({
        "state": "running",
        "started_at": "2026-08-28T01:52:03+00:00",
        "current_table": "cycle_historical_quotes",
        "tables": {
            "cycle_historical_quotes": {
                "destination": "sqlite",
                "verified": False,
                "migration_mode": "resumable_append_only_reconciliation",
                "verification_scope": "append_only_exact_content_reconciliation",
                "last_primary_key": ["b"],
                "reconciliation_pass": 4,
                "source_rows_observed": 2,
                "target_rows_observed": 1,
            }
        },
    }))

    monkeypatch.setattr(
        base_migration,
        "_migrate_resumable_append_only_table",
        stage_one._migrate_captured_append_only_table,
    )
    result = base_migration.migrate_engines(
        source,
        target,
        history,
        progress_path=progress,
        batch_size=1,
    )

    table_result = result["tables"]["cycle_historical_quotes"]
    assert result["state"] == "verified"
    assert table_result["migration_mode"] == stage_one.SNAPSHOT_MIGRATION_MODE
    assert table_result["verification_scope"] == stage_one.SNAPSHOT_VERIFICATION_SCOPE
    assert table_result["snapshot_row_count"] == 2
    assert table_result["verified"] is True
    assert "reconciliation_pass" not in table_result


def test_public_status_exposes_cycle_snapshot_progress(tmp_path, monkeypatch):
    import inefficiency_engine.local_persistence_migration_supervisor as supervisor

    migration = tmp_path / "migration"
    migration.mkdir()
    paths = (
        migration / "postgres-import-supervisor.json",
        migration / "postgres-import-progress.json",
        migration / "postgres-import.lock",
        migration / "postgres-import.stdout.log",
        migration / "postgres-import.stderr.log",
    )
    paths[0].write_text(json.dumps({"state": "running", "attempt": 1}))
    paths[1].write_text(json.dumps({
        "state": "running",
        "current_table": "cycle_historical_quotes",
        "tables": {
            "cycle_historical_quotes": {
                "verified": False,
                "migration_mode": stage_one.SNAPSHOT_MIGRATION_MODE,
                "verification_scope": stage_one.SNAPSHOT_VERIFICATION_SCOPE,
                "snapshot_row_count": 100,
                "snapshot_rows_copied": 60,
                "snapshot_rows_verified": 0,
                "snapshot_captured_at": "2026-08-28T05:00:00+00:00",
                "last_progress_at": "2026-08-28T05:02:00+00:00",
            }
        },
    }))
    monkeypatch.setattr(supervisor, "_storage_root_state", lambda: (True, "ready"))
    monkeypatch.setattr(supervisor, "_paths", lambda: paths)

    payload = supervisor.migration_status_payload()
    cycle = payload["cycle_historical_quotes"]
    assert cycle["migration_mode"] == stage_one.SNAPSHOT_MIGRATION_MODE
    assert cycle["snapshot_row_count"] == 100
    assert cycle["snapshot_rows_copied"] == 60
    assert cycle["snapshot_rows_verified"] == 0
