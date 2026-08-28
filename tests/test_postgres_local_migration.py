from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Column, MetaData, Table, Text, create_engine, inspect, insert
from sqlalchemy.exc import OperationalError

from inefficiency_engine import postgres_local_migration as migration_module
from inefficiency_engine.durable_control_cache import ensure_durable_control_cache_schema
from inefficiency_engine.durable_control_cycle_history import ensure_durable_control_cycle_history_schema
from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory
from inefficiency_engine.postgres_local_migration import migrate_engines
from inefficiency_engine.source_coverage_history import SourceCoverageHistoryLedger


def _quote(index: int) -> MarketQuote:
    return MarketQuote(
        venue="coinbase", asset="BTC", market_kind=MarketKind.SPOT,
        symbol="BTC-USD", quote_currency="USD", contract_key="spot-reference",
        mid=50_000 + index,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
        source=f"migration-test:{index}",
    )


def _stores(tmp_path):
    source = EvidenceStore(tmp_path / "source.sqlite3")
    target = EvidenceStore(tmp_path / "target.sqlite3")
    source_history = SourceCoverageHistoryLedger(source)
    ensure_durable_control_cache_schema(source)
    ensure_durable_control_cycle_history_schema(source)
    with source.engine.begin() as db:
        db.execute(
            insert(source_history.migrations),
            {
                "migration_name": "production-schema-test",
                "checkpoint_heartbeat_id": 42,
                "complete": True,
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
        )
    for index in range(3):
        observed = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index)
        source.record_scan(
            scan_id=f"scan-{index}", started_at=observed, completed_at=observed,
            providers=[ProviderStatus(provider="test", ok=True, observed_at=observed)],
            funding_quotes=[], market_quotes=[_quote(index)], opportunities=[],
        )
    duplicate_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    source.record_scan(
        scan_id="scan-duplicate", started_at=duplicate_at, completed_at=duplicate_at,
        providers=[], funding_quotes=[], market_quotes=[_quote(2)], opportunities=[],
    )
    return source, target, PartitionedMarketHistory(tmp_path / "history")


def test_migration_is_idempotent_and_proves_market_lineage_equivalence(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    first = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    second = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    assert first["state"] == second["state"] == "verified"
    market = second["tables"]["market_quotes"]
    assert market["source_rows"] == 4
    assert market["source_lineage_count"] == 3
    assert market["destination_inventory"]["lineage_count"] == 3
    assert market["destination_inventory"]["valid"] is True
    assert second["forward_evidence_granted"] is False
    target_tables = set(inspect(target.engine).get_table_names())
    assert {
        "source_coverage_history",
        "source_coverage_history_migrations",
        "control_evidence_cache_checkpoints",
        "control_cycle_history_rows",
    } <= target_tables
    with target.engine.connect() as db:
        copied = db.exec_driver_sql(
            "SELECT checkpoint_heartbeat_id FROM source_coverage_history_migrations "
            "WHERE migration_name = 'production-schema-test'"
        ).scalar_one()
    assert copied == 42


def test_interrupted_migration_resumes_from_durable_primary_key_checkpoint(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    with pytest.raises(InterruptedError):
        migrate_engines(
            source.engine, target, history, progress_path=progress,
            batch_size=1, interrupt_after_batches=1,
        )
    interrupted = json.loads(progress.read_text())
    checkpoints = [value.get("last_primary_key") for value in interrupted["tables"].values()]
    assert any(checkpoint for checkpoint in checkpoints)
    result = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    assert result["state"] == "verified"
    assert result["resumed_at"] is not None
    assert result["tables"]["market_quotes"]["last_primary_key"] == [4]


def test_retry_does_not_revalidate_verified_mutable_table_against_new_source_state(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    first = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    table_name = "source_coverage_history_migrations"
    first_table = first["tables"][table_name]

    # Production checkpoint tables update rows in place under stable primary keys.
    # A later retry must retain the already-proven target snapshot instead of pairing
    # its end checkpoint with a newly-mutated source row and reporting a false digest
    # mismatch, which is the race observed in stage-one production migration.
    with source.engine.begin() as db:
        db.exec_driver_sql(
            "UPDATE source_coverage_history_migrations "
            "SET checkpoint_heartbeat_id = 99, updated_at = '2026-01-02T00:00:00+00:00' "
            "WHERE migration_name = 'production-schema-test'"
        )
    failed_retry = json.loads(progress.read_text())
    failed_retry.update(
        state="failed",
        error_type="RuntimeError",
        error="injected later-table failure",
        completed_at=None,
    )
    progress.write_text(json.dumps(failed_retry))

    resumed = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    assert resumed["state"] == "verified"
    assert resumed["tables"][table_name]["row_digest"] == first_table["row_digest"]
    with target.engine.connect() as db:
        copied = db.exec_driver_sql(
            "SELECT checkpoint_heartbeat_id, updated_at "
            "FROM source_coverage_history_migrations "
            "WHERE migration_name = 'production-schema-test'"
        ).one()
    assert tuple(copied) == (42, "2026-01-01T00:00:00+00:00")


def test_migration_fails_closed_when_a_committed_partition_is_missing(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    with history._connect() as db:
        relative = db.execute("SELECT path FROM partitions ORDER BY path LIMIT 1").fetchone()[0]
    (history.root / relative).unlink()
    with pytest.raises(RuntimeError, match="verified market_quotes destination changed"):
        migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    failed = json.loads(progress.read_text())
    assert failed["state"] == "failed"
    assert failed["forward_evidence_granted"] is False


def test_cycle_history_resume_keeps_copied_rows_and_reconciles_hash_ids_inserted_behind_checkpoint(tmp_path):
    source = create_engine(f"sqlite:///{tmp_path / 'cycle-source.sqlite3'}")
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

    target = EvidenceStore(tmp_path / "cycle-target.sqlite3")
    history = PartitionedMarketHistory(tmp_path / "cycle-history")
    progress = tmp_path / "cycle-progress.json"

    with pytest.raises(InterruptedError):
        migrate_engines(
            source,
            target,
            history,
            progress_path=progress,
            batch_size=1,
            interrupt_after_batches=1,
        )

    interrupted = json.loads(progress.read_text())
    cycle_progress = interrupted["tables"]["cycle_historical_quotes"]
    assert cycle_progress["last_primary_key"] == ["b"]
    assert cycle_progress["migration_mode"] == "resumable_append_only_reconciliation"
    with target.engine.connect() as db:
        assert db.exec_driver_sql("SELECT COUNT(*) FROM cycle_historical_quotes").scalar_one() == 1

    # SHA-derived quote IDs are non-monotonic. Simulate a live append that sorts
    # behind the durable checkpoint. The resumed tail pass misses it by design, then
    # count/content reconciliation must reset to the beginning without clearing the
    # already-copied target and converge exactly.
    with source.begin() as db:
        db.execute(cycle.insert(), {"quote_id": "a", "payload_json": "payload-a"})

    result = migrate_engines(source, target, history, progress_path=progress, batch_size=1)
    table_result = result["tables"]["cycle_historical_quotes"]
    assert result["state"] == "verified"
    assert table_result["verified"] is True
    assert table_result["verification_scope"] == "append_only_exact_content_reconciliation"
    assert table_result["reconciliation_pass"] >= 2
    assert table_result["source_rows"] == table_result["verified_rows"] == 3
    with target.engine.connect() as db:
        rows = db.exec_driver_sql(
            "SELECT quote_id, payload_json FROM cycle_historical_quotes ORDER BY quote_id"
        ).all()
    assert rows == [("a", "payload-a"), ("b", "payload-b"), ("d", "payload-d")]


def test_cycle_history_source_read_retries_transient_disconnect_in_same_child(tmp_path, monkeypatch):
    source = create_engine(f"sqlite:///{tmp_path / 'retry-source.sqlite3'}")
    progress = tmp_path / "retry-progress.json"
    table_report: dict[str, object] = {}
    report: dict[str, object] = {
        "state": "running",
        "tables": {"cycle_historical_quotes": table_report},
    }
    attempts = 0
    dispose_calls = 0
    original_dispose = source.dispose

    def dispose() -> None:
        nonlocal dispose_calls
        dispose_calls += 1
        original_dispose()

    def reader():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError(
                "SELECT cycle_historical_quotes",
                {},
                RuntimeError("SSL error: unexpected eof while reading"),
            )
        return ["recovered"]

    monkeypatch.setattr(source, "dispose", dispose)
    monkeypatch.setattr(migration_module.time, "sleep", lambda _seconds: None)

    result = migration_module._source_read_with_retry(
        source,
        reader,
        table_report,
        report,
        progress,
        phase="copy_batch",
    )

    assert result == ["recovered"]
    assert attempts == 2
    assert dispose_calls == 1
    assert table_report["source_transport_retries"] == 1
    assert table_report["last_source_retry_phase"] == "copy_batch"
    assert table_report["last_source_retry_recovered"] is True
    persisted = json.loads(progress.read_text())
    persisted_cycle = persisted["tables"]["cycle_historical_quotes"]
    assert persisted_cycle["source_transport_retries"] == 1
    assert persisted_cycle["last_source_retry_recovered"] is True
