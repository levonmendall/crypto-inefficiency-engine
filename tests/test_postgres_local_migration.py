from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import inspect, insert

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
            scan_id=f"scan-{index", started_at=observed, completed_at=observed,
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
