from __future__ import annotations

import json

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine
from sqlalchemy.exc import OperationalError

import inefficiency_engine.postgres_local_migration as base
import inefficiency_engine.stage_one_monotonic_append_only as monotonic


def _source_and_target(tmp_path):
    source = create_engine(f"sqlite:///{tmp_path / 'source.sqlite3'}")
    target = create_engine(f"sqlite:///{tmp_path / 'target.sqlite3'}")

    source_metadata = MetaData()
    source_table = Table(
        "source_event_observations",
        source_metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("event_id", Text, nullable=False, unique=True),
        Column("payload_json", Text, nullable=False),
    )
    source_metadata.create_all(source)

    target_metadata = MetaData()
    target_table = Table(
        "source_event_observations",
        target_metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("event_id", Text, nullable=False, unique=True),
        Column("payload_json", Text, nullable=False),
    )
    target_metadata.create_all(target)

    with source.begin() as db:
        db.execute(
            source_table.insert(),
            [
                {"id": 1, "event_id": "event-1", "payload_json": "payload-1"},
                {"id": 3, "event_id": "event-3", "payload_json": "payload-3"},
                {"id": 5, "event_id": "event-5", "payload_json": "payload-5"},
            ],
        )
    return source, target, source_table, target_table


def _report():
    table_report: dict[str, object] = {
        "verified": False,
        "migration_mode": "captured_primary_key_membership_manifest",
        "snapshot_capture_retries": 3,
        "source_transport_retries": 2,
    }
    report: dict[str, object] = {
        "state": "running",
        "current_table": "source_event_observations",
        "tables": {"source_event_observations": table_report},
    }
    return report, table_report


def test_monotonic_high_water_resume_does_not_chase_post_capture_rows(tmp_path):
    source, target, source_table, target_table = _source_and_target(tmp_path)
    report, table_report = _report()
    progress = tmp_path / "progress.json"
    shared = ["id", "event_id", "payload_json"]

    with pytest.raises(InterruptedError):
        monotonic.migrate_monotonic_integer_append_only_table(
            source,
            target,
            source_table,
            target_table,
            shared,
            table_report,
            report,
            progress,
            batch_size=2,
            completed_batches=0,
            interrupt_after_batches=1,
        )

    interrupted = json.loads(progress.read_text())["tables"]["source_event_observations"]
    assert interrupted["migration_mode"] == monotonic.MIGRATION_MODE
    assert interrupted["snapshot_high_water_primary_key"] == [5]
    assert interrupted["snapshot_high_water_captured"] is True
    assert interrupted["snapshot_rows_copied"] == 2
    assert interrupted["last_primary_key"] == [3]

    # This row is created after the durable Stage 1 boundary. It must not expand the
    # snapshot on process restart; final quiesced catch-up owns it.
    with source.begin() as db:
        db.execute(
            source_table.insert(),
            {"id": 6, "event_id": "event-6", "payload_json": "payload-6"},
        )

    monotonic.migrate_monotonic_integer_append_only_table(
        source,
        target,
        source_table,
        target_table,
        shared,
        table_report,
        report,
        progress,
        batch_size=2,
        completed_batches=0,
        interrupt_after_batches=None,
    )

    assert table_report["verified"] is True
    assert table_report["snapshot_high_water_primary_key"] == [5]
    assert table_report["high_water_primary_key"] == [5]
    assert table_report["snapshot_row_count"] == 3
    assert table_report["snapshot_rows_copied"] == 3
    assert table_report["snapshot_rows_verified"] == 3
    assert table_report["source_transport_retries"] == 2
    assert table_report["snapshot_capture_retries"] == 3

    with target.connect() as db:
        rows = db.execute(
            target_table.select().order_by(target_table.c.id)
        ).mappings().all()
    assert [row["id"] for row in rows] == [1, 3, 5]


def test_transient_batch_eof_retries_only_bounded_read_and_keeps_high_water(tmp_path, monkeypatch):
    source, target, source_table, target_table = _source_and_target(tmp_path)
    report, table_report = _report()
    progress = tmp_path / "progress.json"
    shared = ["id", "event_id", "payload_json"]

    original_capture = base._capture_high_water
    original_batch = monotonic._source_batch
    captures = 0
    calls = 0
    injected = False

    def recording_capture(engine, table):
        nonlocal captures
        captures += 1
        return original_capture(engine, table)

    def flaky_batch(*args, **kwargs):
        nonlocal calls, injected
        calls += 1
        # Fail after at least one copy batch has already completed. The retry must
        # repeat only this bounded keyset read, not recapture or restart the snapshot.
        checkpoint = kwargs.get("checkpoint")
        if checkpoint == [3] and not injected:
            injected = True
            raise OperationalError(
                "SELECT source_event_observations",
                {},
                Exception("SSL error: unexpected eof while reading"),
            )
        return original_batch(*args, **kwargs)

    monkeypatch.setattr(base, "_capture_high_water", recording_capture)
    monkeypatch.setattr(monotonic, "_source_batch", flaky_batch)
    monkeypatch.setattr(base.time, "sleep", lambda _seconds: None)

    monotonic.migrate_monotonic_integer_append_only_table(
        source,
        target,
        source_table,
        target_table,
        shared,
        table_report,
        report,
        progress,
        batch_size=2,
        completed_batches=0,
        interrupt_after_batches=None,
    )

    assert injected is True
    assert calls >= 4
    assert captures == 1
    assert table_report["verified"] is True
    assert table_report["snapshot_high_water_primary_key"] == [5]
    assert table_report["source_transport_retries"] == 3
    assert table_report["last_source_retry_phase"] == "snapshot_copy_batch"
    assert table_report["last_source_retry_recovered"] is True


def test_monotonic_stage_one_path_rejects_non_integer_primary_keys(tmp_path):
    source = create_engine(f"sqlite:///{tmp_path / 'hash-source.sqlite3'}")
    target = create_engine(f"sqlite:///{tmp_path / 'hash-target.sqlite3'}")
    source_metadata = MetaData()
    target_metadata = MetaData()
    source_table = Table(
        "cycle_historical_quotes",
        source_metadata,
        Column("quote_id", Text, primary_key=True),
        Column("payload_json", Text, nullable=False),
    )
    target_table = Table(
        "cycle_historical_quotes",
        target_metadata,
        Column("quote_id", Text, primary_key=True),
        Column("payload_json", Text, nullable=False),
    )
    source_metadata.create_all(source)
    target_metadata.create_all(target)
    report = {"state": "running", "tables": {"cycle_historical_quotes": {}}}

    with pytest.raises(RuntimeError, match="requires an integer primary key"):
        monotonic.migrate_monotonic_integer_append_only_table(
            source,
            target,
            source_table,
            target_table,
            ["quote_id", "payload_json"],
            report["tables"]["cycle_historical_quotes"],
            report,
            tmp_path / "progress.json",
            batch_size=256,
            completed_batches=0,
            interrupt_after_batches=None,
        )
