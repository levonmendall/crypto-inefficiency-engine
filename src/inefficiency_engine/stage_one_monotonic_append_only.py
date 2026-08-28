from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, Table, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine import postgres_local_migration as base


MIGRATION_MODE = "captured_monotonic_integer_high_water"
VERIFICATION_SCOPE = "captured_monotonic_integer_high_water"
MAX_BATCH_SIZE = 256


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bounded_batch_size(requested: int) -> int:
    return max(1, min(int(requested), MAX_BATCH_SIZE))


def _source_batch(
    source: Engine,
    table: Table,
    shared: list[str],
    primary_key: Any,
    *,
    checkpoint: list[object] | None,
    high_water: list[object] | None,
    batch_size: int,
) -> list[dict[str, object]]:
    if not high_water:
        return []
    statement = (
        select(*(table.c[name] for name in shared))
        .where(primary_key <= high_water[0])
        .order_by(primary_key)
        .limit(batch_size)
    )
    if checkpoint:
        statement = statement.where(primary_key > checkpoint[0])
    with source.connect() as db:
        return [dict(row) for row in db.execute(statement).mappings()]


def _target_rows_for_keys(
    target: Engine,
    table: Table,
    shared: list[str],
    primary_key: Any,
    keys: list[object],
) -> list[dict[str, object]]:
    if not keys:
        return []
    statement = (
        select(*(table.c[name] for name in shared))
        .where(primary_key.in_(keys))
        .order_by(primary_key)
    )
    with target.connect() as db:
        return [dict(row) for row in db.execute(statement).mappings()]


def _insert_immutable_rows(target: Engine, table: Table, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    primary_key_names = [column.name for column in base._primary_key(table)]
    statement = sqlite_insert(table).values(rows).on_conflict_do_nothing(
        index_elements=primary_key_names
    )
    with target.begin() as db:
        db.execute(statement)


def migrate_monotonic_integer_append_only_table(
    source: Engine,
    target: Engine,
    source_table: Table,
    target_table: Table,
    shared: list[str],
    table_report: dict[str, Any],
    report: dict[str, Any],
    progress_path: Path,
    *,
    batch_size: int,
    completed_batches: int,
    interrupt_after_batches: int | None,
) -> int:
    """Migrate one immutable integer-PK ledger to one durable finite high-water.

    The high-water is captured with one short read and persisted before payload copy.
    Every subsequent source operation is a bounded keyset read, so a dropped TLS
    connection retries only that small read and a process restart resumes from the
    durable primary-key checkpoint. Rows inserted after the high-water are deliberately
    deferred to the final quiesced catch-up.
    """

    primary_key = base._primary_key(source_table)
    if len(primary_key) != 1:
        raise RuntimeError(
            f"monotonic stage-one migration requires one primary key: {source_table.name}"
        )
    source_pk = primary_key[0]
    target_pk = target_table.c[source_pk.name]
    try:
        python_type = source_pk.type.python_type
    except (AttributeError, NotImplementedError):
        python_type = None
    if python_type is not int:
        raise RuntimeError(
            f"monotonic stage-one migration requires an integer primary key: {source_table.name}"
        )

    bounded_batch_size = _bounded_batch_size(batch_size)
    if table_report.get("verified") is True and table_report.get("migration_mode") == MIGRATION_MODE:
        if base._verified_target_is_intact(target, target_table, shared, table_report):
            return completed_batches

    if table_report.get("migration_mode") != MIGRATION_MODE:
        preserved_transport_retries = int(table_report.get("source_transport_retries") or 0)
        preserved_capture_retries = int(table_report.get("snapshot_capture_retries") or 0)
        table_report.clear()
        table_report.update(
            destination="sqlite",
            verified=False,
            migration_mode=MIGRATION_MODE,
            verification_scope=VERIFICATION_SCOPE,
            snapshot_batch_size=bounded_batch_size,
            snapshot_rows_copied=0,
            snapshot_rows_verified=0,
            source_transport_retries=preserved_transport_retries,
            snapshot_capture_retries=preserved_capture_retries,
            legacy_target_preserved=True,
            snapshot_phase="capturing_high_water",
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)

    table_report["snapshot_batch_size"] = bounded_batch_size
    if table_report.get("snapshot_high_water_captured") is True:
        high_water = table_report.get("snapshot_high_water_primary_key")
    else:
        high_water = base._source_read_with_retry(
            source,
            lambda: base._capture_high_water(source, source_table),
            table_report,
            report,
            progress_path,
            phase="snapshot_high_water_capture",
        )
        table_report.update(
            snapshot_high_water_primary_key=high_water,
            snapshot_high_water_captured=True,
            snapshot_phase="high_water_captured",
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)

    checkpoint = table_report.get("last_primary_key")
    copied = int(table_report.get("snapshot_rows_copied") or 0)
    table_report.update(snapshot_phase="copying_snapshot", last_progress_at=_now())
    base._publish(report, progress_path)

    while True:
        rows = base._source_read_with_retry(
            source,
            lambda checkpoint=checkpoint: _source_batch(
                source,
                source_table,
                shared,
                source_pk,
                checkpoint=checkpoint,
                high_water=high_water,
                batch_size=bounded_batch_size,
            ),
            table_report,
            report,
            progress_path,
            phase="snapshot_copy_batch",
        )
        if not rows:
            break
        _insert_immutable_rows(target, target_table, rows)
        checkpoint = [rows[-1][source_pk.name]]
        copied += len(rows)
        table_report.update(
            last_primary_key=checkpoint,
            snapshot_rows_copied=copied,
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)
        completed_batches += 1
        if interrupt_after_batches == completed_batches:
            raise InterruptedError("injected migration interruption")

    verify_checkpoint: list[object] | None = None
    verified = 0
    source_digest = hashlib.sha256()
    table_report.update(
        snapshot_phase="verifying_snapshot",
        snapshot_rows_verified=0,
        last_progress_at=_now(),
    )
    base._publish(report, progress_path)

    while True:
        source_rows = base._source_read_with_retry(
            source,
            lambda verify_checkpoint=verify_checkpoint: _source_batch(
                source,
                source_table,
                shared,
                source_pk,
                checkpoint=verify_checkpoint,
                high_water=high_water,
                batch_size=bounded_batch_size,
            ),
            table_report,
            report,
            progress_path,
            phase="snapshot_verification_batch",
        )
        if not source_rows:
            break
        keys = [row[source_pk.name] for row in source_rows]
        target_rows = _target_rows_for_keys(target, target_table, shared, target_pk, keys)
        if base._canonical_rows(source_rows, shared) != base._canonical_rows(target_rows, shared):
            raise RuntimeError(f"captured row-content mismatch for {source_table.name}")
        for row in source_rows:
            source_digest.update(
                json.dumps([row.get(name) for name in shared], sort_keys=True, default=str).encode()
                + b"\n"
            )
        verified += len(source_rows)
        verify_checkpoint = [keys[-1]]
        table_report.update(snapshot_rows_verified=verified, last_progress_at=_now())
        base._publish(report, progress_path)

    target_count, target_digest = base._row_digest(
        target,
        target_table,
        shared,
        high_water=high_water,
    )
    if target_count != verified or target_digest != source_digest.hexdigest():
        raise RuntimeError(
            f"captured monotonic snapshot mismatch for {source_table.name}: "
            f"source={verified}/{source_digest.hexdigest()} target={target_count}/{target_digest}"
        )

    table_report.update(
        source_rows=verified,
        verified_rows=verified,
        snapshot_row_count=verified,
        snapshot_rows_copied=verified,
        snapshot_rows_verified=verified,
        row_digest=target_digest,
        high_water_primary_key=high_water,
        last_primary_key=high_water,
        verification_scope=VERIFICATION_SCOPE,
        target_extra_rows_allowed=True,
        snapshot_phase="verified",
        snapshot_captured_at=table_report.get("snapshot_captured_at") or _now(),
        verified=True,
        last_progress_at=_now(),
    )
    base._publish(report, progress_path)
    return completed_batches
