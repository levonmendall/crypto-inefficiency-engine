from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import Engine, Table, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from inefficiency_engine import postgres_local_migration as base


SNAPSHOT_MIGRATION_MODE = "captured_primary_key_membership_manifest"
SNAPSHOT_VERIFICATION_SCOPE = "captured_primary_key_membership"
MAX_SNAPSHOT_BATCH_SIZE = 256
SNAPSHOT_STREAM_BUFFER_ROWS = 512
SNAPSHOT_CAPTURE_PROGRESS_INTERVAL_ROWS = 10_000
RELATIONAL_SOURCE_RETRY_DELAYS_SECONDS = (1.0, 3.0, 8.0)
_BASE_MIGRATE_ENGINES = base.migrate_engines


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _effective_batch_size(requested: int) -> int:
    return max(1, min(int(requested), MAX_SNAPSHOT_BATCH_SIZE))


def _manifest_path(progress_path: Path, table_name: str) -> Path:
    return progress_path.parent / f"{table_name}-stage-one-primary-keys.jsonl"


def _manifest_inventory(path: Path) -> tuple[int, str, list[object] | None]:
    digest = hashlib.sha256()
    count = 0
    last_key: object | None = None
    with path.open("rb") as manifest:
        for raw_line in manifest:
            if not raw_line.endswith(b"\n"):
                raise RuntimeError(f"snapshot membership manifest is truncated: {path.name}")
            digest.update(raw_line)
            try:
                last_key = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"snapshot membership manifest is invalid: {path.name}") from exc
            count += 1
    return count, digest.hexdigest(), [last_key] if count else None


def _capture_membership_manifest(
    source: Engine,
    source_table: Table,
    source_pk: Any,
    table_report: dict[str, Any],
    report: dict[str, Any],
    progress_path: Path,
) -> tuple[Path, int, list[object] | None]:
    """Capture one finite append-only membership set before copying full rows.

    Hash primary keys are deterministic but not insertion ordered. A max hash therefore
    cannot be a stage-one high-water: a later insert can sort behind it. We instead
    persist the exact primary-key membership visible in one repeatable-read snapshot.
    Once this compact manifest is durable, every expensive row copy and verification
    read can use short-lived connections without chasing writes that arrive afterward.
    """

    path = _manifest_path(progress_path, source_table.name)
    expected_count = table_report.get("snapshot_row_count")
    expected_digest = table_report.get("snapshot_manifest_sha256")
    if expected_count is not None or expected_digest is not None:
        if expected_count is None or not expected_digest or not path.exists():
            raise RuntimeError(f"incomplete snapshot membership evidence for {source_table.name}")
        count, digest, maximum = _manifest_inventory(path)
        if count != int(expected_count) or digest != str(expected_digest):
            raise RuntimeError(
                f"snapshot membership manifest changed for {source_table.name}: "
                f"expected={expected_count}/{expected_digest} current={count}/{digest}"
            )
        return path, count, maximum

    retries = 0
    while True:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        count = 0
        last_key: object | None = None
        table_report.update(
            snapshot_phase="capturing_membership",
            snapshot_manifest_rows_captured=0,
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)
        try:
            with base._open_relational_snapshot(source) as source_db:
                with source_db.begin():
                    if source.dialect.name == "postgresql":
                        source_db.exec_driver_sql("SET TRANSACTION READ ONLY")
                    rows = source_db.execution_options(
                        stream_results=True,
                        max_row_buffer=SNAPSHOT_STREAM_BUFFER_ROWS,
                    ).execute(select(source_pk).order_by(source_pk))
                    with temporary.open("wb") as manifest:
                        for row in rows:
                            value = row[0]
                            encoded = (
                                json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
                                + "\n"
                            ).encode()
                            manifest.write(encoded)
                            digest.update(encoded)
                            count += 1
                            last_key = value
                            if count % SNAPSHOT_CAPTURE_PROGRESS_INTERVAL_ROWS == 0:
                                table_report.update(
                                    snapshot_manifest_rows_captured=count,
                                    last_progress_at=_now(),
                                )
                                base._publish(report, progress_path)
                        manifest.flush()
                        os.fsync(manifest.fileno())
            os.replace(temporary, path)
            table_report.update(
                snapshot_row_count=count,
                snapshot_manifest_sha256=digest.hexdigest(),
                snapshot_manifest_file=path.name,
                snapshot_max_primary_key=[last_key] if count else None,
                snapshot_captured_at=_now(),
                snapshot_manifest_rows_captured=count,
                snapshot_rows_copied=0,
                snapshot_rows_verified=0,
                snapshot_phase="membership_captured",
                last_progress_at=_now(),
            )
            base._publish(report, progress_path)
            return path, count, [last_key] if count else None
        except OperationalError as exc:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            if (
                not base._is_transient_source_read_error(exc)
                or retries >= len(base.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS)
            ):
                raise
            delay = base.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS[retries]
            retries += 1
            source.dispose()
            table_report.update(
                snapshot_capture_retries=int(table_report.get("snapshot_capture_retries") or 0) + 1,
                last_source_retry_phase="snapshot_membership_capture",
                last_source_retry_delay_seconds=delay,
                last_source_retry_recovered=False,
                last_progress_at=_now(),
            )
            base._publish(report, progress_path)
            time.sleep(delay)


def _manifest_batches(
    path: Path,
    batch_size: int,
    *,
    skip_rows: int = 0,
) -> Iterator[list[object]]:
    skipped = 0
    batch: list[object] = []
    with path.open("r", encoding="utf-8") as manifest:
        for line in manifest:
            if skipped < skip_rows:
                skipped += 1
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if skipped != skip_rows:
        raise RuntimeError(
            f"snapshot membership checkpoint exceeds manifest: checkpoint={skip_rows} actual={skipped}"
        )
    if batch:
        yield batch


def _rows_for_keys(
    engine: Engine,
    table: Table,
    shared: list[str],
    primary_key: Any,
    keys: tuple[object, ...],
) -> list[dict[str, object]]:
    statement = (
        select(*(table.c[name] for name in shared))
        .where(primary_key.in_(keys))
        .order_by(primary_key)
    )
    with engine.connect() as db:
        return [dict(row) for row in db.execute(statement).mappings()]


def _insert_immutable_rows(
    target: Engine,
    table: Table,
    rows: list[dict[str, object]],
) -> None:
    """Insert only missing append-only rows without rewriting existing SQLite pages."""
    if not rows:
        return
    primary_key_names = [column.name for column in base._primary_key(table)]
    statement = sqlite_insert(table).values(rows).on_conflict_do_nothing(
        index_elements=primary_key_names
    )
    with target.begin() as db:
        db.execute(statement)


def _update_digest(
    digest: Any,
    rows: list[dict[str, object]],
    shared: list[str],
) -> None:
    for row in rows:
        digest.update(
            json.dumps(
                [row.get(name) for name in shared],
                sort_keys=True,
                default=str,
            ).encode()
            + b"\n"
        )


def _verified_captured_target_is_intact(
    target: Engine,
    target_table: Table,
    shared: list[str],
    target_pk: Any,
    manifest_path: Path,
    table_report: dict[str, Any],
    *,
    batch_size: int,
) -> bool:
    if table_report.get("verified") is not True:
        return False
    expected_count = table_report.get("verified_rows")
    expected_digest = table_report.get("row_digest")
    if expected_count is None or not expected_digest:
        return False
    digest = hashlib.sha256()
    count = 0
    for keys in _manifest_batches(manifest_path, batch_size):
        immutable_keys = tuple(keys)
        rows = _rows_for_keys(target, target_table, shared, target_pk, immutable_keys)
        if [row[target_pk.name] for row in rows] != list(immutable_keys):
            raise RuntimeError(
                f"verified captured membership missing from local target: {target_table.name}"
            )
        _update_digest(digest, rows, shared)
        count += len(rows)
    if count != int(expected_count) or digest.hexdigest() != str(expected_digest):
        raise RuntimeError(
            f"verified captured snapshot changed for {target_table.name}: "
            f"expected={expected_count}/{expected_digest} "
            f"current={count}/{digest.hexdigest()}"
        )
    return True


def _migrate_captured_append_only_table(
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
    primary_key = base._primary_key(source_table)
    if len(primary_key) != 1:
        raise RuntimeError(
            f"captured append-only migration requires one primary key: {source_table.name}"
        )
    source_pk = primary_key[0]
    target_pk = target_table.c[source_pk.name]
    bounded_batch_size = _effective_batch_size(batch_size)

    if (
        table_report.get("verified") is True
        and table_report.get("migration_mode") == SNAPSHOT_MIGRATION_MODE
    ):
        manifest_path, _, _ = _capture_membership_manifest(
            source,
            source_table,
            source_pk,
            table_report,
            report,
            progress_path,
        )
        if _verified_captured_target_is_intact(
            target,
            target_table,
            shared,
            target_pk,
            manifest_path,
            table_report,
            batch_size=bounded_batch_size,
        ):
            return completed_batches

    if table_report.get("migration_mode") != SNAPSHOT_MIGRATION_MODE:
        preserved_retries = int(table_report.get("source_transport_retries") or 0)
        path = _manifest_path(progress_path, source_table.name)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        table_report.clear()
        table_report.update(
            destination="sqlite",
            verified=False,
            migration_mode=SNAPSHOT_MIGRATION_MODE,
            verification_scope=SNAPSHOT_VERIFICATION_SCOPE,
            snapshot_rows_copied=0,
            snapshot_rows_verified=0,
            snapshot_batch_size=bounded_batch_size,
            source_transport_retries=preserved_retries,
            legacy_target_preserved=True,
            upgrade_reason="legacy_live_reconciliation_replaced_without_target_reset",
            snapshot_phase="preparing_snapshot",
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)

    table_report["snapshot_batch_size"] = bounded_batch_size
    manifest_path, snapshot_count, maximum = _capture_membership_manifest(
        source,
        source_table,
        source_pk,
        table_report,
        report,
        progress_path,
    )

    copied = int(table_report.get("snapshot_rows_copied") or 0)
    if copied < 0 or copied > snapshot_count:
        raise RuntimeError(
            f"invalid snapshot copy checkpoint for {source_table.name}: {copied}/{snapshot_count}"
        )

    table_report.update(snapshot_phase="copying_snapshot", last_progress_at=_now())
    base._publish(report, progress_path)
    for keys in _manifest_batches(manifest_path, bounded_batch_size, skip_rows=copied):
        immutable_keys = tuple(keys)
        rows = base._source_read_with_retry(
            source,
            lambda immutable_keys=immutable_keys: _rows_for_keys(
                source, source_table, shared, source_pk, immutable_keys
            ),
            table_report,
            report,
            progress_path,
            phase="snapshot_copy_batch",
        )
        observed_keys = [row[source_pk.name] for row in rows]
        if observed_keys != list(immutable_keys):
            raise RuntimeError(
                f"captured source membership changed for {source_table.name}: "
                f"expected={list(immutable_keys)!r} observed={observed_keys!r}"
            )
        _insert_immutable_rows(target, target_table, rows)
        copied += len(rows)
        table_report.update(
            snapshot_rows_copied=copied,
            last_primary_key=[immutable_keys[-1]],
            last_progress_at=_now(),
        )
        base._publish(report, progress_path)
        completed_batches += 1
        if interrupt_after_batches == completed_batches:
            raise InterruptedError("injected migration interruption")

    verified = 0
    target_digest = hashlib.sha256()
    table_report.update(snapshot_phase="verifying_snapshot", last_progress_at=_now())
    base._publish(report, progress_path)
    for keys in _manifest_batches(manifest_path, bounded_batch_size):
        immutable_keys = tuple(keys)
        source_rows = base._source_read_with_retry(
            source,
            lambda immutable_keys=immutable_keys: _rows_for_keys(
                source, source_table, shared, source_pk, immutable_keys
            ),
            table_report,
            report,
            progress_path,
            phase="snapshot_verification_batch",
        )
        target_rows = _rows_for_keys(target, target_table, shared, target_pk, immutable_keys)
        source_keys = [row[source_pk.name] for row in source_rows]
        target_keys = [row[source_pk.name] for row in target_rows]
        if source_keys != list(immutable_keys) or target_keys != list(immutable_keys):
            raise RuntimeError(
                f"captured membership missing during verification for {source_table.name}"
            )
        if base._canonical_rows(source_rows, shared) != base._canonical_rows(target_rows, shared):
            raise RuntimeError(f"captured row-content mismatch for {source_table.name}")
        _update_digest(target_digest, target_rows, shared)
        verified += len(keys)
        table_report.update(snapshot_rows_verified=verified, last_progress_at=_now())
        base._publish(report, progress_path)

    if verified != snapshot_count:
        raise RuntimeError(
            f"captured verification count mismatch for {source_table.name}: "
            f"snapshot={snapshot_count} verified={verified}"
        )

    table_report.update(
        source_rows=snapshot_count,
        verified_rows=snapshot_count,
        snapshot_rows_copied=snapshot_count,
        snapshot_rows_verified=snapshot_count,
        row_digest=target_digest.hexdigest(),
        high_water_primary_key=maximum,
        last_primary_key=maximum,
        verification_scope=SNAPSHOT_VERIFICATION_SCOPE,
        target_extra_rows_allowed=True,
        snapshot_phase="verified",
        verified=True,
        last_progress_at=_now(),
    )
    table_report.pop("upgrade_reason", None)
    base._publish(report, progress_path)
    return completed_batches


def _current_unverified_relational_table(progress_path: Path) -> str | None:
    progress = base._load_progress(progress_path)
    current_table = str(progress.get("current_table") or "").strip()
    if (
        not current_table
        or current_table == "market_quotes"
        or current_table in base.RESUMABLE_APPEND_ONLY_TABLES
    ):
        return None
    tables = progress.get("tables")
    if not isinstance(tables, dict):
        return None
    table_report = tables.get(current_table)
    if not isinstance(table_report, dict) or table_report.get("verified") is True:
        return None
    return current_table


def _migrate_engines_with_relational_source_retry(
    source: Engine,
    target: Any,
    history: Any,
    *,
    progress_path: Path,
    batch_size: int = base.BATCH_SIZE,
    interrupt_after_batches: int | None = None,
) -> dict[str, object]:
    """Restart a transiently interrupted mutable-table snapshot in this child.

    Mutable relational tables require one repeatable-read transaction, so a dropped
    PostgreSQL connection cannot resume at a checkpoint without weakening snapshot
    consistency. The base importer already restarts an unverified mutable table from
    the beginning while preserving all verified tables. Retry that exact operation in
    this child, but scope the bounded transport budget to the table that actually
    failed. Successful advancement to a different table resets the budget, preventing
    unrelated transient disconnects across a long multi-table migration from
    cumulatively exhausting one global retry allowance.
    """

    retries = 0
    retry_table: str | None = None
    while True:
        try:
            return _BASE_MIGRATE_ENGINES(
                source,
                target,
                history,
                progress_path=progress_path,
                batch_size=batch_size,
                interrupt_after_batches=interrupt_after_batches,
            )
        except OperationalError as exc:
            current_table = _current_unverified_relational_table(progress_path)
            if current_table is not None and current_table != retry_table:
                retry_table = current_table
                retries = 0
            if (
                current_table is None
                or not base._is_transient_source_read_error(exc)
                or retries >= len(RELATIONAL_SOURCE_RETRY_DELAYS_SECONDS)
            ):
                raise
            delay = RELATIONAL_SOURCE_RETRY_DELAYS_SECONDS[retries]
            retries += 1
            source.dispose()
            time.sleep(delay)


def install_stage_one_repair() -> None:
    base._migrate_resumable_append_only_table = _migrate_captured_append_only_table
    base.migrate_engines = _migrate_engines_with_relational_source_retry


def main() -> int:
    install_stage_one_repair()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())