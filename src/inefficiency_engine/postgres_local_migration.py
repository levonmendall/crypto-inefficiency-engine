from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Engine,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
    tuple_,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.local_storage import local_storage_paths
from inefficiency_engine.models import MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory

BATCH_SIZE = 2_000
SKIPPED_RUNTIME_TABLES = {"cycle_history_index_runtime_state"}
RESUMABLE_APPEND_ONLY_TABLES = {"cycle_historical_quotes"}
MAX_APPEND_ONLY_RECONCILIATION_PASSES = 8
APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS = (1.0, 3.0, 8.0)
_TRANSIENT_SOURCE_READ_MARKERS = (
    "unexpected eof while reading",
    "consuming input failed",
    "server closed the connection unexpectedly",
    "connection reset by peer",
    "ssl connection has been closed unexpectedly",
    "terminating connection due to administrator command",
    "the database system is in recovery mode",
)


def _progress_path() -> Path:
    return local_storage_paths().migration / "postgres-import-progress.json"


def _publish(payload: dict[str, object], path: Path | None = None) -> None:
    path = path or _progress_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, path)


def _load_progress(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _primary_key(table: Table) -> list[Any]:
    columns = list(table.primary_key.columns)
    if not columns:
        raise RuntimeError(f"migration requires deterministic primary key: {table.name}")
    return columns


def _after_checkpoint(statement, primary_key: list[Any], checkpoint: list[object] | None):
    if not checkpoint:
        return statement
    if len(primary_key) == 1:
        return statement.where(primary_key[0] > checkpoint[0])
    return statement.where(tuple_(*primary_key) > tuple(checkpoint))


def _at_or_before_high_water(statement, primary_key: list[Any], high_water: list[object] | None):
    if not high_water:
        return statement.where(False)
    if len(primary_key) == 1:
        return statement.where(primary_key[0] <= high_water[0])
    return statement.where(tuple_(*primary_key) <= tuple(high_water))


def _capture_high_water_connection(db: Connection, table: Table) -> list[object] | None:
    primary_key = _primary_key(table)
    statement = select(*primary_key).order_by(*(column.desc() for column in primary_key)).limit(1)
    row = db.execute(statement).first()
    return list(row) if row is not None else None


def _capture_high_water(engine: Engine, table: Table) -> list[object] | None:
    with engine.connect() as db:
        return _capture_high_water_connection(db, table)


def _row_digest_connection(
    db: Connection,
    table: Table,
    column_names: list[str],
    *,
    high_water: list[object] | None,
) -> tuple[int, str]:
    primary_key = _primary_key(table)
    digest = hashlib.sha256()
    count = 0
    statement = select(*(table.c[name] for name in column_names)).order_by(*primary_key)
    statement = _at_or_before_high_water(statement, primary_key, high_water)
    rows = db.execution_options(stream_results=True).execute(statement)
    for row in rows:
        digest.update(json.dumps(list(row), sort_keys=True, default=str).encode() + b"\n")
        count += 1
    return count, digest.hexdigest()


def _row_digest(
    engine: Engine,
    table: Table,
    column_names: list[str],
    *,
    high_water: list[object] | None,
) -> tuple[int, str]:
    with engine.connect() as db:
        return _row_digest_connection(db, table, column_names, high_water=high_water)


def _source_market_inventory(
    engine: Engine,
    table: Table,
    *,
    high_water: list[object] | None,
) -> dict[str, object]:
    required = {"id", "lineage_hash", "venue", "asset", "observed_at", "payload_json"}
    missing = required - set(table.c.keys())
    if missing:
        raise RuntimeError(f"market_quotes migration columns missing: {sorted(missing)}")
    digest = hashlib.sha256()
    identities: set[str] = set()
    minimum: str | None = None
    maximum: str | None = None
    distinct = 0
    primary_key = _primary_key(table)
    count_statement = _at_or_before_high_water(
        select(func.count()).select_from(table), primary_key, high_water
    )
    rows_statement = _at_or_before_high_water(
        select(
            table.c.lineage_hash,
            func.min(table.c.venue),
            func.min(table.c.asset),
            func.min(table.c.observed_at),
            func.max(table.c.observed_at),
        ).group_by(table.c.lineage_hash).order_by(table.c.lineage_hash),
        primary_key,
        high_water,
    )
    with engine.connect() as db:
        source_rows = int(db.execute(count_statement).scalar_one())
        rows = db.execution_options(stream_results=True).execute(rows_statement)
        for lineage, venue, asset, row_minimum, row_maximum in rows:
            digest.update(str(lineage).encode() + b"\n")
            identities.add(f"{venue}|{str(asset).upper()}")
            row_minimum, row_maximum = str(row_minimum), str(row_maximum)
            minimum = row_minimum if minimum is None or row_minimum < minimum else minimum
            maximum = row_maximum if maximum is None or row_maximum > maximum else maximum
            distinct += 1
    return {
        "source_rows": source_rows,
        "lineage_count": distinct,
        "lineage_digest": digest.hexdigest(),
        "min_observed_at": minimum,
        "max_observed_at": maximum,
        "identities": sorted(identities),
    }


def _verify_market_equivalence(source: dict[str, object], history: PartitionedMarketHistory) -> dict[str, object]:
    destination = history.inventory(verify_files=True)
    destination.pop("stream_times", None)
    comparable = ("lineage_count", "lineage_digest", "min_observed_at", "max_observed_at", "identities")
    mismatches = {
        key: {"source": source.get(key), "destination": destination.get(key)}
        for key in comparable if source.get(key) != destination.get(key)
    }
    if not destination["valid"] or mismatches:
        raise RuntimeError("market_quotes equivalence mismatch: " + json.dumps(
            {"physical_errors": destination["errors"], "mismatches": mismatches}, sort_keys=True
        ))
    return destination


def _verify_existing_market_destination(
    table_report: dict[str, Any], history: PartitionedMarketHistory
) -> None:
    expected = table_report.get("destination_inventory")
    if not isinstance(expected, dict):
        table_report.pop("verified", None)
        return
    current = history.inventory(verify_files=True)
    current.pop("stream_times", None)
    comparable = ("lineage_count", "lineage_digest", "min_observed_at", "max_observed_at", "identities")
    mismatches = {
        key: {"expected": expected.get(key), "current": current.get(key)}
        for key in comparable if expected.get(key) != current.get(key)
    }
    if not current.get("valid") or mismatches:
        raise RuntimeError("verified market_quotes destination changed: " + json.dumps(
            {"physical_errors": current.get("errors"), "mismatches": mismatches}, sort_keys=True
        ))


def _portable_column_type(column: Any):
    """Map reflected PostgreSQL types to the smallest lossless SQLite affinity."""
    try:
        python_type = column.type.python_type
    except (AttributeError, NotImplementedError):
        return Text()
    if python_type is bool:
        return Boolean()
    if python_type is int:
        if column.primary_key and len(column.table.primary_key.columns) == 1:
            return Integer()
        return BigInteger() if isinstance(column.type, BigInteger) else Integer()
    if python_type is float:
        return Float()
    if python_type.__name__ == "Decimal":
        return Numeric()
    if python_type is bytes:
        return LargeBinary()
    return Text()


def bootstrap_local_schema_from_source(source_metadata: MetaData, target_engine: Engine) -> set[str]:
    """Create every reflected production table before copying any canonical rows."""
    target_metadata = MetaData()
    for source_table in source_metadata.sorted_tables:
        if source_table.name in SKIPPED_RUNTIME_TABLES:
            continue
        columns: list[Column[Any]] = []
        for source_column in source_table.columns:
            arguments: list[Any] = []
            for foreign_key in source_column.foreign_keys:
                arguments.append(ForeignKey(foreign_key.target_fullname))
            columns.append(
                Column(
                    source_column.name,
                    _portable_column_type(source_column),
                    *arguments,
                    primary_key=source_column.primary_key,
                    nullable=source_column.nullable,
                    unique=source_column.unique,
                    autoincrement=(
                        True
                        if source_column.primary_key
                        and len(source_table.primary_key.columns) == 1
                        and isinstance(_portable_column_type(source_column), Integer)
                        else "auto"
                    ),
                )
            )
        constraints: list[Any] = []
        for constraint in source_table.constraints:
            if isinstance(constraint, UniqueConstraint):
                names = [column.name for column in constraint.columns]
                if names and not any(
                    set(names) == {column.name}
                    for column in source_table.columns
                    if column.unique
                ):
                    constraints.append(UniqueConstraint(*names, name=constraint.name))
        Table(source_table.name, target_metadata, *columns, *constraints)
    target_metadata.create_all(target_engine)
    return set(target_metadata.tables)


def _upsert_rows(target: Engine, table: Table, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    primary_key_names = [column.name for column in _primary_key(table)]
    statement = sqlite_insert(table).values(rows)
    update_names = [name for name in rows[0] if name not in primary_key_names]
    with target.begin() as db:
        if update_names:
            db.execute(
                statement.on_conflict_do_update(
                    index_elements=primary_key_names,
                    set_={name: getattr(statement.excluded, name) for name in update_names},
                )
            )
        else:
            db.execute(statement.on_conflict_do_nothing(index_elements=primary_key_names))


def _verified_target_is_intact(
    target: Engine,
    table: Table,
    shared: list[str],
    table_report: dict[str, Any],
) -> bool:
    if table_report.get("verified") is not True:
        return False
    if "high_water_primary_key" not in table_report:
        return False
    expected_count = table_report.get("verified_rows")
    expected_digest = table_report.get("row_digest")
    if expected_count is None or not expected_digest:
        return False
    current_count, current_digest = _row_digest(
        target,
        table,
        shared,
        high_water=table_report.get("high_water_primary_key"),
    )
    if (current_count, current_digest) != (int(expected_count), str(expected_digest)):
        raise RuntimeError(
            f"verified local snapshot changed for {table.name}: "
            f"expected={expected_count}/{expected_digest} "
            f"current={current_count}/{current_digest}"
        )
    return True


def _clear_unverified_target(target: Engine, table: Table) -> None:
    with target.begin() as db:
        db.execute(table.delete())


def _open_relational_snapshot(source: Engine) -> Connection:
    db = source.connect()
    if source.dialect.name == "postgresql":
        db = db.execution_options(isolation_level="REPEATABLE READ")
    return db


def _row_count(engine: Engine, table: Table) -> int:
    with engine.connect() as db:
        return int(db.execute(select(func.count()).select_from(table)).scalar_one())


def _canonical_rows(rows: list[dict[str, object]], columns: list[str]) -> list[str]:
    return [
        json.dumps([row.get(name) for name in columns], sort_keys=True, default=str)
        for row in rows
    ]


def _is_transient_source_read_error(exc: OperationalError) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_SOURCE_READ_MARKERS)


def _source_read_with_retry(
    source: Engine,
    reader: Callable[[], Any],
    table_report: dict[str, Any],
    report: dict[str, Any],
    progress_path: Path,
    *,
    phase: str,
) -> Any:
    """Retry one append-only source read on a fresh pooled connection.

    The production PostgreSQL source has repeatedly dropped TLS connections while a
    cycle-history batch was being consumed. Because the archive path is append-only
    and durable progress is committed only after the full batch lands locally, the
    same read can be retried safely. Each retry disposes the source pool so the next
    ``source.connect()`` establishes a fresh connection. Non-transient errors and a
    bounded series of transient source failures still fail closed.
    """
    retries_this_read = 0
    while True:
        try:
            value = reader()
            if retries_this_read:
                table_report["last_source_retry_recovered"] = True
                _publish(report, progress_path)
            return value
        except OperationalError as exc:
            if (
                not _is_transient_source_read_error(exc)
                or retries_this_read >= len(APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS)
            ):
                raise
            delay = APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS[retries_this_read]
            retries_this_read += 1
            source.dispose()
            table_report.update(
                source_transport_retries=int(table_report.get("source_transport_retries") or 0) + 1,
                last_source_retry_phase=phase,
                last_source_retry_delay_seconds=delay,
                last_source_retry_recovered=False,
            )
            _publish(report, progress_path)
            time.sleep(delay)


def _source_rows_after_checkpoint(
    source: Engine,
    statement,
    primary_key: list[Any],
    checkpoint: list[object] | None,
) -> list[dict[str, object]]:
    with source.connect() as db:
        return [
            dict(row)
            for row in db.execute(
                _after_checkpoint(statement, primary_key, checkpoint)
            ).mappings()
        ]


def _migrate_resumable_append_only_table(
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
    """Copy an immutable append-only archive without a long-lived source session.

    ``cycle_historical_quotes`` uses a SHA-256 quote_id and INSERT ... ON CONFLICT DO
    NOTHING, so its rows are immutable but new identifiers are not monotonic. Durable
    keyset progress may therefore resume safely after transport failure, while a final
    full reconciliation pass catches identifiers inserted behind an earlier checkpoint.
    Exact source/target row content is then compared in bounded batches. No source
    connection is held for the duration of the archive copy or verification.
    """
    primary_key = _primary_key(source_table)
    if len(primary_key) != 1:
        raise RuntimeError(f"resumable append-only migration requires one primary key: {source_table.name}")
    source_pk = primary_key[0]
    target_pk = target_table.c[source_pk.name]

    if _verified_target_is_intact(target, target_table, shared, table_report):
        return completed_batches

    checkpoint = table_report.get("last_primary_key")
    reconciliation_pass = max(1, int(table_report.get("reconciliation_pass") or 1))
    table_report.update(
        destination="sqlite",
        verified=False,
        migration_mode="resumable_append_only_reconciliation",
        verification_scope="append_only_exact_content_reconciliation",
    )
    table_report.pop("restart_from_beginning", None)
    table_report.pop("high_water_primary_key", None)
    _publish(report, progress_path)

    base_statement = select(*(source_table.c[name] for name in shared)).order_by(source_pk).limit(batch_size)

    while reconciliation_pass <= MAX_APPEND_ONLY_RECONCILIATION_PASSES:
        table_report["reconciliation_pass"] = reconciliation_pass
        table_report["last_primary_key"] = checkpoint
        _publish(report, progress_path)

        while True:
            rows = _source_read_with_retry(
                source,
                lambda: _source_rows_after_checkpoint(
                    source, base_statement, primary_key, checkpoint
                ),
                table_report,
                report,
                progress_path,
                phase="copy_batch",
            )
            if not rows:
                break
            _upsert_rows(target, target_table, rows)
            checkpoint = [rows[-1][source_pk.name]]
            table_report.update(last_primary_key=checkpoint, copied_rows_at_least=_row_count(target, target_table))
            _publish(report, progress_path)
            completed_batches += 1
            if interrupt_after_batches == completed_batches:
                raise InterruptedError("injected migration interruption")

        source_count = _source_read_with_retry(
            source,
            lambda: _row_count(source, source_table),
            table_report,
            report,
            progress_path,
            phase="source_count",
        )
        target_count = _row_count(target, target_table)
        table_report.update(source_rows_observed=source_count, target_rows_observed=target_count)
        _publish(report, progress_path)

        if source_count != target_count:
            reconciliation_pass += 1
            checkpoint = None
            table_report.update(
                reconciliation_pass=reconciliation_pass,
                last_primary_key=None,
                reconciliation_reason="source_target_count_not_converged",
            )
            _publish(report, progress_path)
            continue

        verify_checkpoint: list[object] | None = None
        verified_count = 0
        content_mismatch = False
        while True:
            source_rows = _source_read_with_retry(
                source,
                lambda: _source_rows_after_checkpoint(
                    source, base_statement, primary_key, verify_checkpoint
                ),
                table_report,
                report,
                progress_path,
                phase="verification_batch",
            )
            if not source_rows:
                break
            keys = [row[source_pk.name] for row in source_rows]
            target_statement = (
                select(*(target_table.c[name] for name in shared))
                .where(target_pk.in_(keys))
                .order_by(target_pk)
            )
            with target.connect() as db:
                target_rows = [dict(row) for row in db.execute(target_statement).mappings()]
            if _canonical_rows(source_rows, shared) != _canonical_rows(target_rows, shared):
                content_mismatch = True
                break
            verified_count += len(source_rows)
            verify_checkpoint = [source_rows[-1][source_pk.name]]

        source_count_after = _source_read_with_retry(
            source,
            lambda: _row_count(source, source_table),
            table_report,
            report,
            progress_path,
            phase="final_source_count",
        )
        target_count_after = _row_count(target, target_table)
        if (
            content_mismatch
            or verified_count != source_count_after
            or source_count_after != target_count_after
        ):
            reconciliation_pass += 1
            checkpoint = None
            table_report.update(
                reconciliation_pass=reconciliation_pass,
                last_primary_key=None,
                reconciliation_reason=(
                    "row_content_not_converged" if content_mismatch
                    else "source_changed_during_exact_verification"
                ),
            )
            _publish(report, progress_path)
            continue

        high_water = _capture_high_water(target, target_table)
        target_digest_count, target_digest = _row_digest(
            target,
            target_table,
            shared,
            high_water=high_water,
        )
        if target_digest_count != target_count_after:
            raise RuntimeError(
                f"append-only target digest count mismatch for {source_table.name}: "
                f"count={target_count_after} digest_count={target_digest_count}"
            )
        table_report.pop("reconciliation_reason", None)
        table_report.update(
            source_rows=source_count_after,
            verified_rows=target_count_after,
            row_digest=target_digest,
            high_water_primary_key=high_water,
            last_primary_key=high_water,
            verification_scope="append_only_exact_content_reconciliation",
            verified=True,
        )
        _publish(report, progress_path)
        return completed_batches

    raise RuntimeError(
        f"append-only reconciliation did not converge for {source_table.name} after "
        f"{MAX_APPEND_ONLY_RECONCILIATION_PASSES} passes"
    )


def migrate_engines(
    source: Engine,
    target: EvidenceStore,
    history: PartitionedMarketHistory,
    *,
    progress_path: Path,
    batch_size: int = BATCH_SIZE,
    interrupt_after_batches: int | None = None,
) -> dict[str, object]:
    """Run a restart-safe import verified to deterministic Stage 1 snapshots.

    Mutable relational tables keep the repeatable-read/read-only snapshot semantics.
    ``cycle_historical_quotes`` is separately proven append-only and is copied with
    durable, short-lived keyset batches so a PostgreSQL transport drop no longer
    discards minutes of completed archive work. ``market_quotes`` retains its existing
    integer high-water Parquet migration. PostgreSQL remains authoritative throughout.
    """
    source_metadata, target_metadata = MetaData(), MetaData()
    source_metadata.reflect(source)
    bootstrap_local_schema_from_source(source_metadata, target.engine)
    target_metadata.reflect(target.engine)
    previous = _load_progress(progress_path)
    report: dict[str, Any] = {
        "state": "running",
        "started_at": previous.get("started_at") or datetime.now(timezone.utc).isoformat(),
        "resumed_at": datetime.now(timezone.utc).isoformat() if previous else None,
        "tables": previous.get("tables") if isinstance(previous.get("tables"), dict) else {},
        "current_table": None,
        "paper_only": True,
        "live_execution_authority": False,
        "forward_evidence_granted": False,
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "verification_scope": "captured_primary_key_high_water",
    }
    _publish(report, progress_path)
    completed_batches = 0
    try:
        for source_table in source_metadata.sorted_tables:
            name = source_table.name
            if name in SKIPPED_RUNTIME_TABLES:
                continue
            primary_key = _primary_key(source_table)
            table_report = report["tables"].setdefault(name, {})
            report["current_table"] = name
            _publish(report, progress_path)

            if name == "market_quotes":
                checkpoint = table_report.get("last_primary_key")
                if table_report.get("verified") is True:
                    _verify_existing_market_destination(table_report, history)
                    if table_report.get("verified") is True:
                        latest_high_water = _capture_high_water(source, source_table)
                        stored_high_water = table_report.get("high_water_primary_key")
                        if latest_high_water == stored_high_water:
                            continue
                        table_report.update(verified=False, high_water_primary_key=latest_high_water)
                        table_report.pop("destination_inventory", None)
                        _publish(report, progress_path)
                        high_water = latest_high_water
                    else:
                        high_water = _capture_high_water(source, source_table)
                        table_report["high_water_primary_key"] = high_water
                        _publish(report, progress_path)
                elif "high_water_primary_key" in table_report:
                    high_water = table_report.get("high_water_primary_key")
                else:
                    high_water = _capture_high_water(source, source_table)
                    table_report["high_water_primary_key"] = high_water
                    _publish(report, progress_path)

                source_inventory = _source_market_inventory(source, source_table, high_water=high_water)
                statement = select(
                    source_table.c.id,
                    source_table.c.lineage_hash,
                    source_table.c.payload_json,
                ).order_by(*primary_key).limit(batch_size)
                statement = _at_or_before_high_water(statement, primary_key, high_water)
                while True:
                    with source.connect() as db:
                        rows = list(db.execute(_after_checkpoint(statement, primary_key, checkpoint)))
                    if not rows:
                        break
                    history.append_records(
                        (int(row.id), str(row.lineage_hash), MarketQuote.model_validate_json(row.payload_json))
                        for row in rows
                    )
                    checkpoint = [rows[-1]._mapping[column.name] for column in primary_key]
                    table_report.update(
                        destination="parquet",
                        source_rows=source_inventory["source_rows"],
                        source_lineage_count=source_inventory["lineage_count"],
                        last_primary_key=checkpoint,
                    )
                    _publish(report, progress_path)
                    completed_batches += 1
                    if interrupt_after_batches == completed_batches:
                        raise InterruptedError("injected migration interruption")
                table_report.update(
                    verified=True,
                    verification_scope="captured_primary_key_high_water",
                    destination_inventory=_verify_market_equivalence(source_inventory, history),
                )
                _publish(report, progress_path)
                continue

            if name not in target_metadata.tables:
                raise RuntimeError(f"target schema missing canonical table: {name}")
            target_table = target_metadata.tables[name]
            shared = [column.name for column in source_table.columns if column.name in target_table.c]
            if any(column.name not in shared for column in primary_key):
                raise RuntimeError(f"target schema missing primary key columns for {name}")

            if name in RESUMABLE_APPEND_ONLY_TABLES:
                completed_batches = _migrate_resumable_append_only_table(
                    source,
                    target.engine,
                    source_table,
                    target_table,
                    shared,
                    table_report,
                    report,
                    progress_path,
                    batch_size=batch_size,
                    completed_batches=completed_batches,
                    interrupt_after_batches=interrupt_after_batches,
                )
                continue

            if _verified_target_is_intact(target.engine, target_table, shared, table_report):
                continue

            had_checkpoint = bool(table_report.get("last_primary_key"))
            table_report.clear()
            table_report.update(
                destination="sqlite",
                verified=False,
                restart_from_beginning=had_checkpoint,
                verification_scope="repeatable_read_primary_key_high_water",
            )
            _publish(report, progress_path)
            _clear_unverified_target(target.engine, target_table)

            with _open_relational_snapshot(source) as source_db:
                with source_db.begin():
                    if source.dialect.name == "postgresql":
                        source_db.exec_driver_sql("SET TRANSACTION READ ONLY")
                    high_water = _capture_high_water_connection(source_db, source_table)
                    table_report["high_water_primary_key"] = high_water
                    _publish(report, progress_path)
                    statement = select(
                        *(source_table.c[column] for column in shared)
                    ).order_by(*primary_key).limit(batch_size)
                    statement = _at_or_before_high_water(statement, primary_key, high_water)
                    checkpoint: list[object] | None = None
                    while True:
                        rows = [
                            dict(row)
                            for row in source_db.execute(
                                _after_checkpoint(statement, primary_key, checkpoint)
                            ).mappings()
                        ]
                        if not rows:
                            break
                        _upsert_rows(target.engine, target_table, rows)
                        checkpoint = [rows[-1][column.name] for column in primary_key]
                        table_report["last_primary_key"] = checkpoint
                        _publish(report, progress_path)
                        completed_batches += 1
                        if interrupt_after_batches == completed_batches:
                            raise InterruptedError("injected migration interruption")
                    source_count, source_digest = _row_digest_connection(
                        source_db,
                        source_table,
                        shared,
                        high_water=high_water,
                    )

            target_count, target_digest = _row_digest(
                target.engine,
                target_table,
                shared,
                high_water=high_water,
            )
            if (source_count, source_digest) != (target_count, target_digest):
                raise RuntimeError(
                    f"row-content mismatch for {name}: source={source_count}/{source_digest} "
                    f"target={target_count}/{target_digest}"
                )
            table_report.update(
                source_rows=source_count,
                verified_rows=target_count,
                row_digest=target_digest,
                verification_scope="repeatable_read_primary_key_high_water",
                verified=True,
            )
            _publish(report, progress_path)
    except Exception as exc:
        report.update(state="failed", error_type=type(exc).__name__, error=str(exc))
        _publish(report, progress_path)
        raise

    report.pop("error", None)
    report.pop("error_type", None)
    report.update(
        state="verified",
        current_table=None,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    _publish(report, progress_path)
    return report


def migrate(source_url: str, *, batch_size: int = BATCH_SIZE) -> dict[str, object]:
    if not source_url.startswith(("postgresql://", "postgres://", "postgresql+psycopg://")):
        raise ValueError("migration source must be PostgreSQL")
    normalized = source_url
    if normalized.startswith("postgres://"):
        normalized = "postgresql+psycopg://" + normalized[len("postgres://"):]
    elif normalized.startswith("postgresql://"):
        normalized = "postgresql+psycopg://" + normalized[len("postgresql://"):]
    source = create_engine(normalized)
    return migrate_engines(
        source,
        EvidenceStore(local_storage_paths().metadata_db),
        PartitionedMarketHistory(),
        progress_path=_progress_path(),
        batch_size=batch_size,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Import existing PostgreSQL state into canonical local storage")
    parser.add_argument("--source-url", default=os.getenv("CIE_MIGRATION_POSTGRES_URL"))
    args = parser.parse_args()
    if not args.source_url:
        parser.error("--source-url or CIE_MIGRATION_POSTGRES_URL is required")
    print(json.dumps(migrate(args.source_url), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
