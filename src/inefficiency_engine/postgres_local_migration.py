from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, MetaData, Table, create_engine, func, select, tuple_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.local_storage import local_storage_paths
from inefficiency_engine.models import MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory

BATCH_SIZE = 2_000
SKIPPED_RUNTIME_TABLES = {"cycle_history_index_runtime_state"}


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


def _identity_digest(engine: Engine, table: Table) -> tuple[int, str]:
    primary_key = _primary_key(table)
    digest = hashlib.sha256()
    count = 0
    with engine.connect() as db:
        rows = db.execution_options(stream_results=True).execute(select(*primary_key).order_by(*primary_key))
        for row in rows:
            digest.update(json.dumps(list(row), sort_keys=True, default=str).encode() + b"\n")
            count += 1
    return count, digest.hexdigest()


def _source_market_inventory(engine: Engine, table: Table) -> dict[str, object]:
    required = {"id", "lineage_hash", "venue", "asset", "observed_at", "payload_json"}
    missing = required - set(table.c.keys())
    if missing:
        raise RuntimeError(f"market_quotes migration columns missing: {sorted(missing)}")
    digest = hashlib.sha256()
    identities: set[str] = set()
    minimum: str | None = None
    maximum: str | None = None
    distinct = 0
    with engine.connect() as db:
        source_rows = int(db.execute(select(func.count()).select_from(table)).scalar_one())
        rows = db.execution_options(stream_results=True).execute(
            select(
                table.c.lineage_hash,
                func.min(table.c.venue),
                func.min(table.c.asset),
                func.min(table.c.observed_at),
                func.max(table.c.observed_at),
            ).group_by(table.c.lineage_hash).order_by(table.c.lineage_hash)
        )
        for lineage, venue, asset, row_minimum, row_maximum in rows:
            digest.update(str(lineage).encode() + b"\n")
            identities.add(f"{venue}|{str(asset).upper()}")
            row_minimum, row_maximum = str(row_minimum), str(row_maximum)
            minimum = row_minimum if minimum is None or row_minimum < minimum else minimum
            maximum = row_maximum if maximum is None or row_maximum > maximum else maximum
            distinct += 1
    return {
        "source_rows": source_rows, "lineage_count": distinct,
        "lineage_digest": digest.hexdigest(), "min_observed_at": minimum,
        "max_observed_at": maximum, "identities": sorted(identities),
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


def migrate_engines(
    source: Engine,
    target: EvidenceStore,
    history: PartitionedMarketHistory,
    *,
    progress_path: Path,
    batch_size: int = BATCH_SIZE,
    interrupt_after_batches: int | None = None,
) -> dict[str, object]:
    """Run a keyset-checkpointed, restartable and exactly verified import."""

    source_metadata, target_metadata = MetaData(), MetaData()
    source_metadata.reflect(source)
    target_metadata.reflect(target.engine)
    previous = _load_progress(progress_path)
    report: dict[str, Any] = {
        "state": "running",
        "started_at": previous.get("started_at") or datetime.now(timezone.utc).isoformat(),
        "resumed_at": datetime.now(timezone.utc).isoformat() if previous else None,
        "tables": previous.get("tables") if isinstance(previous.get("tables"), dict) else {},
        "paper_only": True, "live_execution_authority": False, "forward_evidence_granted": False,
    }
    _publish(report, progress_path)
    completed_batches = 0
    try:
        # SQLAlchemy's dependency order preserves parent-before-child foreign keys
        # while every table still uses deterministic primary-key keyset paging.
        for source_table in source_metadata.sorted_tables:
            name = source_table.name
            if name in SKIPPED_RUNTIME_TABLES:
                continue
            primary_key = _primary_key(source_table)
            table_report = report["tables"].setdefault(name, {})
            checkpoint = table_report.get("last_primary_key")
            if name == "market_quotes":
                source_inventory = _source_market_inventory(source, source_table)
                statement = select(
                    source_table.c.id, source_table.c.lineage_hash, source_table.c.payload_json,
                ).order_by(*primary_key).limit(batch_size)
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
                        destination="parquet", source_rows=source_inventory["source_rows"],
                        source_lineage_count=source_inventory["lineage_count"], last_primary_key=checkpoint,
                    )
                    _publish(report, progress_path)
                    completed_batches += 1
                    if interrupt_after_batches == completed_batches:
                        raise InterruptedError("injected migration interruption")
                table_report.update(verified=True, destination_inventory=_verify_market_equivalence(source_inventory, history))
                _publish(report, progress_path)
                continue
            if name not in target_metadata.tables:
                raise RuntimeError(f"target schema missing canonical table: {name}")
            target_table = target_metadata.tables[name]
            shared = [column.name for column in source_table.columns if column.name in target_table.c]
            if any(column.name not in shared for column in primary_key):
                raise RuntimeError(f"target schema missing primary key columns for {name}")
            statement = select(*(source_table.c[column] for column in shared)).order_by(*primary_key).limit(batch_size)
            while True:
                with source.connect() as db:
                    rows = [dict(row) for row in db.execute(
                        _after_checkpoint(statement, primary_key, checkpoint)
                    ).mappings()]
                if not rows:
                    break
                with target.engine.begin() as db:
                    db.execute(sqlite_insert(target_table).values(rows).on_conflict_do_nothing())
                checkpoint = [rows[-1][column.name] for column in primary_key]
                table_report.update(destination="sqlite", last_primary_key=checkpoint)
                _publish(report, progress_path)
                completed_batches += 1
                if interrupt_after_batches == completed_batches:
                    raise InterruptedError("injected migration interruption")
            source_count, source_digest = _identity_digest(source, source_table)
            target_count, target_digest = _identity_digest(target.engine, target_table)
            if (source_count, source_digest) != (target_count, target_digest):
                raise RuntimeError(
                    f"identity mismatch for {name}: source={source_count}/{source_digest} "
                    f"target={target_count}/{target_digest}"
                )
            table_report.update(
                source_rows=source_count, verified_rows=target_count,
                identity_digest=target_digest, verified=True,
            )
            _publish(report, progress_path)
    except Exception as exc:
        report.update(state="failed", error_type=type(exc).__name__, error=str(exc))
        _publish(report, progress_path)
        raise
    report.pop("error", None)
    report.pop("error_type", None)
    report.update(state="verified", completed_at=datetime.now(timezone.utc).isoformat())
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
        source, EvidenceStore(local_storage_paths().metadata_db), PartitionedMarketHistory(),
        progress_path=_progress_path(), batch_size=batch_size,
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
