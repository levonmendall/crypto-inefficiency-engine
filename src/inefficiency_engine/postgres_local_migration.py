from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import MetaData, Table, create_engine, func, insert, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.local_storage import local_storage_paths
from inefficiency_engine.models import MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


BATCH_SIZE = 2_000
SKIPPED_RUNTIME_TABLES = {"cycle_history_index_runtime_state"}


def _progress_path() -> Path:
    return local_storage_paths().migration / "postgres-import-progress.json"


def _publish(payload: dict[str, object]) -> None:
    path = _progress_path()
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2))
    os.replace(temporary, path)


def migrate(source_url: str, *, batch_size: int = BATCH_SIZE) -> dict[str, object]:
    """Restartable PostgreSQL-to-disk import; never grants evidence authority."""

    if not source_url.startswith(("postgresql://", "postgres://", "postgresql+psycopg://")):
        raise ValueError("migration source must be PostgreSQL")
    source = create_engine(source_url.replace("postgres://", "postgresql+psycopg://", 1).replace("postgresql://", "postgresql+psycopg://", 1))
    target = EvidenceStore(local_storage_paths().metadata_db)
    history = PartitionedMarketHistory()
    source_metadata, target_metadata = MetaData(), MetaData()
    source_metadata.reflect(source)
    target_metadata.reflect(target.engine)
    started = datetime.now(timezone.utc).isoformat()
    report: dict[str, object] = {"state": "running", "started_at": started, "tables": {}, "paper_only": True,
                                 "live_execution_authority": False, "forward_evidence_granted": False}
    _publish(report)
    for name in sorted(source_metadata.tables):
        if name in SKIPPED_RUNTIME_TABLES:
            continue
        source_table = source_metadata.tables[name]
        source_count = int(source.connect().execute(select(func.count()).select_from(source_table)).scalar_one())
        if name == "market_quotes":
            offset = 0
            while offset < source_count:
                with source.connect() as db:
                    rows = list(db.execute(select(source_table.c.payload_json).order_by(source_table.c.id).offset(offset).limit(batch_size)).scalars())
                history.append(MarketQuote.model_validate_json(row) for row in rows)
                offset += len(rows)
                report["tables"][name] = {"source_rows": source_count, "processed_rows": offset, "destination": "parquet"}
                _publish(report)
            continue
        if name not in target_metadata.tables:
            report["state"] = "failed"
            report["error"] = f"target schema missing canonical table: {name}"
            _publish(report)
            raise RuntimeError(str(report["error"]))
        target_table = target_metadata.tables[name]
        shared = [column.name for column in source_table.columns if column.name in target_table.c]
        if not shared:
            continue
        offset = 0
        while offset < source_count:
            with source.connect() as db:
                rows = [dict(row) for row in db.execute(select(*(source_table.c[column] for column in shared)).offset(offset).limit(batch_size)).mappings()]
            if rows:
                with target.engine.begin() as db:
                    db.execute(sqlite_insert(target_table).values(rows).on_conflict_do_nothing())
            offset += len(rows)
            report["tables"][name] = {"source_rows": source_count, "processed_rows": offset, "destination": "sqlite"}
            _publish(report)
        with target.engine.connect() as db:
            target_count = int(db.execute(select(func.count()).select_from(target_table)).scalar_one())
        if target_count < source_count:
            report["state"] = "failed"
            report["error"] = f"count mismatch for {name}: source={source_count} target={target_count}"
            _publish(report)
            raise RuntimeError(str(report["error"]))
        report["tables"][name]["verified_rows"] = target_count
    history_status = history.readiness()
    if source_metadata.tables.get("market_quotes") is not None and not history_status["ready"]:
        report["state"] = "failed"
        report["error"] = "market history partition verification failed"
        _publish(report)
        raise RuntimeError(str(report["error"]))
    report.update(state="verified", completed_at=datetime.now(timezone.utc).isoformat(), history=history_status)
    _publish(report)
    return report


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
