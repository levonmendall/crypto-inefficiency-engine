from __future__ import annotations

import json
import os
import sqlite3
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from inefficiency_engine.local_storage import local_storage_paths, safe_partition_component
from inefficiency_engine.models import MarketQuote


SCHEMA_VERSION = 1


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class PartitionedMarketHistory:
    """Atomic append-only Parquet history with a WAL-backed manifest.

    Parquet files are immutable and become visible only after fsync+rename and a
    committed manifest row. Readers never glob temporary files. The manifest's
    unique lineage hash preserves the relational ledger's deduplication semantics
    across processes and restartable imports.
    """

    def __init__(self, root: str | Path | None = None):
        paths = local_storage_paths(root)
        self.root = paths.market_history
        self.manifest_path = self.root / "manifest.sqlite3"
        self._initialize_manifest()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.manifest_path, timeout=30.0, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def _initialize_manifest(self) -> None:
        with self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS partitions (
                    path TEXT PRIMARY KEY,
                    venue TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    day TEXT NOT NULL,
                    min_observed_at TEXT NOT NULL,
                    max_observed_at TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    checksum TEXT NOT NULL,
                    committed_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS quote_lineage (
                    lineage_hash TEXT PRIMARY KEY,
                    partition_path TEXT NOT NULL REFERENCES partitions(path),
                    history_id INTEGER NOT NULL UNIQUE
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS ix_partitions_range "
                "ON partitions(venue, asset, day, min_observed_at, max_observed_at)"
            )

    @staticmethod
    def _lineage(quote: MarketQuote) -> str:
        import hashlib

        return hashlib.sha256(quote.model_dump_json().encode()).hexdigest()

    def append(self, quotes: Iterable[MarketQuote]) -> int:
        grouped: dict[tuple[str, str, str], list[MarketQuote]] = {}
        for quote in quotes:
            observed = _utc(quote.observed_at)
            grouped.setdefault((quote.venue, quote.asset.upper(), observed.date().isoformat()), []).append(quote)
        written = 0
        for (venue, asset, day), rows in grouped.items():
            written += self._append_partition(venue, asset, day, rows)
        return written

    def _append_partition(self, venue: str, asset: str, day: str, rows: list[MarketQuote]) -> int:
        # Collapse duplicates within this batch before checking durable lineage.
        candidates = list({self._lineage(row): row for row in rows}.items())
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = {
                item[0]
                for item in db.execute(
                    "SELECT lineage_hash FROM quote_lineage WHERE lineage_hash IN (%s)"
                    % ",".join("?" for _ in candidates),
                    [item[0] for item in candidates],
                )
            } if candidates else set()
            accepted = [(lineage, row) for lineage, row in candidates if lineage not in existing]
            if not accepted:
                db.rollback()
                return 0
            accepted.sort(key=lambda item: (_utc(item[1].observed_at), item[0]))
            next_id = int(db.execute("SELECT COALESCE(MAX(history_id), 0) + 1 FROM quote_lineage").fetchone()[0])
            history_ids = list(range(next_id, next_id + len(accepted)))
            directory = self.root / f"venue={safe_partition_component(venue)}" / f"asset={safe_partition_component(asset)}" / f"date={day}"
            directory.mkdir(parents=True, exist_ok=True)
            name = f"part-{uuid.uuid4().hex}.parquet"
            final_path = directory / name
            temp_path = directory / f".{name}.tmp"
            payload = {
                "history_id": history_ids,
                "lineage_hash": [item[0] for item in accepted],
                "venue": [venue] * len(accepted),
                "asset": [asset] * len(accepted),
                "observed_at": [_utc(item[1].observed_at).isoformat() for item in accepted],
                "payload_json": [item[1].model_dump_json() for item in accepted],
            }
            pq.write_table(pa.table(payload), temp_path, compression="zstd")
            with temp_path.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temp_path, final_path)
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            import hashlib

            checksum = hashlib.sha256(final_path.read_bytes()).hexdigest()
            relative = str(final_path.relative_to(self.root))
            observed_values = payload["observed_at"]
            db.execute(
                "INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (relative, venue, asset, day, min(observed_values), max(observed_values), len(accepted), checksum,
                 datetime.now(timezone.utc).isoformat(), SCHEMA_VERSION),
            )
            db.executemany(
                "INSERT INTO quote_lineage(lineage_hash, partition_path, history_id) VALUES (?, ?, ?)",
                [(lineage, relative, history_id) for history_id, (lineage, _) in zip(history_ids, accepted)],
            )
            db.commit()
        return len(accepted)

    def range(self, *, start: datetime, end: datetime, venues: Iterable[str] | None = None,
              assets: Iterable[str] | None = None) -> list[MarketQuote]:
        start_iso, end_iso = _utc(start).isoformat(), _utc(end).isoformat()
        clauses = ["max_observed_at >= ?", "min_observed_at <= ?"]
        params: list[object] = [start_iso, end_iso]
        for column, values in (("venue", venues), ("asset", assets)):
            normalized = sorted({str(value).upper() if column == "asset" else str(value) for value in (values or ())})
            if normalized:
                clauses.append(f"{column} IN ({','.join('?' for _ in normalized)})")
                params.extend(normalized)
        with self._connect() as db:
            paths = [row[0] for row in db.execute(f"SELECT path FROM partitions WHERE {' AND '.join(clauses)}", params)]
        result: dict[str, tuple[int, MarketQuote]] = {}
        for relative in paths:
            path = self.root / relative
            if not path.is_file() or path.name.startswith("."):
                continue
            table = pq.ParquetFile(path).read(
                columns=["history_id", "lineage_hash", "observed_at", "payload_json"]
            )
            for history_id, lineage, observed_at, payload_json in zip(*[table[column].to_pylist() for column in table.column_names]):
                if start_iso <= observed_at <= end_iso:
                    result[lineage] = (int(history_id), MarketQuote.model_validate_json(payload_json))
        return [item[1] for item in sorted(result.values(), key=lambda item: (_utc(item[1].observed_at), item[0]))]

    def readiness(self, *, required_start: datetime | None = None, required_end: datetime | None = None) -> dict[str, object]:
        with self._connect() as db:
            row = db.execute("SELECT COUNT(*), COALESCE(SUM(row_count), 0), MIN(min_observed_at), MAX(max_observed_at) FROM partitions").fetchone()
        count, rows, minimum, maximum = row or (0, 0, None, None)
        coverage_ok = bool(count and rows)
        if required_start is not None:
            coverage_ok = coverage_ok and minimum is not None and minimum <= _utc(required_start).isoformat()
        if required_end is not None:
            coverage_ok = coverage_ok and maximum is not None and maximum >= _utc(required_end).isoformat()
        return {
            "ready": bool(coverage_ok), "reason": "partition_coverage_verified" if coverage_ok else "partition_coverage_incomplete",
            "partition_count": int(count), "row_count": int(rows), "min_observed_at": minimum, "max_observed_at": maximum,
            "schema_version": SCHEMA_VERSION, "paper_only": True, "live_execution_authority": False,
            "qualification_thresholds_unchanged": True,
        }


__all__ = ["PartitionedMarketHistory", "SCHEMA_VERSION"]
