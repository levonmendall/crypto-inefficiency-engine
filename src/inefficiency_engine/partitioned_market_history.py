from __future__ import annotations

import hashlib
import os
import sqlite3
import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from inefficiency_engine.local_storage import local_storage_paths, safe_partition_component
from inefficiency_engine.models import MarketQuote


SCHEMA_VERSION = 1
REQUIRED_COLUMNS = (
    "history_id", "lineage_hash", "venue", "asset", "observed_at", "payload_json"
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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
        return hashlib.sha256(quote.model_dump_json().encode()).hexdigest()

    def append(self, quotes: Iterable[MarketQuote]) -> int:
        return self.append_records((None, self._lineage(quote), quote) for quote in quotes)

    def append_records(
        self,
        records: Iterable[tuple[int | None, str, MarketQuote]],
    ) -> int:
        """Append records while preserving source lineage and ids during migration."""

        grouped: dict[tuple[str, str, str], list[tuple[int | None, str, MarketQuote]]] = {}
        for source_id, lineage, quote in records:
            if not lineage:
                raise ValueError("market history lineage_hash is required")
            observed = _utc(quote.observed_at)
            grouped.setdefault(
                (quote.venue, quote.asset.upper(), observed.date().isoformat()), []
            ).append((source_id, lineage, quote))
        written = 0
        for (venue, asset, day), rows in grouped.items():
            written += self._append_partition(venue, asset, day, rows)
        return written

    def _append_partition(
        self,
        venue: str,
        asset: str,
        day: str,
        rows: list[tuple[int | None, str, MarketQuote]],
    ) -> int:
        candidates = list({lineage: (source_id, quote) for source_id, lineage, quote in rows}.items())
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
            accepted = [(lineage, source_id, quote) for lineage, (source_id, quote) in candidates if lineage not in existing]
            if not accepted:
                db.rollback()
                return 0
            accepted.sort(key=lambda item: (_utc(item[2].observed_at), item[0]))
            next_id = int(db.execute("SELECT COALESCE(MAX(history_id), 0) + 1 FROM quote_lineage").fetchone()[0])
            requested = [item[1] for item in accepted]
            requested_ids_available = (
                all(item is not None for item in requested)
                and len(set(requested)) == len(requested)
                and not db.execute(
                    "SELECT 1 FROM quote_lineage WHERE history_id IN (%s) LIMIT 1"
                    % ",".join("?" for _ in requested), requested,
                ).fetchone()
            )
            history_ids = [int(item) for item in requested] if requested_ids_available else list(
                range(next_id, next_id + len(accepted))
            )
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
                "observed_at": [_utc(item[2].observed_at).isoformat() for item in accepted],
                "payload_json": [item[2].model_dump_json() for item in accepted],
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
            checksum = _file_checksum(final_path)
            relative = str(final_path.relative_to(self.root))
            observed_values = payload["observed_at"]
            db.execute(
                "INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (relative, venue, asset, day, min(observed_values), max(observed_values), len(accepted), checksum,
                 datetime.now(timezone.utc).isoformat(), SCHEMA_VERSION),
            )
            db.executemany(
                "INSERT INTO quote_lineage(lineage_hash, partition_path, history_id) VALUES (?, ?, ?)",
                [(item[0], relative, history_id) for history_id, item in zip(history_ids, accepted)],
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

    def inventory(self, *, verify_files: bool = True) -> dict[str, object]:
        """Return a physically verified identity inventory for migration comparison."""

        with self._connect() as db:
            partitions = list(db.execute(
                "SELECT path, venue, asset, day, min_observed_at, max_observed_at, "
                "row_count, checksum, schema_version FROM partitions ORDER BY path"
            ))
            manifest_lineages = list(db.execute(
                "SELECT lineage_hash, partition_path, history_id FROM quote_lineage ORDER BY lineage_hash"
            ))
        errors: list[str] = []
        physical_lineages: set[str] = set()
        stream_times: dict[tuple[str, str], list[datetime]] = {}
        physical_rows = 0
        for relative, venue, asset, day, minimum, maximum, row_count, checksum, schema_version in partitions:
            path = self.root / relative
            if int(schema_version) != SCHEMA_VERSION:
                errors.append(f"schema_version_mismatch:{relative}")
                continue
            if not path.is_file() or path.name.startswith("."):
                errors.append(f"partition_missing:{relative}")
                continue
            if not verify_files:
                continue
            try:
                if _file_checksum(path) != checksum:
                    errors.append(f"checksum_mismatch:{relative}")
                    continue
                parquet = pq.ParquetFile(path)
                names = tuple(parquet.schema_arrow.names)
                if names != REQUIRED_COLUMNS:
                    errors.append(f"parquet_schema_mismatch:{relative}")
                    continue
                table = parquet.read()
                if table.num_rows != int(row_count):
                    errors.append(f"row_count_mismatch:{relative}")
                    continue
                columns = {name: table[name].to_pylist() for name in names}
                observed = [datetime.fromisoformat(value) for value in columns["observed_at"]]
                if not observed or min(value.isoformat() for value in observed) != minimum or max(value.isoformat() for value in observed) != maximum:
                    errors.append(f"observed_range_mismatch:{relative}")
                    continue
                if any(value != venue for value in columns["venue"]) or any(value != asset for value in columns["asset"]):
                    errors.append(f"partition_identity_mismatch:{relative}")
                    continue
                if any(_utc(value).date().isoformat() != day for value in observed):
                    errors.append(f"partition_day_mismatch:{relative}")
                    continue
                physical_rows += table.num_rows
                physical_lineages.update(columns["lineage_hash"])
                stream_times.setdefault((str(venue), str(asset).upper()), []).extend(observed)
            except Exception as exc:
                errors.append(f"partition_unreadable:{relative}:{type(exc).__name__}")
        manifest_set = {str(row[0]) for row in manifest_lineages}
        manifest_paths = {str(row[1]) for row in manifest_lineages}
        partition_paths = {str(row[0]) for row in partitions}
        if verify_files and physical_lineages != manifest_set:
            errors.append("lineage_manifest_mismatch")
        if manifest_paths - partition_paths:
            errors.append("lineage_partition_reference_missing")
        lineage_digest = hashlib.sha256()
        for lineage in sorted(manifest_set):
            lineage_digest.update(lineage.encode() + b"\n")
        all_times = [value for values in stream_times.values() for value in values]
        return {
            "valid": not errors,
            "errors": errors,
            "partition_count": len(partitions),
            "row_count": physical_rows if verify_files else len(manifest_lineages),
            "lineage_count": len(manifest_set),
            "lineage_digest": lineage_digest.hexdigest(),
            "min_observed_at": min(all_times).isoformat() if all_times else None,
            "max_observed_at": max(all_times).isoformat() if all_times else None,
            "identities": sorted(f"{venue}|{asset}" for venue, asset in stream_times),
            "stream_times": stream_times,
        }

    def readiness(
        self,
        *,
        required_start: datetime | None = None,
        required_end: datetime | None = None,
        required_identities: Iterable[tuple[str, str]] | None = None,
        max_gap: timedelta = timedelta(hours=12),
    ) -> dict[str, object]:
        inventory = self.inventory(verify_files=True)
        stream_times = inventory.pop("stream_times")
        identity_source = stream_times.keys() if required_identities is None else required_identities
        required = {(str(venue), str(asset).upper()) for venue, asset in identity_source}
        errors = list(inventory["errors"])
        if not required:
            errors.append("required_identity_scope_empty")
        start = _utc(required_start) if required_start else None
        end = _utc(required_end) if required_end else None
        for identity in sorted(required):
            values = sorted(_utc(value) for value in stream_times.get(identity, ()))
            if not values:
                errors.append(f"required_identity_missing:{identity[0]}|{identity[1]}")
                continue
            scoped = [value for value in values if (start is None or value >= start) and (end is None or value <= end)]
            if not scoped:
                errors.append(f"required_range_empty:{identity[0]}|{identity[1]}")
                continue
            if start is not None and scoped[0] - start > max_gap:
                errors.append(f"required_start_gap:{identity[0]}|{identity[1]}")
            if end is not None and end - scoped[-1] > max_gap:
                errors.append(f"required_end_gap:{identity[0]}|{identity[1]}")
            if any(right - left > max_gap for left, right in zip(scoped, scoped[1:])):
                errors.append(f"continuity_gap:{identity[0]}|{identity[1]}")
        coverage_ok = not errors
        return {
            **inventory,
            "ready": coverage_ok,
            "errors": errors,
            "reason": "partition_coverage_verified" if coverage_ok else "partition_coverage_incomplete",
            "required_identities": sorted(f"{venue}|{asset}" for venue, asset in required),
            "max_gap_seconds": max_gap.total_seconds(),
            "schema_version": SCHEMA_VERSION, "paper_only": True, "live_execution_authority": False,
            "qualification_thresholds_unchanged": True,
        }


__all__ = ["PartitionedMarketHistory", "SCHEMA_VERSION"]
