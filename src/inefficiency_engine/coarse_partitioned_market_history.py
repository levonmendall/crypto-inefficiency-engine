from __future__ import annotations

import os
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from inefficiency_engine.local_storage import safe_partition_component
from inefficiency_engine.models import MarketQuote
from inefficiency_engine.partitioned_market_history import (
    REQUIRED_COLUMNS,
    SCHEMA_VERSION,
    PartitionedMarketHistory,
    _file_checksum,
    _utc,
)


MULTI_ASSET_PARTITION = "__all_assets__"


class CoarsePartitionedMarketHistory(PartitionedMarketHistory):
    """Parquet history physically grouped by venue/day instead of venue/asset/day.

    The logical market identity remains ``(venue, asset)`` in every Parquet row and
    in all readiness/inventory results. Only the physical manifest key is coarsened.
    This prevents high-cardinality asset identifiers from consuming one filesystem
    inode per quote while preserving the existing append-only lineage contract.
    """

    def append_records(
        self,
        records: Iterable[tuple[int | None, str, MarketQuote]],
    ) -> int:
        grouped: dict[tuple[str, str], list[tuple[int | None, str, MarketQuote]]] = {}
        for source_id, lineage, quote in records:
            if not lineage:
                raise ValueError("market history lineage_hash is required")
            observed = _utc(quote.observed_at)
            grouped.setdefault((quote.venue, observed.date().isoformat()), []).append(
                (source_id, lineage, quote)
            )
        written = 0
        for (venue, day), rows in grouped.items():
            written += self._append_coarse_partition(venue, day, rows)
        return written

    def _append_coarse_partition(
        self,
        venue: str,
        day: str,
        rows: list[tuple[int | None, str, MarketQuote]],
    ) -> int:
        candidates = list(
            {lineage: (source_id, quote) for source_id, lineage, quote in rows}.items()
        )
        manifest_asset = MULTI_ASSET_PARTITION
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if (
                self.max_partition_files_per_group > 0
                and self._partition_group_count_connection(
                    db, venue, manifest_asset, day
                )
                >= self.max_partition_files_per_group
            ):
                db.rollback()
                self.compact_partition_group(venue, manifest_asset, day)
                return self._append_coarse_partition(venue, day, rows)

            existing = (
                {
                    item[0]
                    for item in db.execute(
                        "SELECT lineage_hash FROM quote_lineage WHERE lineage_hash IN (%s)"
                        % ",".join("?" for _ in candidates),
                        [item[0] for item in candidates],
                    )
                }
                if candidates
                else set()
            )
            accepted = [
                (lineage, source_id, quote)
                for lineage, (source_id, quote) in candidates
                if lineage not in existing
            ]
            if not accepted:
                db.rollback()
                return 0
            accepted.sort(key=lambda item: (_utc(item[2].observed_at), item[0]))
            next_id = int(
                db.execute(
                    "SELECT COALESCE(MAX(history_id), 0) + 1 FROM quote_lineage"
                ).fetchone()[0]
            )
            requested = [item[1] for item in accepted]
            requested_ids_available = (
                all(item is not None for item in requested)
                and len(set(requested)) == len(requested)
                and not db.execute(
                    "SELECT 1 FROM quote_lineage WHERE history_id IN (%s) LIMIT 1"
                    % ",".join("?" for _ in requested),
                    requested,
                ).fetchone()
            )
            history_ids = (
                [int(item) for item in requested]
                if requested_ids_available
                else list(range(next_id, next_id + len(accepted)))
            )
            directory = (
                self.root
                / f"venue={safe_partition_component(venue)}"
                / f"asset={safe_partition_component(manifest_asset)}"
                / f"date={day}"
            )
            directory.mkdir(parents=True, exist_ok=True)
            name = f"part-{uuid.uuid4().hex}.parquet"
            final_path = directory / name
            temp_path = directory / f".{name}.tmp"
            payload = {
                "history_id": history_ids,
                "lineage_hash": [item[0] for item in accepted],
                "venue": [venue] * len(accepted),
                "asset": [str(item[2].asset).upper() for item in accepted],
                "observed_at": [
                    _utc(item[2].observed_at).isoformat() for item in accepted
                ],
                "payload_json": [item[2].model_dump_json() for item in accepted],
            }
            manifest_committed = False
            try:
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
                    (
                        relative,
                        venue,
                        manifest_asset,
                        day,
                        min(observed_values),
                        max(observed_values),
                        len(accepted),
                        checksum,
                        datetime.now(timezone.utc).isoformat(),
                        SCHEMA_VERSION,
                    ),
                )
                db.executemany(
                    "INSERT INTO quote_lineage(lineage_hash, partition_path, history_id) "
                    "VALUES (?, ?, ?)",
                    [
                        (item[0], relative, history_id)
                        for history_id, item in zip(history_ids, accepted)
                    ],
                )
                db.commit()
                manifest_committed = True
            finally:
                if not manifest_committed:
                    db.rollback()
                    for path in (temp_path, final_path):
                        try:
                            path.unlink()
                        except FileNotFoundError:
                            pass
                        except OSError:
                            pass
        return len(accepted)

    def range(
        self,
        *,
        start: datetime,
        end: datetime,
        venues: Iterable[str] | None = None,
        assets: Iterable[str] | None = None,
    ) -> list[MarketQuote]:
        start_iso, end_iso = _utc(start).isoformat(), _utc(end).isoformat()
        clauses = ["max_observed_at >= ?", "min_observed_at <= ?"]
        params: list[object] = [start_iso, end_iso]
        normalized_venues = sorted({str(value) for value in (venues or ())})
        normalized_assets = sorted({str(value).upper() for value in (assets or ())})
        if normalized_venues:
            clauses.append(f"venue IN ({','.join('?' for _ in normalized_venues)})")
            params.extend(normalized_venues)
        if normalized_assets:
            clauses.append(
                f"(asset = ? OR asset IN ({','.join('?' for _ in normalized_assets)}))"
            )
            params.append(MULTI_ASSET_PARTITION)
            params.extend(normalized_assets)
        with self._connect() as db:
            paths = [
                row[0]
                for row in db.execute(
                    f"SELECT path FROM partitions WHERE {' AND '.join(clauses)}",
                    params,
                )
            ]
        result: dict[str, tuple[int, MarketQuote]] = {}
        requested_assets = set(normalized_assets)
        for relative in paths:
            path = self.root / relative
            if not path.is_file() or path.name.startswith("."):
                continue
            table = pq.ParquetFile(path).read(
                columns=["history_id", "lineage_hash", "observed_at", "payload_json"]
            )
            values = [table[column].to_pylist() for column in table.column_names]
            for history_id, lineage, observed_at, payload_json in zip(*values):
                if not (start_iso <= observed_at <= end_iso):
                    continue
                quote = MarketQuote.model_validate_json(payload_json)
                if requested_assets and str(quote.asset).upper() not in requested_assets:
                    continue
                result[lineage] = (int(history_id), quote)
        return [
            item[1]
            for item in sorted(
                result.values(), key=lambda item: (_utc(item[1].observed_at), item[0])
            )
        ]

    def inventory(self, *, verify_files: bool = True) -> dict[str, object]:
        """Verify physical files while reporting logical venue/asset identities."""

        with self._connect() as db:
            partitions = list(
                db.execute(
                    "SELECT path, venue, asset, day, min_observed_at, max_observed_at, "
                    "row_count, checksum, schema_version FROM partitions ORDER BY path"
                )
            )
            manifest_lineages = list(
                db.execute(
                    "SELECT lineage_hash, partition_path, history_id "
                    "FROM quote_lineage ORDER BY lineage_hash"
                )
            )
        errors: list[str] = []
        physical_lineages: set[str] = set()
        stream_times: dict[tuple[str, str], list[datetime]] = {}
        physical_rows = 0
        for (
            relative,
            venue,
            manifest_asset,
            day,
            minimum,
            maximum,
            row_count,
            checksum,
            schema_version,
        ) in partitions:
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
                if (
                    not observed
                    or min(value.isoformat() for value in observed) != minimum
                    or max(value.isoformat() for value in observed) != maximum
                ):
                    errors.append(f"observed_range_mismatch:{relative}")
                    continue
                if any(value != venue for value in columns["venue"]):
                    errors.append(f"partition_identity_mismatch:{relative}")
                    continue
                if manifest_asset != MULTI_ASSET_PARTITION and any(
                    str(value).upper() != str(manifest_asset).upper()
                    for value in columns["asset"]
                ):
                    errors.append(f"partition_identity_mismatch:{relative}")
                    continue
                if any(_utc(value).date().isoformat() != day for value in observed):
                    errors.append(f"partition_day_mismatch:{relative}")
                    continue
                physical_rows += table.num_rows
                physical_lineages.update(columns["lineage_hash"])
                for actual_asset, observed_at in zip(columns["asset"], observed):
                    stream_times.setdefault(
                        (str(venue), str(actual_asset).upper()), []
                    ).append(observed_at)
            except Exception as exc:
                errors.append(f"partition_unreadable:{relative}:{type(exc).__name__}")
        manifest_set = {str(row[0]) for row in manifest_lineages}
        manifest_paths = {str(row[1]) for row in manifest_lineages}
        partition_paths = {str(row[0]) for row in partitions}
        if verify_files and physical_lineages != manifest_set:
            errors.append("lineage_manifest_mismatch")
        if manifest_paths - partition_paths:
            errors.append("lineage_partition_reference_missing")
        import hashlib

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
            "identities": sorted(
                f"{venue}|{asset}" for venue, asset in stream_times
            ),
            "stream_times": stream_times,
        }


__all__ = ["CoarsePartitionedMarketHistory", "MULTI_ASSET_PARTITION"]
