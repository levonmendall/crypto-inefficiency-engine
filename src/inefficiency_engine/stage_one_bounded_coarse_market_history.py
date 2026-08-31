from __future__ import annotations

import hashlib
from datetime import datetime

import pyarrow.parquet as pq

from inefficiency_engine.coarse_partitioned_market_history import (
    MULTI_ASSET_PARTITION,
    CoarsePartitionedMarketHistory,
)
from inefficiency_engine.partitioned_market_history import (
    REQUIRED_COLUMNS,
    SCHEMA_VERSION,
    _file_checksum,
    _utc,
)


VERIFICATION_BATCH_ROWS = 4_096
VERIFICATION_COLUMNS = (
    "history_id",
    "lineage_hash",
    "venue",
    "asset",
    "observed_at",
)
TEMP_CACHE_KIB = 16 * 1_024


class BoundedStageOneCoarsePartitionedMarketHistory(CoarsePartitionedMarketHistory):
    """Coarse Stage 1 history with disk-backed, bounded final verification.

    Production Stage 1 can contain millions of market lineages.  The normal coarse
    inventory is useful for readiness because it retains per-stream timestamps, but
    that shape is inappropriate for one-time migration equivalence: materializing all
    lineage rows, physical lineage sets, and observation timestamps can exceed the
    service memory limit after the copy has already reached its fixed high-water.

    This Stage-1-only subclass preserves the same fail-closed physical and lineage
    checks while keeping memory proportional to one Parquet batch plus the relatively
    small identity set.  Exact duplicate/membership accounting is held in SQLite TEMP
    tables forced to FILE storage, so millions of lineage hashes are not retained in
    Python memory.
    """

    def inventory(self, *, verify_files: bool = True) -> dict[str, object]:
        with self._connect() as db:
            partitions = list(
                db.execute(
                    "SELECT path, venue, asset, day, min_observed_at, max_observed_at, "
                    "row_count, checksum, schema_version FROM partitions ORDER BY path"
                )
            )
            manifest_count = int(
                db.execute("SELECT COUNT(*) FROM quote_lineage").fetchone()[0]
            )

            lineage_digest = hashlib.sha256()
            for (lineage,) in db.execute(
                "SELECT lineage_hash FROM quote_lineage ORDER BY lineage_hash"
            ):
                lineage_digest.update(str(lineage).encode() + b"\n")

            errors: list[str] = []
            identities: set[str] = set()
            minimum: str | None = None
            maximum: str | None = None
            physical_rows = 0

            if db.execute(
                "SELECT 1 FROM quote_lineage AS q "
                "LEFT JOIN partitions AS p ON p.path = q.partition_path "
                "WHERE p.path IS NULL LIMIT 1"
            ).fetchone():
                errors.append("lineage_partition_reference_missing")

            if verify_files:
                # _connect() defaults temp_store to MEMORY for normal runtime use.
                # Stage 1 equivalence intentionally overrides it before creating any
                # TEMP objects so exact seen-lineage accounting spills to disk.
                db.execute("PRAGMA temp_store=FILE")
                db.execute(
                    "CREATE TEMP TABLE inventory_partition_map ("
                    "partition_id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE)"
                )
                db.execute(
                    "CREATE TEMP TABLE inventory_seen_lineage ("
                    "lineage_hash TEXT PRIMARY KEY, "
                    "history_id INTEGER NOT NULL UNIQUE, "
                    "partition_id INTEGER NOT NULL)"
                )
                db.execute(f"PRAGMA temp.cache_size=-{TEMP_CACHE_KIB}")
                db.executemany(
                    "INSERT INTO inventory_partition_map(partition_id, path) VALUES (?, ?)",
                    [(index, str(row[0])) for index, row in enumerate(partitions)],
                )

                for partition_id, (
                    relative,
                    venue,
                    manifest_asset,
                    day,
                    expected_minimum,
                    expected_maximum,
                    row_count,
                    checksum,
                    schema_version,
                ) in enumerate(partitions):
                    relative = str(relative)
                    venue = str(venue)
                    manifest_asset = str(manifest_asset)
                    day = str(day)
                    expected_rows = int(row_count)
                    path = self.root / relative

                    manifest_partition_rows = int(
                        db.execute(
                            "SELECT COUNT(*) FROM quote_lineage WHERE partition_path = ?",
                            (relative,),
                        ).fetchone()[0]
                    )
                    if manifest_partition_rows != expected_rows:
                        errors.append(f"lineage_partition_row_count_mismatch:{relative}")

                    if int(schema_version) != SCHEMA_VERSION:
                        errors.append(f"schema_version_mismatch:{relative}")
                        continue
                    if not path.is_file() or path.name.startswith("."):
                        errors.append(f"partition_missing:{relative}")
                        continue

                    try:
                        if _file_checksum(path) != str(checksum):
                            errors.append(f"checksum_mismatch:{relative}")
                            continue
                        parquet = pq.ParquetFile(path)
                        if tuple(parquet.schema_arrow.names) != REQUIRED_COLUMNS:
                            errors.append(f"parquet_schema_mismatch:{relative}")
                            continue
                        if int(parquet.metadata.num_rows) != expected_rows:
                            errors.append(f"row_count_mismatch:{relative}")
                            continue

                        file_rows = 0
                        file_minimum: str | None = None
                        file_maximum: str | None = None
                        for batch in parquet.iter_batches(
                            batch_size=VERIFICATION_BATCH_ROWS,
                            columns=list(VERIFICATION_COLUMNS),
                        ):
                            columns = batch.to_pydict()
                            history_ids = columns["history_id"]
                            lineages = columns["lineage_hash"]
                            venues = columns["venue"]
                            assets = columns["asset"]
                            observed_values = columns["observed_at"]

                            before_changes = db.total_changes
                            db.executemany(
                                "INSERT OR IGNORE INTO inventory_seen_lineage("
                                "lineage_hash, history_id, partition_id) VALUES (?, ?, ?)",
                                [
                                    (str(lineage), int(history_id), partition_id)
                                    for history_id, lineage in zip(history_ids, lineages)
                                ],
                            )
                            inserted = db.total_changes - before_changes
                            if inserted != batch.num_rows:
                                errors.append(f"duplicate_physical_lineage:{relative}")

                            for row_venue, row_asset, observed_value in zip(
                                venues, assets, observed_values
                            ):
                                row_venue = str(row_venue)
                                row_asset = str(row_asset).upper()
                                observed = _utc(datetime.fromisoformat(str(observed_value)))
                                observed_iso = observed.isoformat()

                                if row_venue != venue:
                                    errors.append(f"partition_identity_mismatch:{relative}")
                                if (
                                    manifest_asset != MULTI_ASSET_PARTITION
                                    and row_asset != manifest_asset.upper()
                                ):
                                    errors.append(f"partition_identity_mismatch:{relative}")
                                if observed.date().isoformat() != day:
                                    errors.append(f"partition_day_mismatch:{relative}")

                                identities.add(f"{row_venue}|{row_asset}")
                                file_minimum = (
                                    observed_iso
                                    if file_minimum is None or observed_iso < file_minimum
                                    else file_minimum
                                )
                                file_maximum = (
                                    observed_iso
                                    if file_maximum is None or observed_iso > file_maximum
                                    else file_maximum
                                )
                                minimum = (
                                    observed_iso
                                    if minimum is None or observed_iso < minimum
                                    else minimum
                                )
                                maximum = (
                                    observed_iso
                                    if maximum is None or observed_iso > maximum
                                    else maximum
                                )

                            file_rows += batch.num_rows
                            physical_rows += batch.num_rows

                        if file_rows != expected_rows:
                            errors.append(f"row_count_mismatch:{relative}")
                        if (
                            file_minimum != str(expected_minimum)
                            or file_maximum != str(expected_maximum)
                        ):
                            errors.append(f"observed_range_mismatch:{relative}")
                    except Exception as exc:
                        errors.append(
                            f"partition_unreadable:{relative}:{type(exc).__name__}"
                        )

                seen_count = int(
                    db.execute("SELECT COUNT(*) FROM inventory_seen_lineage").fetchone()[0]
                )
                seen_not_in_manifest = db.execute(
                    "SELECT 1 FROM inventory_seen_lineage AS s "
                    "JOIN inventory_partition_map AS p "
                    "ON p.partition_id = s.partition_id "
                    "LEFT JOIN quote_lineage AS q "
                    "ON q.lineage_hash = s.lineage_hash "
                    "AND q.history_id = s.history_id "
                    "AND q.partition_path = p.path "
                    "WHERE q.lineage_hash IS NULL LIMIT 1"
                ).fetchone()
                manifest_not_seen = db.execute(
                    "SELECT 1 FROM quote_lineage AS q "
                    "LEFT JOIN inventory_partition_map AS p ON p.path = q.partition_path "
                    "LEFT JOIN inventory_seen_lineage AS s "
                    "ON s.lineage_hash = q.lineage_hash "
                    "AND s.history_id = q.history_id "
                    "AND s.partition_id = p.partition_id "
                    "WHERE s.lineage_hash IS NULL LIMIT 1"
                ).fetchone()
                if (
                    seen_count != manifest_count
                    or seen_not_in_manifest is not None
                    or manifest_not_seen is not None
                ):
                    errors.append("lineage_manifest_mismatch")
            else:
                physical_rows = manifest_count

        # Stage 1 equivalence removes stream_times before persisting/comparing the
        # inventory.  Returning an empty mapping preserves that interface without
        # retaining one timestamp object per quote in memory.
        return {
            "valid": not errors,
            "errors": errors,
            "partition_count": len(partitions),
            "row_count": physical_rows,
            "lineage_count": manifest_count,
            "lineage_digest": lineage_digest.hexdigest(),
            "min_observed_at": minimum,
            "max_observed_at": maximum,
            "identities": sorted(identities),
            "stream_times": {},
        }


__all__ = [
    "BoundedStageOneCoarsePartitionedMarketHistory",
    "TEMP_CACHE_KIB",
    "VERIFICATION_BATCH_ROWS",
    "VERIFICATION_COLUMNS",
]
