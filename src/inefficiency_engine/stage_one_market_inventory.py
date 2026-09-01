from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, Table, select


MARKET_INVENTORY_BATCH_SIZE = 2_000
MARKET_INVENTORY_MODE = "bounded_primary_key_accumulator"
MARKET_INVENTORY_FINAL_SUMMARY_VERSION = 1
_FINAL_SUMMARY_META_KEYS = (
    "final_summary_version",
    "final_summary_high_water",
    "final_summary_checkpoint",
    "final_summary_payload",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inventory_path(progress_path: Path) -> Path:
    return progress_path.parent / "market-quotes-source-inventory.sqlite3"


def _connect_inventory(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute(
        """CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"""
    )
    db.execute(
        """CREATE TABLE IF NOT EXISTS lineages (
            lineage_hash TEXT PRIMARY KEY,
            venue TEXT NOT NULL,
            asset TEXT NOT NULL,
            min_observed_at TEXT NOT NULL,
            max_observed_at TEXT NOT NULL
        )"""
    )
    return db


def _meta_get(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else str(row[0])


def _meta_set(db: sqlite3.Connection, key: str, value: object) -> None:
    db.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def _invalidate_finalized_inventory(db: sqlite3.Connection) -> None:
    db.executemany(
        "DELETE FROM meta WHERE key = ?",
        ((key,) for key in _FINAL_SUMMARY_META_KEYS),
    )


def _high_water_token(high_water: list[object] | None) -> str:
    return json.dumps(high_water, sort_keys=True, default=str, separators=(",", ":"))


def _prepare_inventory(
    path: Path,
    high_water: list[object] | None,
) -> tuple[int | None, int]:
    token = _high_water_token(high_water)
    with closing(_connect_inventory(path)) as db:
        stored = _meta_get(db, "high_water")
        if stored != token:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM lineages")
            db.execute("DELETE FROM meta")
            _meta_set(db, "high_water", token)
            _meta_set(db, "checkpoint", "")
            _meta_set(db, "source_rows", 0)
            db.commit()
        checkpoint_raw = _meta_get(db, "checkpoint") or ""
        source_rows = int(_meta_get(db, "source_rows") or 0)
    return (int(checkpoint_raw) if checkpoint_raw else None), source_rows


def _read_market_inventory_batch(
    source: Engine,
    table: Table,
    *,
    high_water: list[object] | None,
    checkpoint: int | None,
    batch_size: int,
) -> list[dict[str, object]]:
    if not high_water:
        return []
    if len(high_water) != 1:
        raise RuntimeError("market_quotes inventory requires one integer high-water primary key")
    high_water_id = int(high_water[0])
    statement = (
        select(
            table.c.id,
            table.c.lineage_hash,
            table.c.venue,
            table.c.asset,
            table.c.observed_at,
        )
        .where(table.c.id <= high_water_id)
        .order_by(table.c.id)
        .limit(batch_size)
    )
    if checkpoint is not None:
        statement = statement.where(table.c.id > checkpoint)
    with source.connect() as db:
        return [dict(row) for row in db.execute(statement).mappings()]


def _accumulate_batch(
    path: Path,
    rows: list[dict[str, object]],
    *,
    previous_source_rows: int,
) -> tuple[int, int]:
    if not rows:
        raise RuntimeError("market inventory accumulator requires a non-empty batch")
    normalized = [
        (
            str(row["lineage_hash"]),
            str(row["venue"]),
            str(row["asset"]),
            str(row["observed_at"]),
            str(row["observed_at"]),
        )
        for row in rows
    ]
    checkpoint = int(rows[-1]["id"])
    source_rows = previous_source_rows + len(rows)
    with closing(_connect_inventory(path)) as db:
        db.execute("BEGIN IMMEDIATE")
        _invalidate_finalized_inventory(db)
        db.executemany(
            """INSERT INTO lineages(
                   lineage_hash, venue, asset, min_observed_at, max_observed_at
               ) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(lineage_hash) DO UPDATE SET
                   venue = min(lineages.venue, excluded.venue),
                   asset = min(lineages.asset, excluded.asset),
                   min_observed_at = min(lineages.min_observed_at, excluded.min_observed_at),
                   max_observed_at = max(lineages.max_observed_at, excluded.max_observed_at)
            """,
            normalized,
        )
        _meta_set(db, "checkpoint", checkpoint)
        _meta_set(db, "source_rows", source_rows)
        db.commit()
    return checkpoint, source_rows


def _finalize_inventory(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    identities: set[str] = set()
    minimum: str | None = None
    maximum: str | None = None
    lineage_count = 0
    with closing(_connect_inventory(path)) as db:
        source_rows = int(_meta_get(db, "source_rows") or 0)
        cursor = db.execute(
            "SELECT lineage_hash, venue, asset, min_observed_at, max_observed_at "
            "FROM lineages ORDER BY lineage_hash"
        )
        while True:
            rows = cursor.fetchmany(MARKET_INVENTORY_BATCH_SIZE)
            if not rows:
                break
            for lineage, venue, asset, row_minimum, row_maximum in rows:
                digest.update(str(lineage).encode() + b"\n")
                identities.add(f"{venue}|{str(asset).upper()}")
                row_minimum = str(row_minimum)
                row_maximum = str(row_maximum)
                minimum = row_minimum if minimum is None or row_minimum < minimum else minimum
                maximum = row_maximum if maximum is None or row_maximum > maximum else maximum
                lineage_count += 1
    return {
        "source_rows": source_rows,
        "lineage_count": lineage_count,
        "lineage_digest": digest.hexdigest(),
        "min_observed_at": minimum,
        "max_observed_at": maximum,
        "identities": sorted(identities),
    }


def _validated_final_summary_payload(raw: str | None, *, source_rows: int) -> dict[str, object] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        cached_source_rows = int(payload["source_rows"])
        lineage_count = int(payload["lineage_count"])
    except (KeyError, TypeError, ValueError):
        return None
    if cached_source_rows != source_rows or cached_source_rows < 0:
        return None
    if lineage_count < 0 or lineage_count > cached_source_rows:
        return None

    digest = payload.get("lineage_digest")
    if not isinstance(digest, str) or len(digest) != 64:
        return None
    try:
        int(digest, 16)
    except ValueError:
        return None

    identities = payload.get("identities")
    if not isinstance(identities, list) or not all(isinstance(value, str) for value in identities):
        return None
    if identities != sorted(set(identities)):
        return None

    minimum = payload.get("min_observed_at")
    maximum = payload.get("max_observed_at")
    if minimum is not None and not isinstance(minimum, str):
        return None
    if maximum is not None and not isinstance(maximum, str):
        return None
    if minimum is not None and maximum is not None and minimum > maximum:
        return None
    if cached_source_rows > 0 and (minimum is None or maximum is None):
        return None

    return {
        "source_rows": cached_source_rows,
        "lineage_count": lineage_count,
        "lineage_digest": digest,
        "min_observed_at": minimum,
        "max_observed_at": maximum,
        "identities": identities,
    }


def _read_finalized_inventory(
    path: Path,
    high_water: list[object] | None,
) -> dict[str, object] | None:
    if not high_water or len(high_water) != 1:
        return None
    expected_token = _high_water_token(high_water)
    expected_checkpoint = str(int(high_water[0]))
    with closing(_connect_inventory(path)) as db:
        if _meta_get(db, "high_water") != expected_token:
            return None
        if (_meta_get(db, "checkpoint") or "") != expected_checkpoint:
            return None
        if _meta_get(db, "final_summary_version") != str(MARKET_INVENTORY_FINAL_SUMMARY_VERSION):
            return None
        if _meta_get(db, "final_summary_high_water") != expected_token:
            return None
        if _meta_get(db, "final_summary_checkpoint") != expected_checkpoint:
            return None
        source_rows = int(_meta_get(db, "source_rows") or 0)
        raw = _meta_get(db, "final_summary_payload")
    return _validated_final_summary_payload(raw, source_rows=source_rows)


def _persist_finalized_inventory(
    path: Path,
    high_water: list[object] | None,
    inventory: dict[str, object],
) -> None:
    if not high_water or len(high_water) != 1:
        return
    expected_token = _high_water_token(high_water)
    expected_checkpoint = str(int(high_water[0]))
    payload = json.dumps(inventory, sort_keys=True, separators=(",", ":"))
    with closing(_connect_inventory(path)) as db:
        db.execute("BEGIN IMMEDIATE")
        if _meta_get(db, "high_water") != expected_token:
            db.rollback()
            raise RuntimeError("market source inventory high-water changed before summary commit")
        if (_meta_get(db, "checkpoint") or "") != expected_checkpoint:
            db.rollback()
            raise RuntimeError("market source inventory checkpoint incomplete before summary commit")
        if int(_meta_get(db, "source_rows") or 0) != int(inventory["source_rows"]):
            db.rollback()
            raise RuntimeError("market source inventory row count changed before summary commit")
        _meta_set(db, "final_summary_version", MARKET_INVENTORY_FINAL_SUMMARY_VERSION)
        _meta_set(db, "final_summary_high_water", expected_token)
        _meta_set(db, "final_summary_checkpoint", expected_checkpoint)
        _meta_set(db, "final_summary_payload", payload)
        db.commit()


def _evict_inventory_page_cache(path: Path) -> None:
    """Best-effort eviction of clean accumulator pages after the one exact full scan.

    Render charges filesystem page cache to the cgroup. The finalized source inventory
    is durable and can be reopened later, so keeping its multi-million-row SQLite pages
    resident after finalization has no correctness value. POSIX_FADV_DONTNEED only hints
    that clean pages may be reclaimed; unsupported hosts simply retain the old behavior.
    """

    fadvise = getattr(os, "posix_fadvise", None)
    dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
    if not callable(fadvise) or dontneed is None:
        return
    for candidate in (path, Path(f"{path}-wal")):
        try:
            with candidate.open("rb", buffering=0) as handle:
                fadvise(handle.fileno(), 0, 0, dontneed)
        except (FileNotFoundError, OSError):
            continue


def bounded_market_source_inventory(
    migration: Any,
    source: Engine,
    table: Table,
    table_report: dict[str, Any],
    report: dict[str, Any],
    progress_path: Path,
    *,
    high_water: list[object] | None,
) -> dict[str, object]:
    """Build exact market source inventory using restart-safe finite PK batches.

    The previous implementation executed one streaming GROUP BY over the entire live
    PostgreSQL market ledger. Production repeatedly lost the TLS connection late in
    that query, forcing each retry to restart the whole aggregation. This accumulator
    reads only bounded primary-key batches under the already-fixed market high-water,
    commits each batch into a durable local SQLite lineage inventory, and resumes from
    that separate inventory checkpoint after a child or service restart. Once the exact
    accumulator is complete, its deterministic final summary is cached under that same
    high-water so later memory-pressure child restarts do not reread millions of local
    lineage rows before they can resume the durable market copy checkpoint.
    """

    required = {"id", "lineage_hash", "venue", "asset", "observed_at"}
    missing = required - set(table.c.keys())
    if missing:
        raise RuntimeError(f"market_quotes migration columns missing: {sorted(missing)}")
    if high_water is not None and len(high_water) != 1:
        raise RuntimeError("market_quotes inventory requires one primary-key high-water")

    path = _inventory_path(progress_path)
    checkpoint, source_rows = _prepare_inventory(path, high_water)
    table_report.update(
        source_inventory_mode=MARKET_INVENTORY_MODE,
        source_inventory_phase="scanning",
        source_inventory_high_water_primary_key=high_water,
        source_inventory_last_primary_key=[checkpoint] if checkpoint is not None else None,
        source_inventory_rows_scanned=source_rows,
        source_inventory_batch_size=MARKET_INVENTORY_BATCH_SIZE,
        source_inventory_accumulator_file=path.name,
        last_progress_at=_now(),
    )
    migration._publish(report, progress_path)

    high_water_id = int(high_water[0]) if high_water else None
    while high_water_id is not None and (checkpoint is None or checkpoint < high_water_id):
        rows = migration._source_read_with_retry(
            source,
            lambda checkpoint=checkpoint: _read_market_inventory_batch(
                source,
                table,
                high_water=high_water,
                checkpoint=checkpoint,
                batch_size=MARKET_INVENTORY_BATCH_SIZE,
            ),
            table_report,
            report,
            progress_path,
            phase="market_source_inventory_batch",
        )
        if not rows:
            break
        checkpoint, source_rows = _accumulate_batch(
            path,
            rows,
            previous_source_rows=source_rows,
        )
        table_report.update(
            source_inventory_last_primary_key=[checkpoint],
            source_inventory_rows_scanned=source_rows,
            last_progress_at=_now(),
        )
        migration._publish(report, progress_path)

    inventory = _read_finalized_inventory(path, high_water)
    summary_source = "durable_cache"
    if inventory is None:
        inventory = _finalize_inventory(path)
        _persist_finalized_inventory(path, high_water, inventory)
        _evict_inventory_page_cache(path)
        summary_source = "exact_recompute"

    table_report.update(
        source_inventory_phase="verified",
        source_inventory_last_primary_key=high_water,
        source_inventory_rows_scanned=inventory["source_rows"],
        source_inventory_lineage_count=inventory["lineage_count"],
        source_inventory_final_summary_version=MARKET_INVENTORY_FINAL_SUMMARY_VERSION,
        source_inventory_final_summary_source=summary_source,
        source_inventory_completed_at=_now(),
        last_progress_at=_now(),
    )
    migration._publish(report, progress_path)
    return inventory


def install_bounded_market_inventory(migration: Any) -> None:
    # Stage 1 already has bounded, fail-closed retry budgets for source reads. Treat a
    # PostgreSQL TCP refusal as the same transient transport class as reset/EOF/recovery
    # so the next attempt uses a fresh pool while preserving every durable checkpoint.
    marker = "connection refused"
    if marker not in migration._TRANSIENT_SOURCE_READ_MARKERS:
        migration._TRANSIENT_SOURCE_READ_MARKERS = (
            *migration._TRANSIENT_SOURCE_READ_MARKERS,
            marker,
        )

    def replacement(
        source: Engine,
        table: Table,
        table_report: dict[str, Any],
        report: dict[str, Any],
        progress_path: Path,
        *,
        high_water: list[object] | None,
    ) -> dict[str, object]:
        return bounded_market_source_inventory(
            migration,
            source,
            table,
            table_report,
            report,
            progress_path,
            high_water=high_water,
        )

    replacement._cie_bounded_market_inventory = True  # type: ignore[attr-defined]
    migration._market_source_inventory_with_retry = replacement


__all__ = [
    "MARKET_INVENTORY_BATCH_SIZE",
    "MARKET_INVENTORY_FINAL_SUMMARY_VERSION",
    "MARKET_INVENTORY_MODE",
    "bounded_market_source_inventory",
    "install_bounded_market_inventory",
]
