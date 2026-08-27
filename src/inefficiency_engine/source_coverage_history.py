from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
    insert,
    inspect,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from inefficiency_engine.source_coverage import SourceCoverageSnapshot


SOURCE_COVERAGE_HISTORY_TABLE = "source_coverage_history"
SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE = "source_coverage_history_migrations"
SOURCE_COVERAGE_SNAPSHOT_WORKER_ID = "canonical-source-coverage-snapshot"
MIGRATION_NAME = "worker_heartbeat_snapshot_archive"
DEFAULT_MIGRATION_HEARTBEAT_BATCH = 250


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _snapshot_key(snapshot_observed_at: datetime, lane_id: str) -> str:
    raw = f"{_utc(snapshot_observed_at).isoformat()}|{lane_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


class SourceCoverageHistoryLedger:
    """Canonical append-only lane history derived from complete source snapshots."""

    def __init__(self, store: Any):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            SOURCE_COVERAGE_HISTORY_TABLE,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_key", String(64), nullable=False, unique=True),
            Column("snapshot_observed_at", Text, nullable=False),
            Column("published_at", Text, nullable=False),
            Column("heartbeat_id", Integer),
            Column("lane_id", Text, nullable=False),
            Column("required_evidence_class_count", Integer, nullable=False),
            Column("covered_evidence_class_count", Integer, nullable=False),
            Column("healthy_source_count", Integer, nullable=False),
            Column("evidence_class_coverage_satisfied", Boolean, nullable=False),
            Column("source_layer_sufficient", Boolean, nullable=False),
            Column("covered_evidence_classes_json", Text, nullable=False),
            Column("admitted_source_ids_json", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        self.migrations = Table(
            SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE,
            metadata,
            Column("migration_name", String(96), primary_key=True),
            Column("checkpoint_heartbeat_id", Integer, nullable=False),
            Column("complete", Boolean, nullable=False),
            Column("updated_at", Text, nullable=False),
        )
        Index(
            "ix_source_coverage_history_lane_time",
            self.rows.c.lane_id,
            self.rows.c.snapshot_observed_at,
        )
        Index(
            "ix_source_coverage_history_heartbeat",
            self.rows.c.heartbeat_id,
        )
        try:
            metadata.create_all(store.engine)
        except Exception:
            # API reads and the short-lived source executor can be the first process
            # to touch this release at the same time. SQLAlchemy's check-first DDL is
            # not a cross-process lock, so tolerate the race only when both canonical
            # tables now exist; any genuine schema failure remains fatal to this reader.
            available = set(inspect(store.engine).get_table_names())
            required = {
                SOURCE_COVERAGE_HISTORY_TABLE,
                SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE,
            }
            if not required <= available:
                raise

    def _snapshot_rows(
        self,
        snapshot: SourceCoverageSnapshot,
        *,
        published_at: datetime,
        heartbeat_id: int | None,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        snapshot_time = _utc(snapshot.observed_at)
        published = _utc(published_at)
        for lane in snapshot.lanes:
            payload = lane.model_dump(mode="json")
            admitted_source_ids = sorted(
                {
                    str(source.get("source_id") or "")
                    for source in lane.sources
                    if bool(source.get("admitted")) and str(source.get("source_id") or "")
                }
            )
            covered = sorted(str(value) for value in lane.covered_evidence_classes)
            payload_json = _json(payload)
            rows.append(
                {
                    "snapshot_key": _snapshot_key(snapshot_time, lane.lane_id),
                    "snapshot_observed_at": snapshot_time.isoformat(),
                    "published_at": published.isoformat(),
                    "heartbeat_id": heartbeat_id,
                    "lane_id": lane.lane_id,
                    "required_evidence_class_count": len(lane.required_evidence_classes),
                    "covered_evidence_class_count": len(covered),
                    "healthy_source_count": int(lane.healthy_source_count),
                    "evidence_class_coverage_satisfied": bool(
                        lane.evidence_class_coverage_satisfied
                    ),
                    "source_layer_sufficient": bool(lane.source_layer_sufficient),
                    "covered_evidence_classes_json": _json(covered),
                    "admitted_source_ids_json": _json(admitted_source_ids),
                    "payload_json": payload_json,
                    "lineage_hash": hashlib.sha256(payload_json.encode()).hexdigest(),
                }
            )
        return rows

    @staticmethod
    def _rowcount(result: Any) -> int:
        try:
            value = int(result.rowcount)
        except (AttributeError, TypeError, ValueError):
            return 0
        return max(0, value)

    def _insert_missing_rows(self, db: Any, rows: list[dict[str, object]]) -> int:
        """Insert canonical snapshots atomically when a live writer races migration.

        Production PostgreSQL and test SQLite both use native conflict handling. The
        old SELECT-existing-then-INSERT sequence allowed the live snapshot publisher and
        archive migration child to observe the same key as absent, after which one would
        fail with a uniqueness IntegrityError. The unique snapshot_key remains the source
        of idempotency; conflicts are now resolved by the database in the INSERT itself.
        """

        if not rows:
            return 0

        dialect = str(getattr(db.dialect, "name", ""))
        if dialect == "postgresql":
            statement = postgresql_insert(self.rows).on_conflict_do_nothing(
                index_elements=[self.rows.c.snapshot_key]
            )
            return self._rowcount(db.execute(statement, rows))
        if dialect == "sqlite":
            statement = sqlite_insert(self.rows).on_conflict_do_nothing(
                index_elements=[self.rows.c.snapshot_key]
            )
            return self._rowcount(db.execute(statement, rows))

        # Other dialects are not production targets. Preserve the prior idempotent
        # behavior for portability without weakening PostgreSQL/SQLite race safety.
        keys = [str(row["snapshot_key"]) for row in rows]
        existing: set[str] = set()
        for offset in range(0, len(keys), 500):
            existing.update(
                str(value)
                for value in db.execute(
                    select(self.rows.c.snapshot_key).where(
                        self.rows.c.snapshot_key.in_(keys[offset : offset + 500])
                    )
                ).scalars()
            )
        pending = [row for row in rows if str(row["snapshot_key"]) not in existing]
        if pending:
            db.execute(insert(self.rows), pending)
        return len(pending)

    def _upsert_migration_checkpoint(
        self,
        db: Any,
        *,
        checkpoint_heartbeat_id: int,
        complete: bool,
        updated_at: str,
    ) -> None:
        """Advance the archive checkpoint without a select-before-insert race.

        Concurrent bounded migration children are not expected, but deployment overlap
        can briefly create them. Native UPSERT makes creation atomic and the WHERE clause
        prevents a slower older child from moving the checkpoint backwards.
        """

        payload = {
            "migration_name": MIGRATION_NAME,
            "checkpoint_heartbeat_id": int(checkpoint_heartbeat_id),
            "complete": bool(complete),
            "updated_at": updated_at,
        }
        dialect = str(getattr(db.dialect, "name", ""))
        if dialect == "postgresql":
            statement = postgresql_insert(self.migrations).values(payload)
            excluded = statement.excluded
            statement = statement.on_conflict_do_update(
                index_elements=[self.migrations.c.migration_name],
                set_={
                    "checkpoint_heartbeat_id": excluded.checkpoint_heartbeat_id,
                    "complete": excluded.complete,
                    "updated_at": excluded.updated_at,
                },
                where=(
                    excluded.checkpoint_heartbeat_id
                    >= self.migrations.c.checkpoint_heartbeat_id
                ),
            )
            db.execute(statement)
            return
        if dialect == "sqlite":
            statement = sqlite_insert(self.migrations).values(payload)
            excluded = statement.excluded
            statement = statement.on_conflict_do_update(
                index_elements=[self.migrations.c.migration_name],
                set_={
                    "checkpoint_heartbeat_id": excluded.checkpoint_heartbeat_id,
                    "complete": excluded.complete,
                    "updated_at": excluded.updated_at,
                },
                where=(
                    excluded.checkpoint_heartbeat_id
                    >= self.migrations.c.checkpoint_heartbeat_id
                ),
            )
            db.execute(statement)
            return

        # Non-production fallback: update-first minimizes the insert race while keeping
        # compatibility with SQLAlchemy dialects that do not expose ON CONFLICT helpers.
        result = db.execute(
            update(self.migrations)
            .where(self.migrations.c.migration_name == MIGRATION_NAME)
            .where(
                self.migrations.c.checkpoint_heartbeat_id
                <= int(checkpoint_heartbeat_id)
            )
            .values(
                checkpoint_heartbeat_id=int(checkpoint_heartbeat_id),
                complete=bool(complete),
                updated_at=updated_at,
            )
        )
        if self._rowcount(result) == 0:
            existing = db.execute(
                select(self.migrations.c.migration_name).where(
                    self.migrations.c.migration_name == MIGRATION_NAME
                )
            ).scalar_one_or_none()
            if existing is None:
                db.execute(insert(self.migrations), payload)

    def record_snapshot(
        self,
        snapshot: SourceCoverageSnapshot,
        *,
        published_at: datetime | None = None,
        heartbeat_id: int | None = None,
    ) -> int:
        rows = self._snapshot_rows(
            snapshot,
            published_at=published_at or datetime.now(timezone.utc),
            heartbeat_id=heartbeat_id,
        )
        with self.store.engine.begin() as db:
            return self._insert_missing_rows(db, rows)

    def migration_status(self) -> dict[str, object]:
        with self.store.engine.connect() as db:
            row = db.execute(
                select(
                    self.migrations.c.checkpoint_heartbeat_id,
                    self.migrations.c.complete,
                    self.migrations.c.updated_at,
                ).where(self.migrations.c.migration_name == MIGRATION_NAME)
            ).mappings().first()
        if row is None:
            return {
                "checkpoint_heartbeat_id": 0,
                "complete": False,
                "updated_at": None,
            }
        return {
            "checkpoint_heartbeat_id": int(row["checkpoint_heartbeat_id"] or 0),
            "complete": bool(row["complete"]),
            "updated_at": row["updated_at"],
        }

    def first_snapshot_at(self) -> datetime | None:
        with self.store.engine.connect() as db:
            value = db.execute(select(func.min(self.rows.c.snapshot_observed_at))).scalar_one_or_none()
        return _parse_time(value)

    def summary(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> dict[str, dict[str, object]]:
        start_text = _utc(start).isoformat()
        end_text = _utc(end).isoformat()
        aggregate = (
            select(
                self.rows.c.lane_id,
                func.count().label("snapshot_count"),
                func.sum(self.rows.c.healthy_source_count).label("source_count"),
                func.min(self.rows.c.snapshot_observed_at).label("earliest"),
                func.max(self.rows.c.snapshot_observed_at).label("latest"),
            )
            .where(self.rows.c.snapshot_observed_at >= start_text)
            .where(self.rows.c.snapshot_observed_at < end_text)
            .group_by(self.rows.c.lane_id)
        )
        distinct_truth = (
            select(
                self.rows.c.lane_id,
                self.rows.c.covered_evidence_classes_json,
                self.rows.c.admitted_source_ids_json,
            )
            .where(self.rows.c.snapshot_observed_at >= start_text)
            .where(self.rows.c.snapshot_observed_at < end_text)
            .distinct()
        )
        with self.store.engine.connect() as db:
            aggregate_rows = list(db.execute(aggregate).mappings())
            truth_rows = list(db.execute(distinct_truth).mappings())

        result: dict[str, dict[str, object]] = {}
        for row in aggregate_rows:
            lane_id = str(row["lane_id"])
            result[lane_id] = {
                "source_count": int(row["source_count"] or 0),
                "source_earliest": _parse_time(row["earliest"]),
                "source_latest": _parse_time(row["latest"]),
                "source_ids": set(),
                "evidence_classes": set(),
                "source_ledgers": {SOURCE_COVERAGE_HISTORY_TABLE},
                "canonical_snapshot_count": int(row["snapshot_count"] or 0),
            }
        for row in truth_rows:
            lane_id = str(row["lane_id"])
            state = result.get(lane_id)
            if state is None:
                continue
            try:
                classes = json.loads(str(row["covered_evidence_classes_json"] or "[]"))
            except (TypeError, json.JSONDecodeError):
                classes = []
            try:
                source_ids = json.loads(str(row["admitted_source_ids_json"] or "[]"))
            except (TypeError, json.JSONDecodeError):
                source_ids = []
            if isinstance(classes, list):
                state["evidence_classes"].update(str(value) for value in classes)
            if isinstance(source_ids, list):
                state["source_ids"].update(str(value) for value in source_ids)
        return result


def persist_source_coverage_history_snapshot(
    store: Any,
    snapshot: SourceCoverageSnapshot,
    *,
    published_at: datetime | None = None,
) -> int:
    return SourceCoverageHistoryLedger(store).record_snapshot(
        snapshot,
        published_at=published_at,
    )


def backfill_source_coverage_history_from_heartbeats(
    store: Any,
    *,
    start: datetime | None = None,
    max_heartbeats: int = DEFAULT_MIGRATION_HEARTBEAT_BATCH,
) -> dict[str, object]:
    """Migrate one bounded archive batch with the checkpoint in the same transaction."""

    ledger = SourceCoverageHistoryLedger(store)
    status = ledger.migration_status()
    checkpoint = int(status["checkpoint_heartbeat_id"] or 0)
    bounded = max(1, min(int(max_heartbeats), 2000))
    query = (
        select(
            store.worker_heartbeats.c.id,
            store.worker_heartbeats.c.observed_at,
            store.worker_heartbeats.c.payload_json,
        )
        .where(store.worker_heartbeats.c.worker_id == SOURCE_COVERAGE_SNAPSHOT_WORKER_ID)
        .where(store.worker_heartbeats.c.id > checkpoint)
        .order_by(store.worker_heartbeats.c.id)
        .limit(bounded + 1)
    )
    if start is not None:
        query = query.where(store.worker_heartbeats.c.observed_at >= _utc(start).isoformat())
    with store.engine.connect() as db:
        archive_rows = list(db.execute(query).mappings())

    has_more = len(archive_rows) > bounded
    archive_rows = archive_rows[:bounded]
    lane_rows: list[dict[str, object]] = []
    migrated_heartbeats = 0
    invalid_heartbeats = 0
    latest_heartbeat_id = checkpoint
    for row in archive_rows:
        latest_heartbeat_id = max(latest_heartbeat_id, int(row["id"]))
        try:
            heartbeat_payload = json.loads(str(row["payload_json"]))
            detail = heartbeat_payload.get("detail") if isinstance(heartbeat_payload, dict) else None
            snapshot_payload = detail.get("snapshot") if isinstance(detail, dict) else None
            if not isinstance(snapshot_payload, dict):
                raise ValueError("snapshot missing")
            snapshot = SourceCoverageSnapshot.model_validate(snapshot_payload)
        except Exception:
            invalid_heartbeats += 1
            continue
        published_at = _parse_time(row["observed_at"]) or snapshot.observed_at
        lane_rows.extend(
            ledger._snapshot_rows(
                snapshot,
                published_at=published_at,
                heartbeat_id=int(row["id"]),
            )
        )
        migrated_heartbeats += 1

    migration_complete = not has_more
    now_text = datetime.now(timezone.utc).isoformat()
    with store.engine.begin() as db:
        inserted_lane_snapshots = ledger._insert_missing_rows(db, lane_rows)
        ledger._upsert_migration_checkpoint(
            db,
            checkpoint_heartbeat_id=latest_heartbeat_id,
            complete=migration_complete,
            updated_at=now_text,
        )

    return {
        "complete": migration_complete,
        "checkpoint_heartbeat_id": latest_heartbeat_id,
        "migrated_heartbeats": migrated_heartbeats,
        "inserted_lane_snapshots": inserted_lane_snapshots,
        "invalid_heartbeats": invalid_heartbeats,
        "batch_limit": bounded,
        "conflict_safe_history_insert": True,
        "conflict_safe_checkpoint_upsert": True,
        "paper_only": True,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


__all__ = [
    "DEFAULT_MIGRATION_HEARTBEAT_BATCH",
    "SOURCE_COVERAGE_HISTORY_TABLE",
    "SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE",
    "SourceCoverageHistoryLedger",
    "backfill_source_coverage_history_from_heartbeats",
    "persist_source_coverage_history_snapshot",
]
