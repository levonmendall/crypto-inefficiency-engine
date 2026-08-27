from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import select

from inefficiency_engine.source_coverage import SourceCoverageSnapshot
from inefficiency_engine.source_coverage_history import (
    DEFAULT_MIGRATION_HEARTBEAT_BATCH,
    SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    SourceCoverageHistoryLedger,
)


PhaseCallback = Callable[[str, dict[str, object]], None]


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


def _emit(
    progress: PhaseCallback | None,
    stage: str,
    *,
    timings: dict[str, float],
    **detail: object,
) -> None:
    if progress is None:
        return
    progress(
        stage,
        {
            "batch_phase_timings_seconds": {
                key: round(max(0.0, float(value)), 6)
                for key, value in timings.items()
            },
            **detail,
        },
    )


def migrate_source_coverage_history_batch_with_ledger(
    store: Any,
    *,
    ledger: SourceCoverageHistoryLedger,
    start: datetime | None = None,
    max_heartbeats: int = DEFAULT_MIGRATION_HEARTBEAT_BATCH,
    progress: PhaseCallback | None = None,
) -> dict[str, object]:
    """Migrate one bounded batch without reconstructing or re-DDLing the ledger.

    The caller owns schema initialization and passes the already-initialized ledger.
    This keeps SQLAlchemy ``metadata.create_all`` completely outside the actual bounded
    archive batch while preserving the existing atomic insert+checkpoint transaction.
    """

    timings: dict[str, float] = {}
    batch_started = time.monotonic()

    phase_started = time.monotonic()
    status = ledger.migration_status()
    checkpoint = int(status["checkpoint_heartbeat_id"] or 0)
    timings["checkpoint_read"] = time.monotonic() - phase_started
    _emit(
        progress,
        "canonical_history_checkpoint_read_complete",
        timings=timings,
        checkpoint_heartbeat_id=checkpoint,
    )

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
        query = query.where(
            store.worker_heartbeats.c.observed_at >= _utc(start).isoformat()
        )

    phase_started = time.monotonic()
    with store.engine.connect() as db:
        archive_rows = list(db.execute(query).mappings())
    timings["heartbeat_query"] = time.monotonic() - phase_started
    _emit(
        progress,
        "canonical_history_heartbeat_query_complete",
        timings=timings,
        heartbeat_rows_selected=len(archive_rows),
        checkpoint_heartbeat_id=checkpoint,
    )

    has_more = len(archive_rows) > bounded
    archive_rows = archive_rows[:bounded]
    lane_rows: list[dict[str, object]] = []
    migrated_heartbeats = 0
    invalid_heartbeats = 0
    latest_heartbeat_id = checkpoint

    phase_started = time.monotonic()
    for row in archive_rows:
        latest_heartbeat_id = max(latest_heartbeat_id, int(row["id"]))
        try:
            heartbeat_payload = json.loads(str(row["payload_json"]))
            detail = (
                heartbeat_payload.get("detail")
                if isinstance(heartbeat_payload, dict)
                else None
            )
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
    timings["payload_parse"] = time.monotonic() - phase_started
    _emit(
        progress,
        "canonical_history_payload_parse_complete",
        timings=timings,
        migrated_heartbeats=migrated_heartbeats,
        invalid_heartbeats=invalid_heartbeats,
        pending_lane_rows=len(lane_rows),
        checkpoint_heartbeat_id=latest_heartbeat_id,
    )

    migration_complete = not has_more
    now_text = datetime.now(timezone.utc).isoformat()
    _emit(
        progress,
        "canonical_history_history_write_starting",
        timings=timings,
        pending_lane_rows=len(lane_rows),
        checkpoint_heartbeat_id=latest_heartbeat_id,
        migration_complete=migration_complete,
    )

    transaction_started = time.monotonic()
    with store.engine.begin() as db:
        insert_started = time.monotonic()
        inserted_lane_snapshots = ledger._insert_missing_rows(db, lane_rows)
        timings["history_insert"] = time.monotonic() - insert_started

        checkpoint_started = time.monotonic()
        ledger._upsert_migration_checkpoint(
            db,
            checkpoint_heartbeat_id=latest_heartbeat_id,
            complete=migration_complete,
            updated_at=now_text,
        )
        timings["checkpoint_upsert"] = time.monotonic() - checkpoint_started
    timings["history_transaction_commit"] = time.monotonic() - transaction_started
    timings["batch_total"] = time.monotonic() - batch_started

    _emit(
        progress,
        "canonical_history_checkpoint_commit_complete",
        timings=timings,
        inserted_lane_snapshots=inserted_lane_snapshots,
        checkpoint_heartbeat_id=latest_heartbeat_id,
        migration_complete=migration_complete,
    )

    return {
        "complete": migration_complete,
        "checkpoint_heartbeat_id": latest_heartbeat_id,
        "migrated_heartbeats": migrated_heartbeats,
        "inserted_lane_snapshots": inserted_lane_snapshots,
        "invalid_heartbeats": invalid_heartbeats,
        "batch_limit": bounded,
        "batch_phase_timings_seconds": {
            key: round(max(0.0, float(value)), 6)
            for key, value in timings.items()
        },
        "schema_initialized_outside_archive_batch": True,
        "preinitialized_ledger_reused": True,
        "conflict_safe_history_insert": True,
        "conflict_safe_checkpoint_upsert": True,
        "paper_only": True,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


__all__ = ["migrate_source_coverage_history_batch_with_ledger"]
