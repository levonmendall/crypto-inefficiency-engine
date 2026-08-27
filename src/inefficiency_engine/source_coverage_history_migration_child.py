from __future__ import annotations

import json
import os
import time
from typing import Any

from sqlalchemy import text

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage import SourceCoverageSnapshot
from inefficiency_engine.source_coverage_history import (
    DEFAULT_MIGRATION_HEARTBEAT_BATCH,
    SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    SourceCoverageHistoryLedger,
)
from inefficiency_engine.source_coverage_history_batch_repair import (
    migrate_source_coverage_history_batch_with_ledger,
)
from inefficiency_engine.worker_heartbeat_index_gate import (
    worker_heartbeat_priority_index_status,
)


SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID = "canonical-source-coverage-history-migration"
MIGRATION_INCOMPLETE_EXIT_CODE = 3
MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE = 78
DEFAULT_CHILD_MIGRATION_BATCH = min(50, DEFAULT_MIGRATION_HEARTBEAT_BATCH)


def migration_batch_size() -> int:
    raw = os.getenv(
        "CIE_SOURCE_COVERAGE_HISTORY_MIGRATION_CHILD_BATCH",
        str(DEFAULT_CHILD_MIGRATION_BATCH),
    )
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_CHILD_MIGRATION_BATCH
    return max(1, min(DEFAULT_MIGRATION_HEARTBEAT_BATCH, value))


def _record(store: Any, *, state: str, detail: dict[str, object], error_type: str | None = None) -> None:
    try:
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={
                **detail,
                "migration_owner": "independent-bounded-history-child",
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "candidate_level_history_synthesized": False,
                "historical_counts_as_forward": False,
                "qualification_thresholds_unchanged": True,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def _completed_history_summary(
    store: Any,
    *,
    checkpoint_heartbeat_id: int,
) -> dict[str, object]:
    """Prove compact migration readiness without recounting the archive.

    The priority ``worker_heartbeats(worker_id,id)`` access path is already a migration
    prerequisite. Read only the newest canonical source snapshot, verify that the
    durable migration checkpoint has caught up to that heartbeat, and derive the 13-lane
    certification count from the snapshot payload itself. Full archive counts remain
    diagnostic-only and are deliberately deferred.
    """

    with store.engine.connect() as db:
        row = db.execute(
            text(
                "SELECT id, payload_json FROM worker_heartbeats "
                "WHERE worker_id = :worker_id ORDER BY id DESC LIMIT 1"
            ),
            {"worker_id": SOURCE_COVERAGE_SNAPSHOT_WORKER_ID},
        ).mappings().first()

    if row is None:
        return {
            "compact_certification_summary": False,
            "compact_summary_reason": "canonical_source_snapshot_unavailable",
            "lane_count": 0,
            "snapshot_count": 0,
            "archive_snapshot_count_deferred": True,
            "summary_scope": "latest_canonical_source_snapshot",
        }

    latest_heartbeat_id = int(row["id"])
    caught_up = int(checkpoint_heartbeat_id) >= latest_heartbeat_id
    try:
        heartbeat_payload = json.loads(str(row["payload_json"]))
        detail = heartbeat_payload.get("detail") if isinstance(heartbeat_payload, dict) else None
        snapshot_payload = detail.get("snapshot") if isinstance(detail, dict) else None
        if not isinstance(snapshot_payload, dict):
            raise ValueError("snapshot missing")
        snapshot = SourceCoverageSnapshot.model_validate(snapshot_payload)
        lane_count = len(snapshot.lanes)
    except Exception as exc:
        return {
            "compact_certification_summary": False,
            "compact_summary_reason": "canonical_source_snapshot_invalid",
            "compact_summary_error_type": type(exc).__name__,
            "lane_count": 0,
            "snapshot_count": 0,
            "summary_heartbeat_id": latest_heartbeat_id,
            "checkpoint_heartbeat_id": int(checkpoint_heartbeat_id),
            "archive_snapshot_count_deferred": True,
            "summary_scope": "latest_canonical_source_snapshot",
        }

    return {
        "compact_certification_summary": caught_up,
        "compact_summary_reason": (
            "checkpoint_covers_latest_canonical_source_snapshot"
            if caught_up
            else "source_snapshot_tail_advanced_after_batch"
        ),
        "lane_count": lane_count,
        "snapshot_count": 0,
        "summary_heartbeat_id": latest_heartbeat_id,
        "checkpoint_heartbeat_id": int(checkpoint_heartbeat_id),
        "migration_checkpoint_covers_summary": caught_up,
        "archive_snapshot_count_deferred": True,
        "summary_scope": "latest_canonical_source_snapshot",
        "request_time_archive_count_queries": 0,
    }


def advance_one_history_migration_batch(
    store: Any,
    *,
    ledger: SourceCoverageHistoryLedger | None = None,
    max_heartbeats: int | None = None,
) -> dict[str, object]:
    """Advance one small transactional archive batch and publish exact progress."""

    batch = migration_batch_size() if max_heartbeats is None else max(1, int(max_heartbeats))
    active_ledger = ledger or SourceCoverageHistoryLedger(store)

    def progress(stage: str, detail: dict[str, object]) -> None:
        _record(
            store,
            state="running",
            detail={
                "stage": stage,
                "batch_limit": batch,
                "compact_certification_summary": False,
                "schema_initialized_outside_archive_batch": ledger is not None,
                **detail,
            },
        )

    result = dict(
        migrate_source_coverage_history_batch_with_ledger(
            store,
            ledger=active_ledger,
            max_heartbeats=batch,
            progress=progress,
        )
    )
    complete = bool(result.get("complete"))
    if complete:
        summary_started = time.monotonic()
        _record(
            store,
            state="running",
            detail={
                "stage": "canonical_history_compact_summary_starting",
                "batch_limit": batch,
                "checkpoint_heartbeat_id": int(result.get("checkpoint_heartbeat_id") or 0),
                "compact_certification_summary": False,
                "archive_snapshot_count_deferred": True,
            },
        )
        summary = _completed_history_summary(
            store,
            checkpoint_heartbeat_id=int(result.get("checkpoint_heartbeat_id") or 0),
        )
        result.update(summary)
        result["compact_summary_runtime_seconds"] = round(
            max(0.0, time.monotonic() - summary_started),
            6,
        )
        complete = bool(summary.get("compact_certification_summary"))
        # A new canonical source heartbeat can arrive after the batch query. In that
        # case the completed checkpoint is truthful but no longer caught up; retry the
        # migration rather than publishing a false terminal-ready heartbeat.
        result["complete"] = complete

    _record(
        store,
        state="success" if complete else "running",
        detail={
            "stage": "canonical_history_ready" if complete else "canonical_history_archive_migrating",
            "batch_limit": batch,
            "compact_certification_summary": complete,
            **result,
        },
    )
    return result


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("source coverage history migration requires durable persistence")

    batch = migration_batch_size()
    _record(
        store,
        state="running",
        detail={
            "stage": "canonical_history_store_opened",
            "batch_limit": batch,
            "raw_history_queries_started": False,
        },
    )

    index_status = worker_heartbeat_priority_index_status(store)
    if not bool(index_status.get("ready")):
        _record(
            store,
            state="running",
            detail={
                "stage": "canonical_history_waiting_for_heartbeat_index",
                "batch_limit": batch,
                "heartbeat_index_status": index_status,
                "raw_history_queries_started": False,
                "migration_prerequisite_ready": False,
                "retrying": True,
            },
        )
        return MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE

    current_stage = "canonical_history_schema_initializing"
    try:
        schema_started = time.monotonic()
        _record(
            store,
            state="running",
            detail={
                "stage": current_stage,
                "batch_limit": batch,
                "heartbeat_index_status": index_status,
                "raw_history_queries_started": False,
                "migration_prerequisite_ready": True,
            },
        )
        ledger = SourceCoverageHistoryLedger(store)
        schema_seconds = max(0.0, time.monotonic() - schema_started)
        current_stage = "canonical_history_schema_ready"
        _record(
            store,
            state="running",
            detail={
                "stage": current_stage,
                "batch_limit": batch,
                "heartbeat_index_status": index_status,
                "raw_history_queries_started": False,
                "migration_prerequisite_ready": True,
                "schema_initialization_seconds": round(schema_seconds, 6),
                "schema_initialized_once_per_child": True,
            },
        )

        current_stage = "canonical_history_archive_batch_starting"
        _record(
            store,
            state="running",
            detail={
                "stage": current_stage,
                "batch_limit": batch,
                "heartbeat_index_status": index_status,
                "raw_history_queries_started": True,
                "migration_prerequisite_ready": True,
                "schema_initialization_seconds": round(schema_seconds, 6),
                "schema_initialized_once_per_child": True,
            },
        )
        result = advance_one_history_migration_batch(
            store,
            ledger=ledger,
            max_heartbeats=batch,
        )
    except Exception as exc:
        _record(
            store,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "stage": "canonical_history_archive_migration_failed",
                "failed_stage": current_stage,
                "message": str(exc)[:1000],
                "heartbeat_index_status": index_status,
                "raw_history_queries_started": current_stage
                not in {
                    "canonical_history_schema_initializing",
                    "canonical_history_schema_ready",
                },
                "retrying": True,
            },
        )
        raise

    return 0 if bool(result.get("complete")) else MIGRATION_INCOMPLETE_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_CHILD_MIGRATION_BATCH",
    "MIGRATION_INCOMPLETE_EXIT_CODE",
    "MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE",
    "SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID",
    "_completed_history_summary",
    "advance_one_history_migration_batch",
    "migration_batch_size",
]
