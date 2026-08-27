from __future__ import annotations

import os
import time
from typing import Any

from sqlalchemy import text

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_history import (
    DEFAULT_MIGRATION_HEARTBEAT_BATCH,
    SOURCE_COVERAGE_HISTORY_TABLE,
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


def _completed_history_summary(store: Any) -> dict[str, int]:
    """Publish final archive counts once, off the HTTP request path."""

    with store.engine.connect() as db:
        lane_count = int(
            db.execute(
                text(
                    f"SELECT COUNT(DISTINCT lane_id) FROM {SOURCE_COVERAGE_HISTORY_TABLE}"
                )
            ).scalar_one()
            or 0
        )
        snapshot_count = int(
            db.execute(
                text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_HISTORY_TABLE}")
            ).scalar_one()
            or 0
        )
    return {
        "lane_count": lane_count,
        "snapshot_count": snapshot_count,
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
        # The migration child owns this one-time aggregate read. Certification later
        # consumes the published counts from the worker heartbeat and never recounts
        # the append-only archive on a browser request.
        result.update(_completed_history_summary(store))
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
