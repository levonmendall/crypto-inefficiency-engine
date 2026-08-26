from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_history import (
    DEFAULT_MIGRATION_HEARTBEAT_BATCH,
    SOURCE_COVERAGE_HISTORY_TABLE,
    backfill_source_coverage_history_from_heartbeats,
)


SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID = "canonical-source-coverage-history-migration"
MIGRATION_INCOMPLETE_EXIT_CODE = 3
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


def advance_one_history_migration_batch(store: Any, *, max_heartbeats: int | None = None) -> dict[str, object]:
    """Advance one small transactional archive batch and publish exact progress."""

    batch = migration_batch_size() if max_heartbeats is None else max(1, int(max_heartbeats))
    result = dict(
        backfill_source_coverage_history_from_heartbeats(
            store,
            max_heartbeats=batch,
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

    try:
        result = advance_one_history_migration_batch(store)
    except Exception as exc:
        _record(
            store,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "stage": "canonical_history_archive_migration_failed",
                "message": str(exc)[:1000],
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
    "SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID",
    "_completed_history_summary",
    "advance_one_history_migration_batch",
    "migration_batch_size",
]
