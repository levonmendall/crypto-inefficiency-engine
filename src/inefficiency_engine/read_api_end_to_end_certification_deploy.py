from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException

from inefficiency_engine import read_api_certification_fast_readiness as active
from inefficiency_engine import read_api_lane_history_ui_deploy as inner
from inefficiency_engine.critical_evidence_recovery import (
    DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
)


app = inner.app
_CYCLE_HISTORY_BACKFILL_WORKER_ID = "cycle-history-background-backfill"
_CYCLE_HISTORY_BACKFILL_STALE_SECONDS = 180.0
_ALPHA_RESEARCH_WORKER_ID = "shadow-research-auxiliary"


def _worker(workers: object, name: str) -> dict[str, object]:
    if not isinstance(workers, dict):
        return {}
    row = workers.get(name)
    return dict(row) if isinstance(row, dict) else {}


def _fresh_worker(row: dict[str, object], *, allowed_states: set[str]) -> bool:
    return bool(
        row.get("available")
        and not row.get("stale")
        and str(row.get("state") or "") in allowed_states
    )


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _alpha_forward_status_from_research_worker(
    research: dict[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Re-age the research worker's already-published alpha recovery truth.

    The disposable research process performs the expensive durable alpha-marker lookup
    as part of its normal background cycle and carries the resulting recovery snapshot
    in its heartbeat. Certification consumes that compact publication instead of doing
    a request-time JSON LIKE scan over the append-only heartbeat ledger.
    """

    recovery = research.get("critical_evidence_recovery")
    recovery = dict(recovery) if isinstance(recovery, dict) else {}
    recovery_workers = recovery.get("workers")
    recovery_workers = dict(recovery_workers) if isinstance(recovery_workers, dict) else {}
    raw = recovery_workers.get("alpha_forward")
    raw = dict(raw) if isinstance(raw, dict) else {}

    # A research heartbeat may also carry the successful alpha marker directly after
    # the evidence phase. Use that compact marker only when the cycle-start recovery
    # snapshot is absent; either source remains durable worker-published truth.
    if not raw and research.get("alpha_forward_evidence_cycle_id"):
        raw = {
            "worker_id": _ALPHA_RESEARCH_WORKER_ID,
            "signal": "alpha_forward_evidence_cycle_id",
            "available": True,
            "observed_at": research.get("observed_at"),
            "state": research.get("state"),
            "cycle_id": research.get("alpha_forward_evidence_cycle_id"),
            "recovery_after_seconds": DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
        }

    if not raw or not bool(raw.get("available")):
        return {
            "worker_id": _ALPHA_RESEARCH_WORKER_ID,
            "signal": "alpha_forward_evidence_cycle_id",
            "available": False,
            "recovery_required": True,
            "reason": "compact_alpha_forward_status_unavailable",
            "error_type": raw.get("error_type") if raw else None,
        }

    observed_at = _parse_time(raw.get("observed_at"))
    if observed_at is None:
        return {
            "worker_id": _ALPHA_RESEARCH_WORKER_ID,
            "signal": "alpha_forward_evidence_cycle_id",
            "available": False,
            "recovery_required": True,
            "reason": "compact_alpha_forward_timestamp_unavailable",
        }

    try:
        recovery_after_seconds = max(
            60.0,
            float(
                raw.get("recovery_after_seconds")
                or DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS
            ),
        )
    except (TypeError, ValueError):
        recovery_after_seconds = float(DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS)
    age_seconds = max(
        0.0,
        (now.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
    )
    stale = age_seconds > recovery_after_seconds
    return {
        "worker_id": str(raw.get("worker_id") or _ALPHA_RESEARCH_WORKER_ID),
        "signal": str(raw.get("signal") or "alpha_forward_evidence_cycle_id"),
        "available": True,
        "recovery_required": stale,
        "reason": "alpha_forward_marker_stale" if stale else "alpha_forward_marker_current",
        "age_seconds": age_seconds,
        "observed_at": observed_at.isoformat(),
        "state": raw.get("state"),
        "cycle_id": raw.get("cycle_id"),
        "error_type": raw.get("error_type"),
        "recovery_after_seconds": recovery_after_seconds,
        "source": "research_worker_compact_recovery_snapshot",
    }


def _source_history_status_from_worker(worker: dict[str, object]) -> dict[str, object]:
    """Consume the migration child's final archive summary without recounting tables."""

    available = bool(worker.get("available"))
    complete = bool(
        available
        and str(worker.get("state") or "") == "success"
        and str(worker.get("stage") or "") == "canonical_history_ready"
        and worker.get("complete") is True
        and worker.get("compact_certification_summary") is True
    )
    lane_count = int(worker.get("lane_count") or 0) if available else 0
    snapshot_count = int(worker.get("snapshot_count") or 0) if available else 0
    return {
        "available": available,
        "migration_complete": complete,
        "checkpoint_heartbeat_id": int(worker.get("checkpoint_heartbeat_id") or 0),
        "updated_at": worker.get("observed_at"),
        "lane_count": lane_count,
        "snapshot_count": snapshot_count,
        "reason": (
            "complete"
            if complete
            else "compact_history_summary_pending"
            if available
            else "migration_heartbeat_unavailable"
        ),
        "error_type": worker.get("error_type"),
        "request_time_archive_count_queries": 0,
    }


def _cycle_history_backfill_status_from_worker(worker: dict[str, object]) -> dict[str, object]:
    """Expose the already-batched background bootstrap heartbeat without another read."""

    if not bool(worker.get("available")):
        return {
            "available": False,
            "stale": True,
            "cache_complete": False,
            "progress": {},
            "error_type": worker.get("error_type"),
            "certification_authority": False,
        }

    raw_progress = worker.get("progress")
    progress = dict(raw_progress) if isinstance(raw_progress, dict) else {}
    stale = bool(worker.get("stale", True))
    cache_complete = bool(worker.get("cache_complete") or progress.get("complete"))
    serving_scan_id = progress.get("serving_scan_id") or worker.get("serving_scan_id")
    return {
        "available": True,
        "state": worker.get("state"),
        "error_type": worker.get("error_type"),
        "observed_at": worker.get("observed_at"),
        "age_seconds": worker.get("age_seconds"),
        "stale": stale,
        "stage": worker.get("stage"),
        "cache_complete": cache_complete,
        "first_certified_target_pending": bool(
            worker.get("first_certified_target_pending", not cache_complete)
        ),
        "serving_scan_id": serving_scan_id,
        "progress": progress,
        "certification_authority": False,
    }


def end_to_end_certification_payload(
    *,
    include_worker_truth: bool = False,
) -> dict[str, object]:
    """Return fail-closed certification from one compact durable worker snapshot.

    The request performs readiness/ping plus one batched latest-heartbeat query. Alpha
    freshness, source-history migration, cycle-history backfill, and downstream truth
    are then derived in memory from that same snapshot. No request-time archive recount,
    JSON heartbeat scan, provider work, qualification, allocation, or execution occurs.
    """

    try:
        ready = dict(active.deployment_readiness())
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "end-to-end certification readiness is unavailable",
                "error_type": type(exc).__name__,
            },
        ) from exc

    runtime = ready.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else {}
    workers = dict(workers) if isinstance(workers, dict) else {}
    control = _worker(workers, "canonical_control")
    portfolio = _worker(workers, "portfolio")
    source = _worker(workers, "permanent_source")
    mechanism = _worker(workers, "mechanism_forward")
    research = _worker(workers, "research")
    source_snapshot = _worker(workers, "source_coverage_snapshot")
    research_projection = _worker(workers, "research_projection")
    index_maintenance = _worker(workers, "runtime_index_maintenance")
    source_history_worker = _worker(workers, "source_history_migration")
    cycle_history_worker = _worker(workers, "cycle_history_backfill")

    now = datetime.now(timezone.utc)
    alpha_forward = _alpha_forward_status_from_research_worker(research, now=now)
    source_history = _source_history_status_from_worker(source_history_worker)
    cycle_history_backfill = _cycle_history_backfill_status_from_worker(
        cycle_history_worker
    )
    control_cycle_complete = bool(
        control.get("cycle_history_cache_complete")
        or control.get("historical_cache_complete")
    )
    background_cycle_complete = bool(
        cycle_history_backfill.get("available")
        and not cycle_history_backfill.get("stale")
        and cycle_history_backfill.get("cache_complete")
        and cycle_history_backfill.get("serving_scan_id")
    )
    cycle_history_serving_target_certified = bool(
        control_cycle_complete or background_cycle_complete
    )

    checks = {
        "database_ready": bool(ready.get("database_ok")),
        "release_identity_available": bool(ready.get("release_commit")),
        "paper_only": bool(ready.get("paper_only")) and not bool(ready.get("live_execution")),
        "source_worker_current": _fresh_worker(
            source,
            allowed_states={"starting", "running", "success"},
        ),
        "thirteen_lane_source_snapshot_current": bool(
            source_snapshot.get("available")
            and not source_snapshot.get("handoff_stale")
            and not source_snapshot.get("stale")
            and source_snapshot.get("persisted_complete_snapshot") is True
            and int(source_snapshot.get("lane_count") or 0) == 13
        ),
        "canonical_source_history_migrated": bool(
            source_history.get("migration_complete")
            and int(source_history.get("lane_count") or 0) == 13
        ),
        "structural_forward_worker_current": _fresh_worker(
            mechanism,
            allowed_states={"running", "success"},
        ),
        "alpha_forward_cycle_current": bool(
            alpha_forward.get("available")
            and not alpha_forward.get("recovery_required")
        ),
        "cycle_history_serving_target_certified": cycle_history_serving_target_certified,
        "canonical_control_current": _fresh_worker(
            control,
            allowed_states={"success"},
        ),
        "operating_reconciliation_complete": bool(
            control.get("operating_reconciliation_complete")
        ),
        "qualified_bridge_publication_complete": bool(
            control.get("qualified_bridge_publication_complete")
        ),
        "research_projection_current": _fresh_worker(
            research_projection,
            allowed_states={"success"},
        ),
        "paper_portfolio_worker_current": _fresh_worker(
            portfolio,
            allowed_states={"running", "success"},
        ),
    }
    blockers = [name for name, passed in checks.items() if not passed]
    operationally_certified = not blockers
    lane_count = int(source_snapshot.get("lane_count") or 0)
    sufficient_lane_count = int(source_snapshot.get("sufficient_lane_count") or 0)
    full_13_lane_evidence_scope_complete = bool(
        lane_count == 13 and sufficient_lane_count == 13
    )

    index_advisory = {
        "available": bool(index_maintenance.get("available")),
        "state": index_maintenance.get("state"),
        "error_type": index_maintenance.get("error_type"),
        "background_indexes_complete": index_maintenance.get("background_indexes_complete"),
        "control_gate_released": index_maintenance.get("control_gate_released"),
        "certification_authority": False,
    }
    control_progress = control.get("cycle_history_cache_progress")
    if not isinstance(control_progress, dict) or not control_progress:
        control_progress = control.get("historical_cache_progress")
    if not isinstance(control_progress, dict) or not control_progress:
        control_progress = cycle_history_backfill.get("progress")
    if not isinstance(control_progress, dict):
        control_progress = {}

    payload: dict[str, object] = {
        "certified": operationally_certified,
        "operationally_certified": operationally_certified,
        "status": "certified" if operationally_certified else "blocked",
        "release_commit": ready.get("release_commit"),
        "observed_at": now.isoformat(),
        "checks": checks,
        "blockers": blockers,
        "alpha_forward": alpha_forward,
        "canonical_source_history": source_history,
        "cycle_history_backfill": cycle_history_backfill,
        "source_coverage": {
            "lane_count": lane_count,
            "sufficient_lane_count": sufficient_lane_count,
            "forward_test_eligible_lane_count": source_snapshot.get(
                "forward_test_eligible_lane_count"
            ),
            "allocation_source_qualified_lane_count": source_snapshot.get(
                "allocation_source_qualified_lane_count"
            ),
            "full_13_lane_evidence_scope_complete": full_13_lane_evidence_scope_complete,
            "all_lanes_required_to_be_profitable": False,
            "fail_closed_lane_gaps_allowed_for_operational_certification": True,
        },
        "full_13_lane_evidence_scope_complete": full_13_lane_evidence_scope_complete,
        "control": {
            "state": control.get("state"),
            "error_type": control.get("error_type"),
            "cycle_history_cache_complete": control_cycle_complete,
            "cycle_history_cache_progress": control_progress,
            "operating_reconciliation_complete": control.get(
                "operating_reconciliation_complete"
            ),
            "qualified_bridge_publication_complete": control.get(
                "qualified_bridge_publication_complete"
            ),
        },
        "runtime_index_maintenance": index_advisory,
        "certification_read_model": "single_batched_worker_snapshot",
        "certification_post_readiness_database_reads": 0,
        "trade_required_for_certification": False,
        "positive_candidate_required_for_certification": False,
        "economic_rejection_is_valid": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    if include_worker_truth:
        payload["_certification_workers"] = workers
    return payload


@app.get("/v3/operations/end-to-end-certification")
def end_to_end_certification():
    return end_to_end_certification_payload()


__all__ = [
    "app",
    "end_to_end_certification",
    "end_to_end_certification_payload",
    "_alpha_forward_status_from_research_worker",
    "_cycle_history_backfill_status_from_worker",
    "_source_history_status_from_worker",
]
