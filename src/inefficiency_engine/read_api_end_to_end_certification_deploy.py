from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import inspect, text

from inefficiency_engine import read_api_active_volume_deploy as active
from inefficiency_engine import read_api_lane_history_ui_deploy as inner
from inefficiency_engine.critical_evidence_recovery import (
    DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
    _alpha_forward_status,
)
from inefficiency_engine.source_coverage_history import (
    MIGRATION_NAME,
    SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE,
    SOURCE_COVERAGE_HISTORY_TABLE,
)


app = inner.app
_CYCLE_HISTORY_BACKFILL_WORKER_ID = "cycle-history-background-backfill"
_CYCLE_HISTORY_BACKFILL_STALE_SECONDS = 180.0


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


def _source_history_status(store) -> dict[str, object]:
    """Read canonical source-history migration truth without creating schema."""

    try:
        available = set(inspect(store.engine).get_table_names())
        if SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE not in available:
            return {
                "available": False,
                "migration_complete": False,
                "checkpoint_heartbeat_id": 0,
                "lane_count": 0,
                "reason": "migration_table_unavailable",
            }
        with store.engine.connect() as db:
            row = db.execute(
                text(
                    "SELECT checkpoint_heartbeat_id, complete, updated_at "
                    f"FROM {SOURCE_COVERAGE_HISTORY_MIGRATION_TABLE} "
                    "WHERE migration_name=:migration_name LIMIT 1"
                ),
                {"migration_name": MIGRATION_NAME},
            ).mappings().first()
            lane_count = 0
            snapshot_count = 0
            if SOURCE_COVERAGE_HISTORY_TABLE in available:
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
        if row is None:
            return {
                "available": True,
                "migration_complete": False,
                "checkpoint_heartbeat_id": 0,
                "lane_count": lane_count,
                "snapshot_count": snapshot_count,
                "reason": "migration_checkpoint_unobserved",
            }
        return {
            "available": True,
            "migration_complete": bool(row.get("complete")),
            "checkpoint_heartbeat_id": int(row.get("checkpoint_heartbeat_id") or 0),
            "updated_at": row.get("updated_at"),
            "lane_count": lane_count,
            "snapshot_count": snapshot_count,
            "reason": "complete" if bool(row.get("complete")) else "migration_in_progress",
        }
    except Exception as exc:
        return {
            "available": False,
            "migration_complete": False,
            "checkpoint_heartbeat_id": 0,
            "lane_count": 0,
            "error_type": type(exc).__name__,
            "reason": "source_history_read_unavailable",
        }


def _cycle_history_backfill_status(store) -> dict[str, object]:
    """Expose the bounded background bootstrap heartbeat without granting authority.

    The background worker is the process that creates the first exact active target.
    Canonical control remains an independent required certification gate, so reporting a
    completed background target cannot falsely certify reconciliation or bridge output.
    """

    try:
        heartbeat = store.latest_worker_heartbeat(_CYCLE_HISTORY_BACKFILL_WORKER_ID)
    except Exception as exc:
        return {
            "available": False,
            "stale": True,
            "cache_complete": False,
            "progress": {},
            "error_type": type(exc).__name__,
            "certification_authority": False,
        }
    if heartbeat is None:
        return {
            "available": False,
            "stale": True,
            "cache_complete": False,
            "progress": {},
            "certification_authority": False,
        }

    detail = dict(getattr(heartbeat, "detail", {}) or {})
    raw_progress = detail.get("progress")
    progress = dict(raw_progress) if isinstance(raw_progress, dict) else {}
    observed_at = getattr(heartbeat, "observed_at", None)
    age_seconds = None
    if isinstance(observed_at, datetime):
        observed = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (datetime.now(timezone.utc) - observed).total_seconds())
    stale = age_seconds is None or age_seconds > _CYCLE_HISTORY_BACKFILL_STALE_SECONDS
    cache_complete = bool(detail.get("cache_complete") or progress.get("complete"))
    serving_scan_id = progress.get("serving_scan_id")
    return {
        "available": True,
        "state": getattr(heartbeat, "state", None),
        "error_type": getattr(heartbeat, "error_type", None),
        "observed_at": observed_at,
        "age_seconds": age_seconds,
        "stale": stale,
        "stage": detail.get("stage"),
        "cache_complete": cache_complete,
        "first_certified_target_pending": bool(
            detail.get("first_certified_target_pending", not cache_complete)
        ),
        "serving_scan_id": serving_scan_id,
        "progress": progress,
        "certification_authority": False,
    }


def end_to_end_certification_payload() -> dict[str, object]:
    """Return a fail-closed production certification from durable runtime truth.

    Certification proves that the paper pipeline can move truthful evidence through
    every authority boundary. It deliberately does *not* require a profitable candidate,
    allocation, position or trade: correct economic/statistical rejection is a healthy
    end-to-end result. Historical or presentation data can never create qualification.

    Operational certification and full 13-lane evidence completeness are reported
    separately. A fail-closed evidence gap does not make the runtime dishonest, but the
    endpoint never labels a partially source-sufficient universe as fully complete.
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
    control = _worker(workers, "canonical_control")
    portfolio = _worker(workers, "portfolio")
    source = _worker(workers, "permanent_source")
    mechanism = _worker(workers, "mechanism_forward")
    source_snapshot = _worker(workers, "source_coverage_snapshot")
    research_projection = _worker(workers, "research_projection")
    index_maintenance = _worker(workers, "runtime_index_maintenance")

    store = active._store()  # noqa: SLF001 - production read-plane composition
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    alpha_forward = _alpha_forward_status(
        store,
        now=datetime.now(timezone.utc),
        stale_after_seconds=DEFAULT_ALPHA_FORWARD_RECOVERY_STALE_SECONDS,
    )
    source_history = _source_history_status(store)
    cycle_history_backfill = _cycle_history_backfill_status(store)
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

    return {
        "certified": operationally_certified,
        "operationally_certified": operationally_certified,
        "status": "certified" if operationally_certified else "blocked",
        "release_commit": ready.get("release_commit"),
        "observed_at": datetime.now(timezone.utc).isoformat(),
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
        "trade_required_for_certification": False,
        "positive_candidate_required_for_certification": False,
        "economic_rejection_is_valid": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


@app.get("/v3/operations/end-to-end-certification")
def end_to_end_certification():
    return end_to_end_certification_payload()


__all__ = ["app", "end_to_end_certification", "end_to_end_certification_payload"]
