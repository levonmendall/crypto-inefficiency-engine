from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select

from inefficiency_engine import read_api_active_volume_deploy as active
from inefficiency_engine.evidence import WorkerHeartbeat


BATCHED_HEARTBEAT_READ = True


def _store():
    return active._store()  # noqa: SLF001 - deploy-layer composition


def _latest_heartbeats(store, worker_ids: list[str]) -> dict[str, WorkerHeartbeat]:
    """Read the latest durable heartbeat for all requested workers in one query."""

    ordered_ids = list(dict.fromkeys(str(worker_id) for worker_id in worker_ids if worker_id))
    if not ordered_ids:
        return {}

    rows = store.worker_heartbeats
    latest_ids = (
        select(
            rows.c.worker_id.label("worker_id"),
            func.max(rows.c.id).label("id"),
        )
        .where(rows.c.worker_id.in_(ordered_ids))
        .group_by(rows.c.worker_id)
        .subquery()
    )
    query = (
        select(rows.c.worker_id, rows.c.payload_json)
        .join(latest_ids, rows.c.id == latest_ids.c.id)
    )
    with store.engine.connect() as db:
        payload_rows = list(db.execute(query).mappings())

    result: dict[str, WorkerHeartbeat] = {}
    for row in payload_rows:
        worker_id = str(row.get("worker_id") or "")
        payload = row.get("payload_json")
        if not worker_id or payload is None:
            continue
        try:
            result[worker_id] = WorkerHeartbeat.model_validate_json(payload)
        except Exception:
            continue
    return result


def _runtime_heartbeats() -> dict[str, object]:
    """Mirror active readiness semantics with one heartbeat-ledger round trip."""

    store = _store()
    if store is None:
        return {
            "available": False,
            "workers": {},
            "reason": "evidence persistence is not configured",
            "batched_latest_heartbeat_read": True,
        }

    now = datetime.now(timezone.utc)
    stale_seconds = max(
        1.0,
        float(
            getattr(
                active._base_deploy._base.settings,  # noqa: SLF001
                "worker_heartbeat_stale_seconds",
                180.0,
            )
        ),
    )
    mapping = dict(active._RUNTIME_HEARTBEATS)  # noqa: SLF001 - deploy composition
    try:
        latest = _latest_heartbeats(store, list(mapping.values()))
        batch_error_type = None
    except Exception as exc:
        latest = {}
        batch_error_type = type(exc).__name__

    workers: dict[str, object] = {}
    for label, worker_id in mapping.items():
        worker_stale_seconds = active._worker_stale_after_seconds(  # noqa: SLF001
            label,
            stale_seconds,
        )
        heartbeat = latest.get(worker_id)
        if heartbeat is None:
            workers[label] = {
                "worker_id": worker_id,
                "available": False,
                "state": "unavailable" if batch_error_type else "unobserved",
                "error_type": batch_error_type,
                "stale_after_seconds": worker_stale_seconds,
            }
            continue

        observed_at = heartbeat.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        age_seconds = max(
            0.0,
            (now - observed_at.astimezone(timezone.utc)).total_seconds(),
        )
        detail = heartbeat.detail if isinstance(heartbeat.detail, dict) else {}
        worker: dict[str, object] = {
            "worker_id": worker_id,
            "available": True,
            "state": heartbeat.state,
            "error_type": heartbeat.error_type,
            "observed_at": observed_at.isoformat(),
            "age_seconds": age_seconds,
            "stale_after_seconds": worker_stale_seconds,
            "stale": age_seconds > worker_stale_seconds,
            "sequence": detail.get("sequence"),
            "stage": detail.get("stage"),
        }
        if label == "canonical_control":
            alpha = (
                detail.get("alpha_durable_promotion")
                if isinstance(detail.get("alpha_durable_promotion"), dict)
                else {}
            )
            worker.update(
                {
                    "provider_requests_allowed": detail.get("provider_requests_allowed"),
                    "provider_requests_used": detail.get(
                        "provider_requests_used",
                        alpha.get("provider_requests_used"),
                    ),
                    "parent_process_identity": detail.get("parent_process_identity"),
                    "parent_pid": detail.get("parent_pid"),
                    "parent_generation": detail.get("parent_generation"),
                    "parent_sequence": detail.get("parent_sequence"),
                    "parent_heartbeat_current": detail.get("parent_heartbeat_current"),
                    "executor_pid": detail.get("executor_pid"),
                    "executor_cycle_id": detail.get("executor_cycle_id"),
                    "executor_current_stage": detail.get("executor_current_stage"),
                    "executor_stage_observed_at": detail.get(
                        "executor_stage_observed_at"
                    ),
                    "executor_age_seconds": detail.get("executor_age_seconds"),
                    "executor_deadline_seconds": detail.get("executor_deadline_seconds"),
                    "last_executor_result": detail.get("last_executor_result"),
                    "last_executor_error_type": detail.get("last_executor_error_type"),
                    "last_executor_runtime_seconds": detail.get(
                        "last_executor_runtime_seconds"
                    ),
                    "executor_last_stage_before_failure": detail.get(
                        "executor_last_stage_before_failure"
                    ),
                    "executor_terminated": detail.get("executor_terminated"),
                    "executor_killed": detail.get("executor_killed"),
                    "retry_count": detail.get("retry_count"),
                    "historical_cache_progress": detail.get("historical_cache_progress"),
                    "historical_cache_complete": detail.get("historical_cache_complete"),
                    "cycle_history_cache_progress": detail.get(
                        "cycle_history_cache_progress"
                    ),
                    "cycle_history_cache_complete": detail.get(
                        "cycle_history_cache_complete"
                    ),
                    "external_process_deadline_enforced": detail.get(
                        "external_process_deadline_enforced"
                    ),
                    "paper_only": detail.get("paper_only"),
                    "operating_reconciliation_complete": detail.get(
                        "operating_reconciliation_complete"
                    ),
                    "operating_observed_at": detail.get("operating_observed_at"),
                    "qualified_bridge_publication_complete": detail.get(
                        "qualified_bridge_publication_complete"
                    ),
                    "qualified_bridge_observed_at": detail.get(
                        "qualified_bridge_observed_at"
                    ),
                    "qualified_bridge_candidate_count": detail.get(
                        "qualified_bridge_candidate_count"
                    ),
                    "research_projection_publication_complete": detail.get(
                        "research_projection_publication_complete"
                    ),
                    "control_plane_errors": detail.get("control_plane_errors"),
                    "control_stage_timings_seconds": detail.get(
                        "control_stage_timings_seconds"
                    ),
                }
            )
        else:
            # Preserve worker-specific durable details used by certification checks.
            for key, value in detail.items():
                worker.setdefault(str(key), value)
        workers[label] = worker

    return {
        "available": True,
        "stale_after_seconds": stale_seconds,
        "worker_specific_staleness": True,
        "workers": workers,
        "liveness_authority": False,
        "diagnostic_only": True,
        "batched_latest_heartbeat_read": True,
        "heartbeat_query_count": 1,
    }


def deployment_readiness() -> dict[str, object]:
    """Certification-only readiness using one batched worker-heartbeat query."""

    payload = dict(active._base_deploy.deployment_readiness())  # noqa: SLF001
    payload.update(
        {
            "release_commit": active._release_commit(),  # noqa: SLF001
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
            "runtime_heartbeat_observability": True,
            "runtime_heartbeats": _runtime_heartbeats(),
            "dashboard_critical_path_persisted_only": True,
            "certification_batched_heartbeat_read": True,
        }
    )
    return payload


__all__ = [
    "BATCHED_HEARTBEAT_READ",
    "_latest_heartbeats",
    "_runtime_heartbeats",
    "_store",
    "deployment_readiness",
]
