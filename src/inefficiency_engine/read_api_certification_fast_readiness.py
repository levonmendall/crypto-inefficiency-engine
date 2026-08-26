from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select, union_all

from inefficiency_engine import read_api_active_volume_deploy as active
from inefficiency_engine.evidence import WorkerHeartbeat


BATCHED_HEARTBEAT_READ = True
LATEST_HEARTBEAT_QUERY_STRATEGY = "targeted_latest_per_worker_union"

# Certification consumes only durable worker publications. Keep every worker needed by
# the E2E truth composition in the same latest-heartbeat query so the HTTP request does
# not serially revisit the append-only heartbeat ledger after readiness is assembled.
_CERTIFICATION_EXTRA_HEARTBEATS = {
    "source_coverage_snapshot": "canonical-source-coverage-snapshot",
    "research_projection": "dashboard-research-projection-publisher",
    "runtime_index_maintenance": "source-coverage-runtime-index-maintenance",
    "source_history_migration": "canonical-source-coverage-history-migration",
    "cycle_history_backfill": "cycle-history-background-backfill",
    "cycle_history_index_maintenance": "cycle-history-index-maintenance",
}


def _store():
    return active._store()  # noqa: SLF001 - deploy-layer composition


def _certification_heartbeat_mapping() -> dict[str, str]:
    mapping = dict(active._RUNTIME_HEARTBEATS)  # noqa: SLF001 - deploy composition
    mapping.update(_CERTIFICATION_EXTRA_HEARTBEATS)
    return mapping


def _latest_heartbeats(store, worker_ids: list[str]) -> dict[str, WorkerHeartbeat]:
    """Read newest requested worker rows in one round trip using targeted index seeks.

    A grouped ``MAX(id)`` over the append-only heartbeat ledger can scan a large portion
    of the table before PostgreSQL applies the request's short read deadline. Build one
    bounded newest-row subquery per requested worker instead. The outer ``UNION ALL`` is
    still one database statement/round trip, while ``worker_heartbeats(worker_id, id)``
    gives the planner a direct access path for each ``ORDER BY id DESC LIMIT 1`` lookup.
    """

    ordered_ids = list(dict.fromkeys(str(worker_id) for worker_id in worker_ids if worker_id))
    if not ordered_ids:
        return {}

    rows = store.worker_heartbeats
    targeted = []
    for worker_id in ordered_ids:
        latest = (
            select(rows.c.worker_id, rows.c.payload_json)
            .where(rows.c.worker_id == worker_id)
            .order_by(rows.c.id.desc())
            .limit(1)
            .subquery()
        )
        targeted.append(select(latest.c.worker_id, latest.c.payload_json))
    query = targeted[0] if len(targeted) == 1 else union_all(*targeted)

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
            "heartbeat_query_strategy": LATEST_HEARTBEAT_QUERY_STRATEGY,
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
    mapping = _certification_heartbeat_mapping()
    batch_error: dict[str, object] | None = None
    try:
        latest = _latest_heartbeats(store, list(mapping.values()))
    except Exception as exc:
        latest = {}
        batch_error = {
            "error_type": type(exc).__name__,
            "message": str(exc)[:1000],
            "query_strategy": LATEST_HEARTBEAT_QUERY_STRATEGY,
            "retryable": True,
            "certification_authority": False,
        }

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
                "state": "unavailable" if batch_error else "unobserved",
                "error_type": batch_error.get("error_type") if batch_error else None,
                "error_message": batch_error.get("message") if batch_error else None,
                "heartbeat_query_strategy": LATEST_HEARTBEAT_QUERY_STRATEGY,
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
            # Preserve compact worker-specific durable details used by certification.
            # The source snapshot's full lane payload is intentionally omitted: the E2E
            # endpoint needs only its already-published aggregate counts and handoff truth.
            for key, value in detail.items():
                if label == "source_coverage_snapshot" and key == "snapshot":
                    continue
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
        "heartbeat_query_strategy": LATEST_HEARTBEAT_QUERY_STRATEGY,
        "heartbeat_query_failed": batch_error is not None,
        "batch_error": batch_error,
        "certification_worker_count": len(mapping),
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
            "certification_post_readiness_database_reads": 0,
        }
    )
    return payload


__all__ = [
    "BATCHED_HEARTBEAT_READ",
    "LATEST_HEARTBEAT_QUERY_STRATEGY",
    "_certification_heartbeat_mapping",
    "_latest_heartbeats",
    "_runtime_heartbeats",
    "_store",
    "deployment_readiness",
]
