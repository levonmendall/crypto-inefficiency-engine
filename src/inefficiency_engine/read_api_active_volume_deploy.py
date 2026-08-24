from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import HTTPException

from inefficiency_engine import read_api_research_deploy as _base_deploy
from inefficiency_engine.active_volume_runtime import (
    read_active_cycle_history_status,
    read_active_volume_universe_status,
)
from inefficiency_engine.dashboard_source_connectivity import read_source_connectivity
from inefficiency_engine.dashboard_source_truth import overlay_dashboard_source_truth
from inefficiency_engine.lane_readiness import build_lane_executable_readiness
from inefficiency_engine.production_dashboard_fastpath import build_production_dashboard_snapshot
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.source_coverage_catalog import LANES
from inefficiency_engine.source_runtime_safety import (
    install_source_coverage_reconciliation_runtime,
)


install_source_coverage_reconciliation_runtime()
app = _base_deploy.app

# Replace only the deploy/read-model routes whose payloads need the active volume
# universe. All portfolio, mechanism, and research routes remain unchanged.
_REPLACED_PATHS = {
    "/health",
    "/ready",
    "/v3/dashboard/snapshot",
    "/v3/dashboard/source-connectivity",
    "/v3/research/cycle-history",
}
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in _REPLACED_PATHS
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None

_PRODUCTION_EVIDENCE_DISCONNECTED = {"capital_location_settlement"}
_RUNTIME_HEARTBEATS = {
    "canonical_control": "canonical-control-operating-loop",
    "portfolio": "canonical-portfolio-operating-loop",
    "permanent_source": "canonical-source-operating-loop",
    "volume_universe": "volume-universe-lightweight-refresh",
    "market_universe_routing": "market-universe-routing",
    "research": "shadow-research-auxiliary",
    "heavy_worker": "disposable-heavy-work",
    "source_refresh": "priority-source-refresh-plane",
    "mechanism_forward": "mechanism-forward-evidence",
    "alpha_l2_sampling": "alpha-l2-research-sampling",
}
# Runtime liveness cadence is not evidence freshness. The source owner pulses every
# 30s and is supervised at the baseline 180s boundary, while portfolio/research and
# their subordinate diagnostics can legitimately go several minutes between terminal
# heartbeats. Source/evidence cards retain their own strict TTL/readiness contracts.
_RUNTIME_STALE_AFTER_SECONDS = {
    "canonical_control": 180.0,
    "portfolio": 600.0,
    "permanent_source": 180.0,
    "volume_universe": 600.0,
    "market_universe_routing": 600.0,
    "research": 600.0,
    "heavy_worker": 600.0,
    "source_refresh": 600.0,
    "mechanism_forward": 600.0,
    "alpha_l2_sampling": 600.0,
}


def _store():
    return _base_deploy._base.evidence_store  # noqa: SLF001 - deploy-layer composition


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def _worker_stale_after_seconds(label: str, baseline_seconds: float) -> float:
    """Return the worker liveness window without changing evidence freshness."""

    return max(
        max(1.0, float(baseline_seconds)),
        float(_RUNTIME_STALE_AFTER_SECONDS.get(label, baseline_seconds)),
    )


def _runtime_heartbeats() -> dict[str, object]:
    """Best-effort durable runtime truth without changing liveness semantics.

    Render uses /health as a process liveness probe. A transient provider or research
    degradation must therefore remain visible in the payload without converting the
    endpoint into a restart trigger. Each worker's durable heartbeat is reported with
    age/staleness using that worker's actual operating cadence. Evidence/source TTLs
    remain separate and are not relaxed here.
    """

    store = _store()
    if store is None:
        return {
            "available": False,
            "workers": {},
            "reason": "evidence persistence is not configured",
        }
    now = datetime.now(timezone.utc)
    stale_seconds = max(
        1.0,
        float(
            getattr(
                _base_deploy._base.settings,  # noqa: SLF001
                "worker_heartbeat_stale_seconds",
                180.0,
            )
        ),
    )
    workers: dict[str, object] = {}
    for label, worker_id in _RUNTIME_HEARTBEATS.items():
        worker_stale_seconds = _worker_stale_after_seconds(label, stale_seconds)
        try:
            heartbeat = store.latest_worker_heartbeat(worker_id)
        except Exception as exc:
            workers[label] = {
                "worker_id": worker_id,
                "available": False,
                "state": "unavailable",
                "error_type": type(exc).__name__,
                "stale_after_seconds": worker_stale_seconds,
            }
            continue
        if heartbeat is None:
            workers[label] = {
                "worker_id": worker_id,
                "available": False,
                "state": "unobserved",
                "stale_after_seconds": worker_stale_seconds,
            }
            continue
        observed_at = heartbeat.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (now - observed_at).total_seconds())
        detail = heartbeat.detail if isinstance(getattr(heartbeat, "detail", None), dict) else {}
        worker = {
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
                    "parent_heartbeat_current": detail.get(
                        "parent_heartbeat_current"
                    ),
                    "executor_pid": detail.get("executor_pid"),
                    "executor_cycle_id": detail.get("executor_cycle_id"),
                    "executor_current_stage": detail.get("executor_current_stage"),
                    "executor_stage_observed_at": detail.get(
                        "executor_stage_observed_at"
                    ),
                    "executor_age_seconds": detail.get("executor_age_seconds"),
                    "executor_deadline_seconds": detail.get(
                        "executor_deadline_seconds"
                    ),
                    "last_executor_result": detail.get("last_executor_result"),
                    "last_executor_error_type": detail.get(
                        "last_executor_error_type"
                    ),
                    "last_executor_runtime_seconds": detail.get(
                        "last_executor_runtime_seconds"
                    ),
                    "executor_last_stage_before_failure": detail.get(
                        "executor_last_stage_before_failure"
                    ),
                    "executor_terminated": detail.get("executor_terminated"),
                    "executor_killed": detail.get("executor_killed"),
                    "retry_count": detail.get("retry_count"),
                    "historical_cache_progress": detail.get(
                        "historical_cache_progress"
                    ),
                    "historical_cache_complete": detail.get(
                        "historical_cache_complete"
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
        workers[label] = worker
    return {
        "available": True,
        "stale_after_seconds": stale_seconds,
        "worker_specific_staleness": True,
        "workers": workers,
        "liveness_authority": False,
        "diagnostic_only": True,
    }


def _lane_readiness():
    """Detailed diagnostic path only; never called by the dashboard fast path."""
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    core = OpportunityService(settings=_base_deploy._base.settings, evidence_store=store)  # noqa: SLF001
    return build_lane_executable_readiness(core, store)


def _lane_summary_from_payload(payload: dict[str, object]) -> dict[str, object]:
    """Build a non-blocking lane summary from persisted operating truth.

    Architecture capability, production evidence connectivity, positive decision-grade
    qualification, and current promoted opportunities are separate claims. A lane is
    paper-execution-capable here only when a worker-published operating row has reached
    a positive allocation-grade conclusion and the production evidence path is connected.
    Research shadows and code presence cannot create executability. This function
    performs no provider work.
    """

    mechanisms = payload.get("mechanisms")
    rows: list[dict[str, object]] = []
    if isinstance(mechanisms, dict):
        raw_rows = mechanisms.get("mechanisms")
        if isinstance(raw_rows, list):
            rows = [row for row in raw_rows if isinstance(row, dict)]

    def _currently_qualified(row: dict[str, object]) -> bool:
        if bool(row.get("currently_qualified")):
            return True
        try:
            return int(row.get("current_promoted_count") or 0) > 0
        except (TypeError, ValueError):
            return False

    def _decision_grade_positive(row: dict[str, object]) -> bool:
        return _currently_qualified(row) or str(row.get("state") or "") in {
            "certifying",
            "certified",
        }

    lane_count = len(LANES)
    current_source_truth = payload.get("current_source_truth")
    connected_ids = {
        lane_id
        for lane_id in LANES
        if (
            isinstance(current_source_truth, dict)
            and isinstance(current_source_truth.get(lane_id), dict)
            and bool(current_source_truth[lane_id].get("connected"))
        )
    }
    if not connected_ids:
        connected_ids = {
            lane_id for lane_id in LANES
            if lane_id not in _PRODUCTION_EVIDENCE_DISCONNECTED
        }
    connected_count = len(connected_ids)
    current_ids = {
        str(row.get("mechanism_id") or "")
        for row in rows
        if str(row.get("mechanism_id") or "") in LANES
        and _currently_qualified(row)
    }
    decision_grade_ids = {
        str(row.get("mechanism_id") or "")
        for row in rows
        if str(row.get("mechanism_id") or "") in LANES
        and _decision_grade_positive(row)
    }
    projection_current = not bool(payload.get("research_projection_stale")) and not bool(
        payload.get("operating_projection_stale")
    )
    executable_ids = decision_grade_ids & connected_ids if projection_current else set()

    return {
        "available": bool(rows),
        "lane_count": lane_count,
        "architecture_executable_count": lane_count,
        "production_evidence_connected_count": connected_count,
        "all_lanes_production_evidence_connected": connected_count == lane_count,
        "production_evidence_disconnected_lanes": sorted(set(LANES) - connected_ids),
        "decision_grade_outcome_qualified_count": len(decision_grade_ids),
        "currently_qualified_count": len(current_ids),
        "paper_execution_capable_count": len(executable_ids),
        "paper_execution_capable_lanes": sorted(executable_ids),
        "profitability_certified_count": sum(
            bool(row.get("profitability_certified")) for row in rows
        ),
        "all_lanes_paper_execution_capable": len(executable_ids) == lane_count,
        "projection_current_for_execution": projection_current,
        "live_execution_capable": False,
        "summary_source": (
            "current_source_coverage_plus_persisted_dashboard_projection"
            if isinstance(current_source_truth, dict) and current_source_truth
            else "persisted_dashboard_projection_plus_static_runtime_connectivity"
        ),
        "request_time_research_computation": False,
        "detail_endpoint": "/v3/lane-executability",
    }


@app.get("/health")
def deployment_health():
    payload = dict(_base_deploy.deployment_health())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
            "runtime_heartbeat_observability": True,
            "runtime_heartbeats": _runtime_heartbeats(),
            "dashboard_critical_path_persisted_only": True,
        }
    )
    return payload


@app.get("/ready")
def deployment_readiness():
    payload = dict(_base_deploy.deployment_readiness())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
            "runtime_heartbeat_observability": True,
            "runtime_heartbeats": _runtime_heartbeats(),
            "dashboard_critical_path_persisted_only": True,
        }
    )
    return payload


@app.get("/v3/research/volume-universe")
def volume_universe_status():
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_active_volume_universe_status(store)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="active volume universe is temporarily unavailable",
        ) from exc


@app.get("/v3/research/cycle-history")
def cycle_history_status():
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_active_cycle_history_status(store)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="active cycle-history status is temporarily unavailable",
        ) from exc


@app.get("/v3/lane-executability")
def lane_executability():
    """Detailed diagnostic endpoint; intentionally outside the dashboard deadline."""
    try:
        return _lane_readiness().model_dump(mode="json")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="lane executable readiness is temporarily unavailable",
        ) from exc


@app.get("/v3/lane-executability/{lane_id}")
def lane_executability_detail(lane_id: str):
    snapshot = _lane_readiness()
    for lane in snapshot.lanes:
        if lane.lane_id == lane_id:
            return lane.model_dump(mode="json")
    raise HTTPException(status_code=404, detail="unknown profit-mechanism lane")


@app.get("/v3/dashboard/source-connectivity")
def source_connectivity():
    """Return source-by-source persisted connectivity independent of the main snapshot."""

    store = _store()
    if store is None:
        return {
            "available": False,
            "read_error_type": "EvidencePersistenceNotConfigured",
            "summary": {},
            "sources": [],
            "release_commit": _release_commit(),
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }
    payload = read_source_connectivity(store)
    payload["release_commit"] = _release_commit()
    payload["diagnostic_only"] = True
    return payload


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    """Return a bounded persisted-only command-center snapshot.

    The phone refresh deadline must never include research/service construction.
    Normal reads use worker-published compact projections; if the portfolio compact
    row is absent, the server reconstructs the portfolio once from durable tables in
    one bounded read path. Top-volume and history metadata remain optional DB-only
    enrichments and cannot erase the canonical portfolio payload.
    """

    store = _store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "persisted dashboard snapshot is unavailable",
                "stage": "evidence_store",
                "error_type": "EvidencePersistenceNotConfigured",
            },
        )

    try:
        payload = build_production_dashboard_snapshot(
            store,
            forward_target=max(
                1,
                int(getattr(_base_deploy._base.settings, "alpha_min_forward_samples", 30)),  # noqa: SLF001
            ),
            settled_target=max(
                5,
                int(
                    getattr(
                        _base_deploy._base.settings,  # noqa: SLF001
                        "operating_certification_min_settled_trials",
                        20,
                    )
                ),
            ),
        )
    except Exception as exc:
        cause = exc.__cause__
        raise HTTPException(
            status_code=503,
            detail={
                "message": "persisted dashboard snapshot is temporarily unavailable",
                "stage": "production_dashboard_fastpath",
                "error_type": type(exc).__name__,
                "cause_type": type(cause).__name__ if cause is not None else None,
            },
        ) from exc

    try:
        payload = overlay_dashboard_source_truth(store, payload)
    except Exception as exc:
        payload["source_truth_overlay_degraded"] = True
        payload["source_truth_overlay_error_type"] = type(exc).__name__

    try:
        volume = read_active_volume_universe_status(store)
    except Exception:
        volume = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "volume_is_defining_metric": True,
        }
    try:
        cycle_history = read_active_cycle_history_status(store)
    except Exception:
        cycle_history = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "historical_counts_as_forward": False,
            "live_execution_authority": False,
        }

    payload["volume_universe"] = volume
    payload["cycle_history"] = cycle_history
    payload["lane_executability"] = _lane_summary_from_payload(payload)
    payload["release_commit"] = _release_commit()
    payload["volume_universe_observability"] = True
    payload["active_cycle_history_membership"] = True
    payload["runtime_heartbeat_observability"] = True
    payload["runtime_heartbeats"] = _runtime_heartbeats()
    payload["dashboard_critical_path_persisted_only"] = True
    return payload
