from __future__ import annotations

import os

from fastapi import HTTPException

from inefficiency_engine import read_api_research_deploy as _base_deploy
from inefficiency_engine.active_volume_runtime import (
    read_active_cycle_history_status,
    read_active_volume_universe_status,
)
from inefficiency_engine.lane_readiness import build_lane_executable_readiness
from inefficiency_engine.service import OpportunityService


app = _base_deploy.app

# Replace only the deploy/read-model routes whose payloads need the active volume
# universe. All portfolio, mechanism, and research routes remain unchanged.
_REPLACED_PATHS = {
    "/health",
    "/ready",
    "/v3/dashboard/snapshot",
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


def _store():
    return _base_deploy._base.evidence_store  # noqa: SLF001 - deploy-layer composition


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def _lane_readiness():
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    core = OpportunityService(settings=_base_deploy._base.settings, evidence_store=store)  # noqa: SLF001
    return build_lane_executable_readiness(core, store)


@app.get("/health")
def deployment_health():
    payload = dict(_base_deploy.deployment_health())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
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
    """Separate architecture capability from real current evidence qualification."""
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


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    """Augment the established dashboard projection with exact active membership."""

    payload = dict(_base_deploy.dashboard_snapshot())
    store = _store()
    if store is None:
        payload["volume_universe"] = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "volume_is_defining_metric": True,
        }
        payload["cycle_history"] = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "historical_counts_as_forward": False,
            "live_execution_authority": False,
        }
        payload["lane_executability"] = {
            "available": False,
            "all_lanes_paper_execution_capable": False,
            "live_execution_capable": False,
        }
        payload["release_commit"] = _release_commit()
        return payload

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
    try:
        readiness = _lane_readiness()
        lane_summary = {
            "available": True,
            "lane_count": readiness.lane_count,
            "architecture_executable_count": readiness.architecture_executable_count,
            "currently_qualified_count": readiness.currently_qualified_count,
            "profitability_certified_count": readiness.profitability_certified_count,
            "all_lanes_paper_execution_capable": readiness.all_lanes_paper_execution_capable,
            "live_execution_capable": False,
        }
    except Exception:
        lane_summary = {
            "available": False,
            "all_lanes_paper_execution_capable": False,
            "live_execution_capable": False,
        }

    payload["volume_universe"] = volume
    payload["cycle_history"] = cycle_history
    payload["lane_executability"] = lane_summary
    payload["release_commit"] = _release_commit()
    payload["volume_universe_observability"] = True
    payload["active_cycle_history_membership"] = True
    return payload
