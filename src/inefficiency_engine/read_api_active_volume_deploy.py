from __future__ import annotations

import os

from fastapi import HTTPException

from inefficiency_engine import read_api_research_deploy as _base_deploy
from inefficiency_engine.active_volume_runtime import (
    read_active_cycle_history_status,
    read_active_volume_universe_status,
)


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


@app.get("/health")
def deployment_health():
    payload = dict(_base_deploy.deployment_health())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
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

    payload["volume_universe"] = volume
    payload["cycle_history"] = cycle_history
    payload["release_commit"] = _release_commit()
    payload["volume_universe_observability"] = True
    payload["active_cycle_history_membership"] = True
    return payload
