from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard_cards_v5 import (
    DASHBOARD_UI_CONTRACT_VERSION,
    DASHBOARD_V5_HTML,
    build_dashboard_v5_snapshot,
)


V5_SNAPSHOT_PATH = "/v3/dashboard/v5-snapshot"


def _headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Dashboard-Contract": DASHBOARD_UI_CONTRACT_VERSION,
        "X-Dashboard-Route": "canonical-v5-router",
    }


def _html() -> str:
    """Serve V5 from a dedicated snapshot route that survives deploy composition.

    The production app has several historical composition layers.  The dashboard
    page therefore no longer depends on which module owns /v3/dashboard/snapshot.
    A dedicated V5 endpoint always builds the server-side card model from the
    current persisted compact snapshot.
    """

    return DASHBOARD_V5_HTML.replace(
        "fetch('/v3/dashboard/snapshot'",
        f"fetch('{V5_SNAPSHOT_PATH}'",
    )


def _legacy_snapshot() -> dict[str, object]:
    # Imported lazily to avoid the read-api composition cycle during process boot.
    # By request time the production active-volume deploy module is fully loaded
    # and its dashboard_snapshot function is the canonical persisted compact read.
    try:
        from inefficiency_engine import read_api_active_volume_deploy as active

        payload = active.dashboard_snapshot()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="canonical persisted dashboard snapshot is temporarily unavailable",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=503, detail="canonical dashboard snapshot is invalid")
    return dict(payload)


def build_v5_dashboard_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", include_in_schema=False, response_class=HTMLResponse)
    def dashboard_root() -> HTMLResponse:
        return HTMLResponse(_html(), headers=_headers())

    @router.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
    def portfolio_dashboard() -> HTMLResponse:
        return HTMLResponse(_html(), headers=_headers())

    @router.get(V5_SNAPSHOT_PATH)
    def dashboard_v5_snapshot():
        legacy = _legacy_snapshot()
        result = build_dashboard_v5_snapshot(legacy)
        result.update(
            {
                "dashboard_contract_active": True,
                "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
                "dashboard_route_authority": "canonical-v5-router",
                "legacy_snapshot_fields_available": True,
            }
        )
        return result

    return router
