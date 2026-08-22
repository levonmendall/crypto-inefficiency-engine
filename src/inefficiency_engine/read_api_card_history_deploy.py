from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine import read_api_active_volume_deploy as _base
from inefficiency_engine.dashboard_card_history import (
    CARD_HISTORY_DASHBOARD_HTML,
    DASHBOARD_UI_CONTRACT_VERSION,
    restore_card_history_truth,
)


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"
app = _base.app

# Own every route needed to prove the dashboard contract is actually active.  The
# base read plane still supplies the underlying persisted data and runtime health.
_REPLACED_PATHS = {"/", "/dashboard", "/health", "/ready", "/v3/dashboard/snapshot"}
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in _REPLACED_PATHS
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None


def _html_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Dashboard-Contract": DASHBOARD_UI_CONTRACT_VERSION,
        "X-Canonical-API-App": CANONICAL_API_APP,
    }


def _runtime_contract(payload: dict[str, object]) -> dict[str, object]:
    result = dict(payload)
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "canonical_api_app": CANONICAL_API_APP,
            "dashboard_card_truth_resolver_active": True,
            "dashboard_history_preserving": True,
        }
    )
    return result


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def dashboard_root() -> HTMLResponse:
    return HTMLResponse(CARD_HISTORY_DASHBOARD_HTML, headers=_html_headers())


@app.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
def portfolio_dashboard() -> HTMLResponse:
    return HTMLResponse(CARD_HISTORY_DASHBOARD_HTML, headers=_html_headers())


@app.get("/health")
def deployment_health():
    return _runtime_contract(dict(_base.deployment_health()))


@app.get("/ready")
def deployment_readiness():
    return _runtime_contract(dict(_base.deployment_readiness()))


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    try:
        payload = _base.dashboard_snapshot()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="history-preserving dashboard snapshot is temporarily unavailable",
        ) from exc
    result = restore_card_history_truth(dict(payload))
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "canonical_api_app": CANONICAL_API_APP,
        }
    )
    return result
