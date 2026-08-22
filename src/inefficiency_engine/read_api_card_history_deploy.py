from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine import read_api_active_volume_deploy as _base
from inefficiency_engine.dashboard_card_history import (
    CARD_HISTORY_DASHBOARD_HTML,
    DASHBOARD_UI_CONTRACT_VERSION,
    restore_card_history_truth,
)


app = _base.app

_REPLACED_PATHS = {"/", "/dashboard", "/v3/dashboard/snapshot"}
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
    }


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def dashboard_root() -> HTMLResponse:
    return HTMLResponse(CARD_HISTORY_DASHBOARD_HTML, headers=_html_headers())


@app.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
def portfolio_dashboard() -> HTMLResponse:
    return HTMLResponse(CARD_HISTORY_DASHBOARD_HTML, headers=_html_headers())


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
    result["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
    return result
