from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine import read_api_active_volume_deploy as _base
from inefficiency_engine.dashboard_cards_v5 import (
    DASHBOARD_UI_CONTRACT_VERSION,
    DASHBOARD_V5_HTML,
    build_dashboard_v5_snapshot,
)


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"
app = _base.app

# V5 owns presentation, but /v3/dashboard/snapshot remains backward compatible.
# Existing browser tabs and diagnostic consumers may still expect the legacy compact
# portfolio/runtime/mechanism sections. The V5 server-built card model is therefore
# added to that persisted-only payload instead of replacing it.
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
            "dashboard_card_read_model": "standalone_server_built_v5",
            "dashboard_inherited_card_overlay_chain_active": False,
            "dashboard_snapshot_backward_compatible": True,
            "legacy_snapshot_fields_preserved": True,
        }
    )
    return result


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def dashboard_root() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_V5_HTML, headers=_html_headers())


@app.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
def portfolio_dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_V5_HTML, headers=_html_headers())


@app.get("/health")
def deployment_health():
    return _runtime_contract(dict(_base.deployment_health()))


@app.get("/ready")
def deployment_readiness():
    return _runtime_contract(dict(_base.deployment_readiness()))


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    try:
        legacy = dict(_base.dashboard_snapshot())
        v5 = build_dashboard_v5_snapshot(legacy)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="v5 mechanism truth snapshot is temporarily unavailable",
        ) from exc

    # Preserve every legacy compact section so an already-open pre-V5 browser tab
    # continues to render portfolio, runtime, history, and mechanism data while the
    # new page consumes the V5 `cards`, `summary`, and `system` fields. This prevents
    # a deployment from turning a healthy old page into a screen full of fallbacks.
    result = dict(legacy)
    result.update(v5)
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "canonical_api_app": CANONICAL_API_APP,
            "dashboard_snapshot_backward_compatible": True,
            "legacy_snapshot_fields_preserved": True,
        }
    )
    return result
