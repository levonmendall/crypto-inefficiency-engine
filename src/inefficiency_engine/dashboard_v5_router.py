from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard_cards_v5 import (
    DASHBOARD_UI_CONTRACT_VERSION,
    build_dashboard_v5_snapshot,
)
from inefficiency_engine.dashboard_command_center_v6 import (
    COMMAND_CENTER_LAYOUT_VERSION,
    DASHBOARD_COMMAND_CENTER_HTML,
)


V5_SNAPSHOT_PATH = "/v3/dashboard/v5-snapshot"


def _headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Dashboard-Contract": DASHBOARD_UI_CONTRACT_VERSION,
        "X-Dashboard-Layout": COMMAND_CENTER_LAYOUT_VERSION,
        "X-Dashboard-Route": "canonical-v5-router",
    }


def _html() -> str:
    """Serve one standalone full command center with V5 mechanism cards.

    Portfolio/account history, runtime diagnostics, and research evidence share one
    persisted snapshot request with the server-built V5 mechanism read model. The
    historical dashboard card renderer is intentionally not composed back in.
    """

    # The page is otherwise a standalone literal. On mobile, viewport changes can
    # resize the canvas; re-read the same persisted snapshot rather than retaining a
    # second client-side data authority just for chart redraws.
    return DASHBOARD_COMMAND_CENTER_HTML.replace(
        "window.addEventListener('resize',()=>renderChart(window.__history||[]));",
        "window.addEventListener('resize',()=>refresh());",
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


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _command_center_payload(legacy: dict[str, object]) -> dict[str, object]:
    """Preserve non-mechanism command-center truth beside the V5 card model.

    This intentionally excludes the historical mechanism renderer. Mechanism state
    has one presentation authority: ``cards`` from ``build_dashboard_v5_snapshot``.
    """

    runtime_heartbeats = _dict(legacy.get("runtime_heartbeats"))
    return {
        "portfolio": _dict(legacy.get("portfolio")),
        "performance": _dict(legacy.get("performance")),
        "runtime": _dict(legacy.get("runtime")),
        "positions": _dict(legacy.get("positions")) or {"positions": []},
        "trades": _dict(legacy.get("trades")) or {"trades": []},
        "history": _dict(legacy.get("history")) or {"count": 0, "snapshots": []},
        "skips": _dict(legacy.get("skips")) or {"skips": []},
        "attribution": _dict(legacy.get("attribution")) or {
            "pnl_by_mechanism_usd": {},
            "pnl_by_strategy_usd": {},
        },
        "queue": _dict(legacy.get("queue")) or {"actions": []},
        "cycle_history": _dict(legacy.get("cycle_history")) or {
            "available": False,
            "assets": [],
        },
        "runtime_heartbeats": runtime_heartbeats,
        "projection_mode": legacy.get("projection_mode"),
        "presentation_fallback": bool(legacy.get("presentation_fallback")),
        "presentation_fallback_reason": legacy.get("presentation_fallback_reason"),
    }


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
                "command_center_layout_version": COMMAND_CENTER_LAYOUT_VERSION,
                "dashboard_route_authority": "canonical-v5-router",
                "legacy_snapshot_fields_available": True,
                "command_center": _command_center_payload(legacy),
            }
        )
        return result

    return router
