from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from inefficiency_engine import __version__
from inefficiency_engine import read_api_active_volume_deploy as _base
from inefficiency_engine.dashboard_card_currentness import preserve_meaningful_card_conclusions
from inefficiency_engine.dashboard_cards_v5 import (
    DASHBOARD_UI_CONTRACT_VERSION,
    build_dashboard_v5_snapshot,
)
from inefficiency_engine.dashboard_command_center_v6 import (
    COMMAND_CENTER_LAYOUT_VERSION,
    DASHBOARD_COMMAND_CENTER_HTML,
)


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"

_SOURCE_CONNECTIVITY_SECTION = r'''<section class="section"><div class="section-head"><div><div class="section-title">Source connectivity</div><div class="section-note">Independent persisted source diagnostics · updates even if the main dashboard snapshot fails</div></div><div id="sourceSummary" class="section-note">Loading source status…</div></div><div id="sourceProblems"></div></section>
'''
_SOURCE_SECTION_MARKER = '<section class="section"><div class="section-head"><div><div class="section-title">Evidence accumulation</div>'
_SOURCE_CONNECTIVITY_JS = r'''
function renderSourceConnectivity(p){const s=p?.summary||{},rows=p?.sources||[];if(!p?.available){$('sourceSummary').textContent=`Source diagnostic unavailable${p?.read_error_type?` · ${p.read_error_type}`:''}`;$('sourceProblems').innerHTML=empty('Could not read persisted source status.');return}const attention=rows.filter(x=>!['healthy','not_applicable'].includes(x.state)),parts=[`${num(s.stale)} stale`,`${num(s.failed)} failed`,`${num(s.unobserved)} unobserved`,`${num(s.credential_required)} credential-gated`,`${num(s.awaiting_endogenous)} endogenous waiting`,`${num(s.not_applicable)} policy-disabled`];$('sourceSummary').textContent=`${num(s.healthy)} / ${num(s.connectivity_configured??s.configured)} connected · ${parts.join(' · ')}`;const rank={failed:0,stale:1,unobserved:2,credential_required:3,awaiting_endogenous:4};attention.sort((a,b)=>(rank[a.state]??9)-(rank[b.state]??9)||String(a.name).localeCompare(String(b.name)));$('sourceProblems').innerHTML=attention.length?attention.map(x=>{const observation=x.state==='awaiting_endogenous'?'Awaiting governed activity':x.observed_at?`Last observation ${when(x.observed_at)} · ${age(x.age_seconds)} · TTL ${age(x.freshness_ttl_seconds)}`:'No persisted observation';return `<div class="item"><div class="item-top"><div><div class="item-title">${esc(x.name||x.source_id)}</div><div class="item-sub">${esc((x.lane_ids||[]).join(' · '))} · ${esc((x.classes||[]).join(', '))}</div></div><span class="badge ${clsStatus(x.state)}">${esc(String(x.state||'unknown').replaceAll('_',' '))}</span></div><div class="item-sub">${observation}${x.error_type?` · error ${esc(x.error_type)}`:''}${x.credential_env&&!x.credential_configured?` · configure ${esc(x.credential_env)}`:''}</div></div>`}).join(''):empty('All applicable provider source surfaces currently report healthy.')}
async function refreshSourceConnectivity(){try{const r=await fetch('/v3/dashboard/source-connectivity',{cache:'no-store'});const p=await r.json();renderSourceConnectivity(p)}catch(e){$('sourceSummary').textContent='Source diagnostic unavailable';$('sourceProblems').innerHTML=empty(`Source diagnostic failed: ${e.message}`)}}
'''
_SOURCE_JS_MARKER = "function renderSummary(s){"
_OLD_REFRESH_JS = "async function refresh(){const b=$('refreshBtn');b.disabled=true;$('error').classList.remove('show');try{const r=await fetch('/v3/dashboard/v5-snapshot',{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);const p=await r.json();if(p.dashboard_ui_contract_version!=='v5_mechanism_truth')throw new Error(`Unexpected card contract ${p.dashboard_ui_contract_version||'missing'}`);render(p)}catch(e){$('error').textContent=`Dashboard refresh failed: ${e.message}`;$('error').classList.add('show')}finally{b.disabled=false}}"
_NEW_REFRESH_JS = r'''async function refresh(){const b=$('refreshBtn');b.disabled=true;$('error').classList.remove('show');try{const r=await fetch('/v3/dashboard/v5-snapshot',{cache:'no-store'});if(!r.ok){let d=null;try{d=(await r.json())?.detail}catch(_e){}let m=`HTTP ${r.status}`;if(d&&typeof d==='object'){if(d.stage)m+=` · ${d.stage}`;if(d.error_type)m+=` · ${d.error_type}`;if(d.cause_type)m+=` / ${d.cause_type}`}else if(d)m+=` · ${d}`;throw new Error(m)}const p=await r.json();if(p.dashboard_ui_contract_version!=='v5_mechanism_truth')throw new Error(`Unexpected card contract ${p.dashboard_ui_contract_version||'missing'}`);render(p)}catch(e){$('error').textContent=`Dashboard refresh failed: ${e.message}`;$('error').classList.add('show')}finally{b.disabled=false}}'''
_OLD_BOOT_JS = "$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>renderChart(window.__history||[]));refresh();setInterval(refresh,30000);"
_NEW_BOOT_JS = "$('refreshBtn').addEventListener('click',()=>{refresh();refreshSourceConnectivity()});window.addEventListener('resize',()=>refresh());refresh();refreshSourceConnectivity();setInterval(()=>{refresh();refreshSourceConnectivity()},30000);"

# Do not mutate the inherited FastAPI router in place. The inherited application
# has already been composed through several deploy layers and Starlette may have
# materialized its ASGI middleware/router stack by the time this module imports it.
# In that state app.router.routes can look correct while requests still dispatch
# through a previously-built legacy root. A fresh final application makes the
# canonical routes authoritative at ASGI request time and then appends only the
# non-conflicting legacy diagnostic/API routes.
_legacy_app = _base.app
app = FastAPI(
    title=getattr(_legacy_app, "title", "Crypto Inefficiency Engine Read Plane"),
    version=__version__,
)

_CANONICAL_PATHS = {
    "/",
    "/dashboard",
    "/health",
    "/ready",
    "/v3/dashboard/snapshot",
    "/v3/dashboard/v5-snapshot",
}


def _html_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Dashboard-Contract": DASHBOARD_UI_CONTRACT_VERSION,
        "X-Dashboard-Layout": COMMAND_CENTER_LAYOUT_VERSION,
        "X-Canonical-API-App": CANONICAL_API_APP,
    }


def _dashboard_html() -> str:
    """Serve the command center with an independent source-diagnostic read path."""

    html = DASHBOARD_COMMAND_CENTER_HTML.replace(
        _SOURCE_SECTION_MARKER,
        _SOURCE_CONNECTIVITY_SECTION + _SOURCE_SECTION_MARKER,
        1,
    )
    html = html.replace(
        _SOURCE_JS_MARKER,
        _SOURCE_CONNECTIVITY_JS + _SOURCE_JS_MARKER,
        1,
    )
    html = html.replace(_OLD_REFRESH_JS, _NEW_REFRESH_JS, 1)
    html = html.replace(_OLD_BOOT_JS, _NEW_BOOT_JS, 1)
    return html


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _command_center_payload(legacy: dict[str, object]) -> dict[str, object]:
    """Expose the non-mechanism command-center sections beside V5 card truth."""

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
        "runtime_heartbeats": _dict(legacy.get("runtime_heartbeats")),
        "projection_mode": legacy.get("projection_mode"),
        "presentation_fallback": bool(legacy.get("presentation_fallback")),
        "presentation_fallback_reason": legacy.get("presentation_fallback_reason"),
    }


def _runtime_contract(payload: dict[str, object]) -> dict[str, object]:
    result = dict(payload)
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "command_center_layout_version": COMMAND_CENTER_LAYOUT_VERSION,
            "canonical_api_app": CANONICAL_API_APP,
            "dashboard_card_truth_resolver_active": True,
            "dashboard_card_read_model": "standalone_server_built_v5",
            "dashboard_inherited_card_overlay_chain_active": False,
            "dashboard_final_router_rebuilt": True,
            "dashboard_conclusion_currentness_active": True,
            "dashboard_snapshot_backward_compatible": True,
            "legacy_snapshot_fields_preserved": True,
        }
    )
    return result


def _legacy_snapshot() -> dict[str, object]:
    try:
        payload = _base.dashboard_snapshot()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "canonical persisted dashboard snapshot is temporarily unavailable",
                "stage": "canonical_snapshot_adapter",
                "error_type": type(exc).__name__,
            },
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=503,
            detail={
                "message": "canonical dashboard snapshot is invalid",
                "stage": "canonical_snapshot_adapter",
                "error_type": "InvalidSnapshotPayload",
            },
        )
    return dict(payload)


def _v5_from_legacy(legacy: dict[str, object] | None = None) -> dict[str, object]:
    try:
        source = dict(legacy) if isinstance(legacy, dict) else _legacy_snapshot()
        v5 = build_dashboard_v5_snapshot(source)
        v5 = preserve_meaningful_card_conclusions(v5)
        v5.update(
            {
                "command_center_layout_version": COMMAND_CENTER_LAYOUT_VERSION,
                "command_center": _command_center_payload(source),
            }
        )
        return v5
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "v5 mechanism truth snapshot is temporarily unavailable",
                "stage": "v5_card_build",
                "error_type": type(exc).__name__,
            },
        ) from exc


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def dashboard_root() -> HTMLResponse:
    return HTMLResponse(_dashboard_html(), headers=_html_headers())


@app.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
def portfolio_dashboard() -> HTMLResponse:
    return HTMLResponse(_dashboard_html(), headers=_html_headers())


@app.get("/health")
def deployment_health():
    return _runtime_contract(dict(_base.deployment_health()))


@app.get("/ready")
def deployment_readiness():
    return _runtime_contract(dict(_base.deployment_readiness()))


@app.get("/v3/dashboard/v5-snapshot")
def dashboard_v5_snapshot():
    result = _v5_from_legacy()
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "dashboard_route_authority": "final-fresh-router",
            "canonical_api_app": CANONICAL_API_APP,
            "legacy_snapshot_fields_available": True,
        }
    )
    return result


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    try:
        legacy = _legacy_snapshot()
        v5 = _v5_from_legacy(legacy)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "v5 mechanism truth snapshot is temporarily unavailable",
                "stage": "final_snapshot_adapter",
                "error_type": type(exc).__name__,
            },
        ) from exc

    # Keep the compact legacy fields for diagnostic consumers while making the V5
    # card model and command-center context available from the same persisted read.
    result = dict(legacy)
    result.update(v5)
    result.update(
        {
            "dashboard_contract_active": True,
            "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
            "command_center_layout_version": COMMAND_CENTER_LAYOUT_VERSION,
            "canonical_api_app": CANONICAL_API_APP,
            "dashboard_route_authority": "final-fresh-router",
            "dashboard_snapshot_backward_compatible": True,
            "legacy_snapshot_fields_preserved": True,
        }
    )
    return result


# Append inherited routes only after every canonical production path above has
# been registered. Conflicting historical dashboard routes are intentionally not
# copied. This preserves the detailed read API without allowing an older root or
# snapshot endpoint to shadow the final dashboard contract.
for _route in _legacy_app.router.routes:
    _path = getattr(_route, "path", None)
    _methods = getattr(_route, "methods", set()) or set()
    if _path in _CANONICAL_PATHS and "GET" in _methods:
        continue
    app.router.routes.append(_route)

app.openapi_schema = None
