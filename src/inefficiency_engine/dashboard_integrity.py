from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard import DASHBOARD_HTML


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError("dashboard integrity overlay target changed unexpectedly")
    return source.replace(old, new, 1)


def _build_integrity_dashboard_html() -> str:
    html = DASHBOARD_HTML
    html = _replace_once(
        html,
        '<div id="updated" class="muted" style="margin-top:9px">Awaiting portfolio data…</div>',
        '<div id="updated" class="muted" style="margin-top:9px">Awaiting portfolio data…</div>'
        '<div id="valuationDetail" class="muted" style="margin-top:5px;font-size:12px">Awaiting valuation provenance…</div>',
    )
    html = _replace_once(
        html,
        '<div class="status-row"><span class="muted">Mechanisms certified</span><span id="certifiedCount" class="status-val">—</span></div>',
        '<div class="status-row"><span class="muted">Runtime</span><span id="runtimeStatus" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Valuation</span><span id="valuationStatus" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Opportunity families</span><span id="familyStatus" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Mechanisms certified</span><span id="certifiedCount" class="status-val">—</span></div>',
    )
    html = _replace_once(
        html,
        'const [portfolio,performance,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([',
        'const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([',
    )
    html = _replace_once(
        html,
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/positions'),getJSON('/v3/portfolio/trades?limit=20'),getJSON('/v3/portfolio/history?limit=500'),getJSON('/v3/portfolio/skips?limit=20'),getJSON('/v3/portfolio/attribution'),getJSON('/v3/operations/mechanisms'),getJSON('/v3/operations/action-queue')",
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/runtime-status'),getJSON('/v3/portfolio/positions'),getJSON('/v3/portfolio/trades?limit=20'),getJSON('/v3/portfolio/history?limit=500'),getJSON('/v3/portfolio/skips?limit=20'),getJSON('/v3/portfolio/attribution'),getJSON('/v3/operations/mechanisms'),getJSON('/v3/operations/action-queue')",
    )
    html = _replace_once(
        html,
        "const nav=+performance.current_nav_usd, ret=+performance.total_return;$('nav').textContent=money(nav);$('totalReturn').textContent=`${ret>=0?'+':''}${pct(ret)} since $250,000 genesis`;$('totalReturn').className=`return ${pnlClass(ret)}`;$('updated').textContent=portfolio.observed_at?`Last portfolio snapshot ${new Date(portfolio.observed_at).toLocaleString()}`:'Portfolio awaiting first worker snapshot';",
        "const nav=+performance.current_nav_usd, ret=+performance.total_return;$('nav').textContent=money(nav);$('totalReturn').textContent=`${ret>=0?'+':''}${pct(ret)} since $250,000 genesis`;$('totalReturn').className=`return ${pnlClass(ret)}`;\n"
        "    const accountText=portfolio.observed_at?`Account snapshot ${new Date(portfolio.observed_at).toLocaleString()}`:'Portfolio awaiting first worker snapshot';const evidenceText=runtime.market_evidence_observed_at?` · Market evidence ${new Date(runtime.market_evidence_observed_at).toLocaleString()}`:'';$('updated').textContent=accountText+evidenceText;\n"
        "    const runtimeLabel=runtime.operational?(runtime.degraded?'Operational · degraded':'Operational'):'Attention';$('runtimeStatus').textContent=runtimeLabel;$('runtimeStatus').style.color=runtime.operational?(runtime.degraded?'#facc15':'#4ade80'):'#fb7185';\n"
        "    const valuation=runtime.valuation_status||portfolio.valuation_status||'unavailable';const valuationLabel=valuation==='cash_only'?'Cash-only · exact':valuation.replaceAll('_',' ');$('valuationStatus').textContent=valuationLabel;$('valuationStatus').style.color=(valuation==='cash_only'||(valuation==='fresh'&&runtime.valuation_fresh))?'#4ade80':(valuation==='partial'?'#facc15':'#fb7185');\n"
        "    const familyFailures=runtime.allocation_family_failures||{};const familyFailureCount=Object.keys(familyFailures).length;$('familyStatus').textContent=familyFailureCount?`${familyFailureCount} degraded`:'Healthy';$('familyStatus').style.color=familyFailureCount?'#facc15':'#4ade80';\n"
        "    const cycleLabel=(runtime.cycle_status||portfolio.cycle_status||'unknown').replaceAll('_',' ');const fallback=runtime.fallback_snapshot?' · fallback accounting snapshot':'';const stale=runtime.stale_position_count?` · ${runtime.stale_position_count} stale position mark(s)`:'';$('valuationDetail').textContent=`Cycle ${cycleLabel}${fallback}${stale}`;",
    )
    html = _replace_once(
        html,
        'Auto-refresh: 30 seconds · Source: canonical durable evidence database',
        'Auto-refresh: 30 seconds · Account freshness and market-valuation freshness tracked separately',
    )
    return html


INTEGRITY_DASHBOARD_HTML = _build_integrity_dashboard_html()


def build_dashboard_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", include_in_schema=False, response_class=HTMLResponse)
    def dashboard_root() -> HTMLResponse:
        return HTMLResponse(INTEGRITY_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    @router.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
    def portfolio_dashboard() -> HTMLResponse:
        return HTMLResponse(INTEGRITY_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    return router
