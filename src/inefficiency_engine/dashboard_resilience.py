from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard_integrity import INTEGRITY_DASHBOARD_HTML


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError("dashboard resilience overlay target changed unexpectedly")
    return source.replace(old, new, 1)


def _build_resilient_dashboard_html() -> str:
    """Serve the command center from one worker-published compact projection.

    The browser makes one bounded request per refresh instead of fanning out ten
    concurrent PostgreSQL-backed reads. The latest successful projection remains
    available in session storage for brief API/database interruptions.
    """

    html = INTEGRITY_DASHBOARD_HTML
    html = _replace_once(
        html,
        "async function getJSON(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`${url}: HTTP ${r.status}`);return r.json()}\n"
        "async function safeJSON(url,fallback){try{return await getJSON(url)}catch(e){return {...fallback,__error:e.message}}}",
        """const DASHBOARD_REQUEST_TIMEOUT_MS=5000;
async function getJSON(url,timeoutMs=DASHBOARD_REQUEST_TIMEOUT_MS){const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),timeoutMs);try{const r=await fetch(url,{cache:'no-store',signal:controller.signal});if(!r.ok){const e=new Error(`${url}: HTTP ${r.status}`);e.status=r.status;throw e}return await r.json()}catch(e){if(e&&e.name==='AbortError'){const timeoutError=new Error(`${url}: timed out after ${timeoutMs}ms`);timeoutError.status=504;throw timeoutError}throw e}finally{clearTimeout(timer)}}
const DASHBOARD_CACHE_TTL_MS=15*60*1000;
const dashboardLastGood={};
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function saveLastGood(key,payload){dashboardLastGood[key]=payload;try{sessionStorage.setItem(`cie-dashboard-${key}`,JSON.stringify({saved_at:Date.now(),payload}))}catch(e){}}
function loadLastGood(key){if(dashboardLastGood[key])return dashboardLastGood[key];try{const raw=sessionStorage.getItem(`cie-dashboard-${key}`);if(!raw)return null;const cached=JSON.parse(raw);if(!cached||Date.now()-(+cached.saved_at||0)>DASHBOARD_CACHE_TTL_MS){sessionStorage.removeItem(`cie-dashboard-${key}`);return null}dashboardLastGood[key]=cached.payload;return cached.payload}catch(e){return null}}
async function resilientJSON(url,key,fallback,attempts=2){let lastError=null;for(let attempt=0;attempt<attempts;attempt++){try{const payload=await getJSON(url,5000);saveLastGood(key,payload);return payload}catch(e){lastError=e;const transient=e.status===502||e.status===503||e.status===504||e.status===429||e.status===undefined;if(!transient||attempt===attempts-1)break;await wait(350*Math.pow(2,attempt))}}const cached=loadLastGood(key);if(cached)return {...cached,__error:lastError?.message||`${url}: temporarily unavailable`,__stale:true};return {...fallback,__error:lastError?.message||`${url}: temporarily unavailable`,__stale:true}}
async function safeJSON(url,fallback){try{return await getJSON(url,5000)}catch(e){return {...fallback,__error:e.message}}}""",
    )
    html = _replace_once(
        html,
        "const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([",
        """const dashboardSnapshot=resilientJSON('/v3/dashboard/snapshot','dashboard',{portfolio:{available:false},performance:{},runtime:{operational:false,degraded:true,valuation_status:'unavailable',allocation_family_failures:[],cycle_status:'unavailable'},positions:{positions:[]},trades:{trades:[]},history:{count:0,snapshots:[]},skips:{skips:[]},attribution:{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}},mechanisms:{mechanisms:[],requirements:{}},queue:{actions:[]}});
const projectionSection=(payload,key,fallback,reportError=false)=>{const section={...fallback,...(payload?.[key]||{})};if(reportError&&payload?.__error){section.__error=payload.__error;section.__stale=payload.__stale}return section};
const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([""",
    )
    html = _replace_once(
        html,
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/runtime-status'),safeJSON('/v3/portfolio/positions',{positions:[]}),safeJSON('/v3/portfolio/trades?limit=20',{trades:[]}),safeJSON('/v3/portfolio/history?limit=500',{count:0,snapshots:[]}),safeJSON('/v3/portfolio/skips?limit=20',{skips:[]}),safeJSON('/v3/portfolio/attribution',{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}}),safeJSON('/v3/operations/mechanisms',{mechanisms:[],requirements:{}}),safeJSON('/v3/operations/action-queue',{actions:[]})",
        "dashboardSnapshot.then(x=>projectionSection(x,'portfolio',{available:false},true)),dashboardSnapshot.then(x=>projectionSection(x,'performance',{})),dashboardSnapshot.then(x=>projectionSection(x,'runtime',{operational:false,degraded:true,valuation_status:'unavailable',allocation_family_failures:[],cycle_status:'unavailable'})),dashboardSnapshot.then(x=>projectionSection(x,'positions',{positions:[]})),dashboardSnapshot.then(x=>projectionSection(x,'trades',{trades:[]})),dashboardSnapshot.then(x=>projectionSection(x,'history',{count:0,snapshots:[]})),dashboardSnapshot.then(x=>projectionSection(x,'skips',{skips:[]})),dashboardSnapshot.then(x=>projectionSection(x,'attribution',{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}})),dashboardSnapshot.then(x=>projectionSection(x,'mechanisms',{mechanisms:[],requirements:{}})),dashboardSnapshot.then(x=>projectionSection(x,'queue',{actions:[]}))",
    )
    html = _replace_once(
        html,
        "const partial=[positions,trades,history,skips,attribution,mechanisms,queue].filter(x=>x&&x.__error).map(x=>x.__error);if(partial.length){$('error').textContent=`Partial dashboard data unavailable: ${partial.join(' · ')}`;$('error').classList.add('show')}",
        "const partial=[portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue].filter(x=>x&&x.__error).map(x=>x.__error);if(partial.length){const stale=[portfolio,performance,runtime].some(x=>x&&x.__stale);$('error').textContent=`${stale?'Temporary API issue; showing last known dashboard projection':'Dashboard projection temporarily unavailable'}: ${[...new Set(partial)].join(' · ')}`;$('error').classList.add('show')}",
    )
    html = _replace_once(
        html,
        "Auto-refresh: 30 seconds · Account freshness and market-valuation freshness tracked separately",
        "Auto-refresh: 30 seconds · One compact worker-published snapshot per refresh · detailed endpoints remain diagnostic-only",
    )
    html = _replace_once(
        html,
        "async function refresh(){",
        """function strategyEvidenceDetail(r){
  const rows=r?.strategy_evidence||[];if(!rows.length)return '';
  const gateText=s=>(s.failed_gates||[]).length?(s.failed_gates||[]).map(x=>`<div class="bad" style="font-size:11px;margin-top:2px">• ${esc(x)}</div>`).join(''):'<div class="good" style="font-size:11px;margin-top:2px">No diagnostic evidence gate currently failing</div>';
  const metric=(label,value)=>`<span style="display:inline-block;margin-right:10px"><span class="muted">${esc(label)}</span> ${esc(value)}</span>`;
  return `<details style="margin-top:8px"><summary style="cursor:pointer;color:#bae6fd;font-weight:750">Strategy evidence (${rows.length})</summary><div style="display:grid;gap:7px;margin-top:7px">${rows.map(s=>{
    const forwardRequired=+s.required_forward_outcomes||0,forward=+s.independent_forward_outcome_count||0,regimeRequired=+s.required_regimes||0,regimes=+s.observed_regime_count||0;
    const forwardMean=Number.isFinite(+s.mean_forward_net_return)?pct(s.mean_forward_net_return):'—',meanLower=Number.isFinite(+s.mean_forward_net_return_ci_lower)?pct(s.mean_forward_net_return_ci_lower):'—',meanRequired=Number.isFinite(+s.required_mean_return_ci_lower)?pct(s.required_mean_return_ci_lower):'—';
    const hit=Number.isFinite(+s.forward_hit_rate)?pct(s.forward_hit_rate):'—',hitLower=Number.isFinite(+s.forward_hit_rate_ci_lower)?pct(s.forward_hit_rate_ci_lower):'—',hitRequired=Number.isFinite(+s.required_hit_rate_ci_lower)?pct(s.required_hit_rate_ci_lower):'—';
    const forwardLabel=forwardRequired?`${num(forward)} / ${num(forwardRequired)}`:'n/a',regimeLabel=regimeRequired?`${num(regimes)} / ${num(regimeRequired)}`:'n/a';
    return `<div class="item" style="margin:0"><div class="item-top"><div><div class="item-title">${esc(s.name||s.strategy_id)}</div><div class="item-sub">${esc(s.strategy_id)} · qualification scope: ${esc(s.qualification_scope||'strategy-specific')}</div></div><span class="state ${esc(s.state)}">${esc((s.state||'unknown').replaceAll('_',' '))}</span></div><div class="item-sub" style="margin-top:7px">${metric('Forward',forwardLabel)}${metric('Mean',forwardMean)}${metric('CI lower',`${meanLower} vs ${meanRequired}`)}${metric('Hit',hit)}${metric('Hit CI',`${hitLower} vs ${hitRequired}`)}${metric('Regimes',regimeLabel)}${metric('Settled',num(s.settled_allocator_outcome_count||0))}</div><div class="item-sub" style="margin-top:5px">${esc(s.primary_reason||'No strategy-specific reason recorded')}</div>${gateText(s)}</div>`
  }).join('')}</div><div class="muted" style="font-size:10px;margin-top:6px">Diagnostic aggregation only. Capital authority remains qualified independently by strategy + asset + direction; historical failed cohorts are not reset.</div></details>`;
}
renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class="state ${esc(r.state)}">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class="muted">${esc(r.mechanism_id)}</span></td><td>${state(r)}</td><td class="num">${num(r.independent_forward_outcome_count)}</td><td class="num">${num(r.settled_allocator_outcome_count)}</td><td class="num ${pnlClass(r.mean_forward_net_return)}">${pct(r.mean_forward_net_return)}</td><td><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan="6" class="muted">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class="item"><div class="item-top"><div class="item-title">${esc(r.name)}</div>${state(r)}</div><div class="item-sub">${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}
async function refresh(){""",
    )
    return html


RESILIENT_DASHBOARD_HTML = _build_resilient_dashboard_html()


def build_dashboard_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", include_in_schema=False, response_class=HTMLResponse)
    def dashboard_root() -> HTMLResponse:
        return HTMLResponse(RESILIENT_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    @router.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
    def portfolio_dashboard() -> HTMLResponse:
        return HTMLResponse(RESILIENT_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    return router
