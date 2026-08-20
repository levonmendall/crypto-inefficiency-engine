from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard_integrity import INTEGRITY_DASHBOARD_HTML


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError("dashboard resilience overlay target changed unexpectedly")
    return source.replace(old, new, 1)


def _build_resilient_dashboard_html() -> str:
    """Serve one HTTP snapshot with independently refreshed portfolio/research projections."""

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
        "    const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;\n"
        "    return `<div class=\"evidence-card\"><div class=\"evidence-card-head\"><div><div class=\"evidence-card-name\">${esc(r.name)}</div><div class=\"evidence-card-meta\">${esc((r.stage||'unknown').replaceAll('_',' '))} · ${esc(worker)} · ${esc(persist)}</div></div><span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span></div><div class=\"evidence-pipeline\">${evidenceStep('Provider',r.provider_ready?'Ready':'Gap',`${obs} authoritative`,providerCls)}${evidenceStep('Observations',num(obs),`${signals} signals`,obsCls)}${evidenceStep('Forward',forwardApplicable?`${num(outcomes)} / ${num(forwardTarget)}`:'Not enabled',forwardApplicable?'independent outcomes':'stage not forward-testable',forwardCls)}${evidenceStep('Qualified',forwardApplicable?num(q):'—','current statistical gate',statsCls)}${evidenceStep('Executable',stage>=4?num(p):'—','current L2 / cost / capacity',executionCls)}${evidenceStep('Settled',settlementApplicable?`${num(settled)} / ${num(settledTarget)}`:'Not enabled',settlementApplicable?'allocator outcomes':'allocation not enabled',settlementCls)}${evidenceStep('Certified',r.state==='certified'?'Yes':'No','profitability certification',certCls)}</div><div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div><div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
        "    const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at,projected=r.research_projection_observed_at||observedAt;\n"
        "    return `<div class=\"evidence-card\"><div class=\"evidence-card-head\"><div><div class=\"evidence-card-name\">${esc(r.name)}</div><div class=\"evidence-card-meta\">${esc((r.stage||'unknown').replaceAll('_',' '))} · ${esc(worker)} · ${esc(persist)}</div></div><span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span></div><div class=\"evidence-pipeline\">${evidenceStep('Provider',r.provider_ready?'Ready':'Gap',`${obs} authoritative`,providerCls)}${evidenceStep('Observations',num(obs),`${signals} signals`,obsCls)}${evidenceStep('Forward',forwardApplicable?`${num(outcomes)} / ${num(forwardTarget)}`:'Not enabled',forwardApplicable?'independent outcomes':'stage not forward-testable',forwardCls)}${evidenceStep('Qualified',forwardApplicable?num(q):'—','current statistical gate',statsCls)}${evidenceStep('Executable',stage>=4?num(p):'—','current L2 / cost / capacity',executionCls)}${evidenceStep('Settled',settlementApplicable?`${num(settled)} / ${num(settledTarget)}`:'Not enabled',settlementApplicable?'allocator outcomes':'allocation not enabled',settlementCls)}${evidenceStep('Certified',r.state==='certified'?'Yes':'No','profitability certification',certCls)}</div><div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div><div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${projected?`Updated ${when(projected)}`:'Update time unavailable'}${last?` · Last evidence ${when(last)}`:' · No forward evidence yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
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
        "Auto-refresh: 30 seconds · One HTTP snapshot · portfolio and research projections refresh independently",
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
