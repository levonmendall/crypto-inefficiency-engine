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
        '  <section class="card section full">\n    <div class="section-head"><div><div class="section-title">Evidence accumulation</div>',
        '''  <section class="card section full">
    <div class="section-head"><div><div class="section-title">Cycle history backfill</div><div class="section-note">Separated historical research · never counted as forward evidence</div></div><div id="cycleHistorySummary" class="section-note">Awaiting maintenance status</div></div>
    <div id="cycleHistoryList" class="queue"><div class="muted">Awaiting first historical maintenance pass.</div></div>
  </section>

  <section class="card section full">
    <div class="section-head"><div><div class="section-title">Evidence accumulation</div>''',
    )
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
        """const dashboardSnapshot=resilientJSON('/v3/dashboard/snapshot','dashboard',{portfolio:{available:false},performance:{},runtime:{operational:false,degraded:true,valuation_status:'unavailable',allocation_family_failures:[],cycle_status:'unavailable'},positions:{positions:[]},trades:{trades:[]},history:{count:0,snapshots:[]},skips:{skips:[]},attribution:{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}},mechanisms:{mechanisms:[],requirements:{}},queue:{actions:[]},cycle_history:{available:false,assets:[]},lane_executability:{available:false,lane_count:13,paper_execution_capable_lanes:[]}});
const projectionSection=(payload,key,fallback,reportError=false)=>{const section={...fallback,...(payload?.[key]||{})};if(reportError&&payload?.__error){section.__error=payload.__error;section.__stale=payload.__stale}return section};
const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue,cycleHistory]=await Promise.all([""",
    )
    html = _replace_once(
        html,
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/runtime-status'),safeJSON('/v3/portfolio/positions',{positions:[]}),safeJSON('/v3/portfolio/trades?limit=20',{trades:[]}),safeJSON('/v3/portfolio/history?limit=500',{count:0,snapshots:[]}),safeJSON('/v3/portfolio/skips?limit=20',{skips:[]}),safeJSON('/v3/portfolio/attribution',{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}}),safeJSON('/v3/operations/mechanisms',{mechanisms:[],requirements:{}}),safeJSON('/v3/operations/action-queue',{actions:[]})",
        "dashboardSnapshot.then(x=>projectionSection(x,'portfolio',{available:false},true)),dashboardSnapshot.then(x=>projectionSection(x,'performance',{})),dashboardSnapshot.then(x=>projectionSection(x,'runtime',{operational:false,degraded:true,valuation_status:'unavailable',allocation_family_failures:[],cycle_status:'unavailable'})),dashboardSnapshot.then(x=>projectionSection(x,'positions',{positions:[]})),dashboardSnapshot.then(x=>projectionSection(x,'trades',{trades:[]})),dashboardSnapshot.then(x=>projectionSection(x,'history',{count:0,snapshots:[]})),dashboardSnapshot.then(x=>projectionSection(x,'skips',{skips:[]})),dashboardSnapshot.then(x=>projectionSection(x,'attribution',{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}})),dashboardSnapshot.then(x=>projectionSection(x,'mechanisms',{mechanisms:[],requirements:{}})),dashboardSnapshot.then(x=>projectionSection(x,'queue',{actions:[]})),dashboardSnapshot.then(x=>projectionSection(x,'cycle_history',{available:false,assets:[]}))",
    )
    html = _replace_once(
        html,
        "const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue,cycleHistory]=await Promise.all([",
        "const dashboardMeta=await dashboardSnapshot;\nconst [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue,cycleHistory]=await Promise.all([",
    )
    html = _replace_once(
        html,
        "const partial=[positions,trades,history,skips,attribution,mechanisms,queue].filter(x=>x&&x.__error).map(x=>x.__error);if(partial.length){$('error').textContent=`Partial dashboard data unavailable: ${partial.join(' · ')}`;$('error').classList.add('show')}",
        "const partial=[portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue].filter(x=>x&&x.__error).map(x=>x.__error);if(partial.length){const stale=[portfolio,performance,runtime].some(x=>x&&x.__stale);$('error').textContent=`${stale?'Temporary API issue; showing last known dashboard projection':'Dashboard projection temporarily unavailable'}: ${[...new Set(partial)].join(' · ')}`;$('error').classList.add('show')}",
    )
    html = _replace_once(
        html,
        '<div class="status-row"><span class="muted">Mechanisms certified</span><span id="certifiedCount" class="status-val">—</span></div>',
        '<div class="status-row"><span class="muted">Mechanisms certified</span><span id="certifiedCount" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Evidence-connected lanes</span><span id="sourceConnectedCount" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Decision-grade lanes</span><span id="decisionGradeCount" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Paper-capable lanes</span><span id="paperCapableCount" class="status-val">—</span></div>'
        '<div class="status-row"><span class="muted">Card data</span><span id="cardTruthStatus" class="status-val">—</span></div>',
    )
    html = _replace_once(
        html,
        "function renderEvidenceProgress(rows,requirements,observedAt){",
        "function renderEvidenceProgress(rows,requirements,observedAt,laneTruth){",
    )
    html = _replace_once(
        html,
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,promoted=rows.filter(r=>+r.current_promoted_count>0).length,certified=rows.filter(r=>r.state==='certified').length;",
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,promoted=rows.filter(r=>+r.current_promoted_count>0).length,certified=rows.filter(r=>r.state==='certified').length;const laneCount=Math.max(1,+laneTruth?.lane_count||rows.length||13),paperCapableIds=new Set(laneTruth?.paper_execution_capable_lanes||[]),paperCapableCount=laneTruth?.available?(+laneTruth.paper_execution_capable_count||0):null;",
    )
    html = _replace_once(
        html,
        "stat('Qualified now',num(qualified)),stat('Executable now',num(promoted)),stat('Certified',`${certified} / ${rows.length}`)",
        "stat('Qualified now',num(qualified)),stat('Paper-capable',paperCapableCount===null?'—':`${num(paperCapableCount)} / ${num(laneCount)}`),stat('Certified',`${certified} / ${rows.length}`)",
    )
    html = _replace_once(
        html,
        "    const stage=order[r.stage]??0,obs=+r.authoritative_observation_count||0,signals=+r.forward_signal_count||0,outcomes=+r.independent_forward_outcome_count||0,q=+r.current_statistically_qualified_count||0,p=+r.current_promoted_count||0,settled=+r.settled_allocator_outcome_count||0;",
        "    const stage=order[r.stage]??0,obs=+r.authoritative_observation_count||0,signals=+r.forward_signal_count||0,outcomes=+r.independent_forward_outcome_count||0,q=+r.current_statistically_qualified_count||0,p=+r.current_promoted_count||0,settled=+r.settled_allocator_outcome_count||0,paperReady=paperCapableIds.has(r.mechanism_id),projectionCurrent=laneTruth?.projection_current_for_execution===true;",
    )
    html = _replace_once(
        html,
        "${evidenceStep('Executable',stage>=4?num(p):'—','current L2 / cost / capacity',executionCls)}",
        "${evidenceStep('Paper-capable',laneTruth?.available?(paperReady?'Yes':'No'):'—',!projectionCurrent?'stale or unavailable projection · fail closed':(paperReady?'decision-grade + source-connected':'not decision-grade/currently executable'),paperReady?'done':(projectionCurrent?'active':'blocked'))}",
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
function renderCycleHistory(payload){
  const rows=payload?.assets||[],summary=$('cycleHistorySummary'),list=$('cycleHistoryList');if(!summary||!list)return;
  if(!payload?.available){summary.textContent='Awaiting first maintenance pass';list.innerHTML=itemEmpty('Historical maintenance has not published durable status yet.');return}
  const coverage=Number.isFinite(+payload.overall_coverage_fraction)?pct(payload.overall_coverage_fraction):'—';
  summary.textContent=payload.all_complete?`Complete · ${coverage} coverage`:`${num(payload.complete_asset_count||0)} / ${num(payload.asset_count||0)} complete · ${coverage} coverage · retrying incomplete assets`;
  list.innerHTML=rows.length?rows.map(r=>{
    const stateClass=r.complete?'certified':'collecting',stateLabel=r.complete?'Complete':'Retrying';
    const coverageLabel=Number.isFinite(+r.coverage_fraction)?pct(r.coverage_fraction):'—';
    const range=(r.earliest_observed_at&&r.latest_observed_at)?`${when(r.earliest_observed_at)} → ${when(r.latest_observed_at)}`:'No durable historical range yet';
    const replay=r.historical_replay_long_qualified?'Long replay support qualified':(r.walk_forward_ready?'Walk-forward span ready · replay support not qualified yet':'Building walk-forward span');
    const error=r.last_error_type?`<div class="bad" style="margin-top:5px">Last fetch error: ${esc(r.last_error_type)}</div>`:'';
    const retry=(!r.complete&&r.next_retry_at)?` · next retry ${when(r.next_retry_at)}`:'';
    return `<div class="queue-item"><div class="queue-title"><strong>${esc(r.asset)}</strong><span class="state ${stateClass}">${stateLabel}</span></div><div class="queue-reason">${num(r.quote_count||0)} / ${num(r.expected_quote_count||0)} candles · ${coverageLabel} · ${esc(range)}</div><div class="queue-action">${esc(replay)} · historical samples never increment genuine forward outcomes${esc(retry)}</div>${error}</div>`;
  }).join(''):itemEmpty('No cycle-history assets are configured.')
}
renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class="state ${esc(r.state)}">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class="muted">${esc(r.mechanism_id)}</span></td><td>${state(r)}</td><td class="num">${num(r.independent_forward_outcome_count)}</td><td class="num">${num(r.settled_allocator_outcome_count)}</td><td class="num ${pnlClass(r.mean_forward_net_return)}">${pct(r.mean_forward_net_return)}</td><td><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan="6" class="muted">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class="item"><div class="item-top"><div class="item-title">${esc(r.name)}</div>${state(r)}</div><div class="item-sub">${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}
async function refresh(){""",
    )
    html = _replace_once(
        html,
        "async function refresh(){",
        """function renderDashboardTruth(snapshot){
  const lane=snapshot?.lane_executability||{},laneCount=Math.max(1,+lane.lane_count||13),available=!!lane.available;
  const connected=available?(+lane.production_evidence_connected_count||0):null,decision=available?(+lane.decision_grade_outcome_qualified_count||0):null,paper=available?(+lane.paper_execution_capable_count||0):null;
  const set=(id,text,color)=>{const el=$(id);if(!el)return;el.textContent=text;el.style.color=color};
  set('sourceConnectedCount',connected===null?'Unavailable':`${num(connected)} / ${num(laneCount)}`,connected===laneCount?'#4ade80':(connected===null?'#fb7185':'#facc15'));
  set('decisionGradeCount',decision===null?'Unavailable':`${num(decision)} / ${num(laneCount)}`,decision>0?'#4ade80':(decision===null?'#fb7185':'#8ea7b5'));
  const projectionCurrent=lane.projection_current_for_execution===true;
  set('paperCapableCount',paper===null?'Unavailable':`${num(paper)} / ${num(laneCount)}${projectionCurrent?'':' · fail-closed'}`,paper>0&&projectionCurrent?'#4ade80':(paper===null?'#fb7185':projectionCurrent?'#8ea7b5':'#facc15'));
  const staleReasons=[];if(snapshot?.__stale)staleReasons.push('showing a cached snapshot after an API failure');if(snapshot?.research_projection_stale)staleReasons.push(snapshot?.research_projection_freshness?.reason||'research projection is stale');if(snapshot?.operating_projection_stale)staleReasons.push(snapshot?.operating_projection_freshness?.reason||'operating certification projection is stale');
  const current=available&&projectionCurrent&&!snapshot?.__stale&&!staleReasons.length;const release=snapshot?.release_commit?` · ${String(snapshot.release_commit).slice(0,7)}`:'';
  set('cardTruthStatus',current?`Current${release}`:(available?`Stale / fail-closed${release}`:'Unavailable'),current?'#4ade80':(available?'#facc15':'#fb7185'));
  const notice=$('notice');if(staleReasons.length){notice.textContent=`Card data is not current: ${staleReasons.join(' · ')}. Paper-capable lane status remains fail-closed until the audited projections are current.`;notice.classList.add('show')}else if(notice?.textContent?.startsWith('Card data is not current:')){notice.textContent='';notice.classList.remove('show')}
}
async function refresh(){""",
    )
    html = _replace_once(
        html,
        'renderChart(history.snapshots);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderEvidenceProgress(mechRows,mechanisms.requirements||{},mechanisms.observed_at);renderMechanisms(mechRows);renderQueue(queue.actions);',
        'window.__dashboardHistory=history.snapshots||[];renderChart(window.__dashboardHistory);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderCycleHistory(cycleHistory);renderEvidenceProgress(mechRows,mechanisms.requirements||{},mechanisms.observed_at,dashboardMeta.lane_executability||{});renderMechanisms(mechRows);renderQueue(queue.actions);renderDashboardTruth(dashboardMeta);',
    )
    html = _replace_once(
        html,
        "$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>refresh());refresh();setInterval(refresh,30000);",
        "$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>renderChart(window.__dashboardHistory||[]));refresh();setInterval(refresh,30000);",
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
