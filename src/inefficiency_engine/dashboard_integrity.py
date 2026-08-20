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
        "async function getJSON(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`${url}: HTTP ${r.status}`);return r.json()}",
        "async function getJSON(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`${url}: HTTP ${r.status}`);return r.json()}\n"
        "async function safeJSON(url,fallback){try{return await getJSON(url)}catch(e){return {...fallback,__error:e.message}}}",
    )
    html = _replace_once(
        html,
        "</style>",
        """    .evidence-summary{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:8px;margin-bottom:12px}
    .evidence-stat{padding:11px 12px;border:1px solid var(--line);border-radius:12px;background:#0a151e}.evidence-stat .k{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.07em;font-weight:800}.evidence-stat .v{font-size:18px;font-weight:850;margin-top:3px}
    .evidence-progress{display:grid;gap:10px}.evidence-card{padding:13px;border:1px solid var(--line);border-radius:14px;background:#0a151e}.evidence-card-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}.evidence-card-name{font-weight:850}.evidence-card-meta{color:var(--muted);font-size:11px;margin-top:3px}.evidence-pipeline{display:grid;grid-template-columns:repeat(7,minmax(86px,1fr));gap:6px;margin-top:10px}.evidence-step{border:1px solid var(--line);border-radius:9px;padding:8px;background:#071019;min-width:0}.evidence-step.done{border-color:#215836;background:#0b2115}.evidence-step.active{border-color:#245270;background:#0b1d29}.evidence-step.blocked{border-color:#665c22;background:#211d0b}.evidence-step .step-k{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.06em;font-weight:800}.evidence-step .step-v{font-weight:850;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.evidence-step .step-d{color:var(--muted);font-size:9px;margin-top:2px;line-height:1.25}.evidence-reason{margin-top:9px;color:var(--muted);font-size:12px}.evidence-next{margin-top:4px;color:#d7f4fb;font-size:12px}.evidence-time{margin-top:6px;color:var(--muted);font-size:10px}
    @media(max-width:900px){.evidence-summary{grid-template-columns:repeat(3,1fr)}.evidence-pipeline{grid-template-columns:repeat(4,1fr)}}
    @media(max-width:650px){.evidence-summary{grid-template-columns:repeat(2,1fr)}.evidence-pipeline{grid-template-columns:repeat(2,1fr)}}
  </style>""",
    )
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
        '  <section class="card section full">\n    <div class="section-head"><div class="section-title">Profit mechanism certification</div><div class="section-note">Provider → evidence → economics → execution → settlement → certification</div></div>',
        '  <section class="card section full">\n'
        '    <div class="section-head"><div><div class="section-title">Evidence accumulation</div><div id="evidenceSnapshot" class="section-note">Awaiting certification evidence snapshot</div></div><div class="section-note">Read-only progress · thresholds unchanged</div></div>\n'
        '    <div id="evidenceSummary" class="evidence-summary"><div class="muted">No evidence progress yet.</div></div>\n'
        '    <div id="evidenceProgress" class="evidence-progress"><div class="muted">No mechanism evidence snapshot yet.</div></div>\n'
        '  </section>\n\n'
        '  <section class="card section full">\n'
        '    <div class="section-head"><div class="section-title">Profit mechanism certification</div><div class="section-note">Provider → evidence → economics → execution → settlement → certification</div></div>',
    )
    html = _replace_once(
        html,
        "function renderMechanisms(rows){",
        """function evidenceStep(label,value,detail,cls){return `<div class="evidence-step ${esc(cls||'')}"><div class="step-k">${esc(label)}</div><div class="step-v">${esc(value)}</div><div class="step-d">${esc(detail||'')}</div></div>`}
function renderEvidenceProgress(rows,requirements,observedAt){
  rows=rows||[];requirements=requirements||{};const forwardTarget=Math.max(1,+requirements.independent_forward_outcomes||30),settledTarget=Math.max(1,+requirements.settled_allocator_outcomes||20);
  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,promoted=rows.filter(r=>+r.current_promoted_count>0).length,certified=rows.filter(r=>r.state==='certified').length;
  const stat=(k,v)=>`<div class="evidence-stat"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div></div>`;
  $('evidenceSummary').innerHTML=rows.length?[stat('Provider ready',`${providerReady} / ${rows.length}`),stat('Authoritative data',`${observed} / ${rows.length}`),stat(`Forward ≥ ${forwardTarget}`,`${forwardMature} / ${rows.length}`),stat('Qualified now',num(qualified)),stat('Executable now',num(promoted)),stat('Certified',`${certified} / ${rows.length}`)].join(''):itemEmpty('No evidence progress yet.');
  $('evidenceSnapshot').textContent=observedAt?`Certification evidence snapshot ${new Date(observedAt).toLocaleString()} · common forward target ${forwardTarget} · settled-trial target ${settledTarget}`:`Awaiting certification evidence snapshot · forward target ${forwardTarget} · settled-trial target ${settledTarget}`;
  if(!rows.length){$('evidenceProgress').innerHTML=itemEmpty('No mechanism evidence snapshot yet.');return}
  const order={catalogued:0,discoverable:1,economics_modelled:2,forward_testable:3,statistically_gated:4,paper_allocatable:5,profitability_certifiable:6};
  $('evidenceProgress').innerHTML=rows.map(r=>{
    const stage=order[r.stage]??0,obs=+r.authoritative_observation_count||0,signals=+r.forward_signal_count||0,outcomes=+r.independent_forward_outcome_count||0,q=+r.current_statistically_qualified_count||0,p=+r.current_promoted_count||0,settled=+r.settled_allocator_outcome_count||0;
    const forwardApplicable=stage>=3||signals>0||outcomes>0,settlementApplicable=stage>=5||settled>0;
    const statsDone=q>0||['execution_blocked','settlement_blocked','certifying','certified'].includes(r.state),executionDone=p>0||['settlement_blocked','certifying','certified'].includes(r.state),settlementDone=settled>=settledTarget||r.state==='certified';
    const providerCls=r.provider_ready?'done':'blocked',obsCls=obs>0?'done':(r.provider_ready?'active':'blocked'),forwardCls=!forwardApplicable?'':(outcomes>=forwardTarget?'done':'active'),statsCls=statsDone?'done':(forwardApplicable?'active':''),executionCls=executionDone?'done':(statsDone?'active':''),settlementCls=settlementDone?'done':(settlementApplicable?'active':''),certCls=r.state==='certified'?'done':(r.state==='certifying'?'active':'');
    const worker=r.forward_evidence_worker_healthy===true?'worker healthy':(r.forward_evidence_worker_healthy===false?'worker attention':'worker n/a'),persist=r.forward_evidence_persistence_healthy===true?'persistence healthy':(r.forward_evidence_persistence_healthy===false?'persistence attention':'persistence n/a');
    const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;
    return `<div class="evidence-card"><div class="evidence-card-head"><div><div class="evidence-card-name">${esc(r.name)}</div><div class="evidence-card-meta">${esc((r.stage||'unknown').replaceAll('_',' '))} · ${esc(worker)} · ${esc(persist)}</div></div><span class="state ${esc(r.state)}">${esc((r.state||'unknown').replaceAll('_',' '))}</span></div><div class="evidence-pipeline">${evidenceStep('Provider',r.provider_ready?'Ready':'Gap',`${obs} authoritative`,providerCls)}${evidenceStep('Observations',num(obs),`${signals} signals`,obsCls)}${evidenceStep('Forward',forwardApplicable?`${num(outcomes)} / ${num(forwardTarget)}`:'Not enabled',forwardApplicable?'independent outcomes':'stage not forward-testable',forwardCls)}${evidenceStep('Qualified',forwardApplicable?num(q):'—','current statistical gate',statsCls)}${evidenceStep('Executable',stage>=4?num(p):'—','current L2 / cost / capacity',executionCls)}${evidenceStep('Settled',settlementApplicable?`${num(settled)} / ${num(settledTarget)}`:'Not enabled',settlementApplicable?'allocator outcomes':'allocation not enabled',settlementCls)}${evidenceStep('Certified',r.state==='certified'?'Yes':'No','profitability certification',certCls)}</div><div class="evidence-reason">${esc(r.primary_reason||'No current reason recorded')}</div><div class="evidence-next">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class="evidence-time">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`
  }).join('');
}
function renderMechanisms(rows){""",
    )
    html = _replace_once(
        html,
        'const [portfolio,performance,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([',
        'const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([',
    )
    html = _replace_once(
        html,
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/positions'),getJSON('/v3/portfolio/trades?limit=20'),getJSON('/v3/portfolio/history?limit=500'),getJSON('/v3/portfolio/skips?limit=20'),getJSON('/v3/portfolio/attribution'),getJSON('/v3/operations/mechanisms'),getJSON('/v3/operations/action-queue')",
        "getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/runtime-status'),safeJSON('/v3/portfolio/positions',{positions:[]}),safeJSON('/v3/portfolio/trades?limit=20',{trades:[]}),safeJSON('/v3/portfolio/history?limit=500',{count:0,snapshots:[]}),safeJSON('/v3/portfolio/skips?limit=20',{skips:[]}),safeJSON('/v3/portfolio/attribution',{pnl_by_mechanism_usd:{},pnl_by_strategy_usd:{}}),safeJSON('/v3/operations/mechanisms',{mechanisms:[],requirements:{}}),safeJSON('/v3/operations/action-queue',{actions:[]})",
    )
    html = _replace_once(
        html,
        "const nav=+performance.current_nav_usd, ret=+performance.total_return;$('nav').textContent=money(nav);$('totalReturn').textContent=`${ret>=0?'+':''}${pct(ret)} since $250,000 genesis`;$('totalReturn').className=`return ${pnlClass(ret)}`;$('updated').textContent=portfolio.observed_at?`Last portfolio snapshot ${new Date(portfolio.observed_at).toLocaleString()}`:'Portfolio awaiting first worker snapshot';",
        "const nav=+performance.current_nav_usd, ret=+performance.total_return;$('nav').textContent=money(nav);$('totalReturn').textContent=`${ret>=0?'+':''}${pct(ret)} since $250,000 genesis`;$('totalReturn').className=`return ${pnlClass(ret)}`;\n"
        "    const accountText=portfolio.observed_at?`Account snapshot ${new Date(portfolio.observed_at).toLocaleString()}`:'Portfolio awaiting first worker snapshot';const evidenceText=runtime.market_evidence_observed_at?` · Market evidence ${new Date(runtime.market_evidence_observed_at).toLocaleString()}`:'';$('updated').textContent=accountText+evidenceText;\n"
        "    const runtimeLabel=runtime.operational?(runtime.degraded?'Operational · degraded':'Operational'):'Attention';$('runtimeStatus').textContent=runtimeLabel;$('runtimeStatus').style.color=runtime.operational?(runtime.degraded?'#facc15':'#4ade80'):'#fb7185';\n"
        "    const valuation=runtime.valuation_status||'unavailable';const valuationLabel=valuation==='cash_only'?'Cash-only · exact':valuation.replaceAll('_',' ');$('valuationStatus').textContent=valuationLabel;$('valuationStatus').style.color=(valuation==='cash_only'||(valuation==='fresh'&&runtime.valuation_fresh))?'#4ade80':(valuation==='partial'?'#facc15':'#fb7185');\n"
        "    const familyFailures=runtime.allocation_family_failures||[];$('familyStatus').textContent=familyFailures.length?`${familyFailures.length} degraded`:'Healthy';$('familyStatus').style.color=familyFailures.length?'#facc15':'#4ade80';\n"
        "    const cycleLabel=(runtime.cycle_status||'unknown').replaceAll('_',' ');const fallback=runtime.fallback_snapshot?' · fallback accounting snapshot':'';const settlement=runtime.settlement_evidence_blocked_count?` · ${runtime.settlement_evidence_blocked_count} awaiting post-horizon settlement evidence`:'';const stale=runtime.stale_position_count?` · ${runtime.stale_position_count} stale position mark(s)`:'';$('valuationDetail').textContent=`Cycle ${cycleLabel}${fallback}${settlement}${stale}`;\n"
        "    const partial=[positions,trades,history,skips,attribution,mechanisms,queue].filter(x=>x&&x.__error).map(x=>x.__error);if(partial.length){$('error').textContent=`Partial dashboard data unavailable: ${partial.join(' · ')}`;$('error').classList.add('show')}",
    )
    html = _replace_once(
        html,
        'renderChart(history.snapshots);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderMechanisms(mechRows);renderQueue(queue.actions);',
        'renderChart(history.snapshots);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderEvidenceProgress(mechRows,mechanisms.requirements||{},mechanisms.observed_at);renderMechanisms(mechRows);renderQueue(queue.actions);',
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
