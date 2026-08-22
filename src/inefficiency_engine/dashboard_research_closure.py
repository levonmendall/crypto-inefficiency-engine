from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from inefficiency_engine.dashboard_resilience import RESILIENT_DASHBOARD_HTML


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError("research closure dashboard overlay target changed unexpectedly")
    return source.replace(old, new, 1)


def _build_research_closure_dashboard_html() -> str:
    html = RESILIENT_DASHBOARD_HTML
    html = _replace_once(
        html,
        "</style>",
        """    .evidence-diagnostic{margin-top:7px;padding:8px 9px;border:1px solid var(--line);border-radius:9px;background:#071019;color:var(--muted);font-size:10px;line-height:1.35}.evidence-diagnostic strong{color:var(--text)}
  </style>""",
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
        "const [portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue,cycleHistory]=await Promise.all([",
        "const [dashboardMeta,portfolio,performance,runtime,positions,trades,history,skips,attribution,mechanisms,queue,cycleHistory]=await Promise.all([dashboardSnapshot,",
    )
    html = _replace_once(
        html,
        "function renderEvidenceProgress(rows,requirements,observedAt){",
        """function sourcePhase(r){
  const stage=String(r?.stage||''),prefix='waiting_for_source:',sourceWait=stage.startsWith(prefix)?stage.slice(prefix.length):'';
  if(sourceWait==='provider_gap')return 'Provider missing';
  if(sourceWait==='stale')return 'Provider stale';
  if(sourceWait==='evidence_class_gap'||stage==='research_active_waiting_for_complete_forward_evidence')return 'Evidence incomplete';
  if(sourceWait==='redundancy_gap'||stage==='forward_learning_active_redundancy_pending'||stage==='provisional_forward_positive')return 'Redundancy pending';
  const funnel=r?.rejection_funnel||{},raw=+funnel.raw_candidate_count||0,emitted=+funnel.emitted_candidate_count||0,gate=String(funnel.dominant_rejection_gate||'').toLowerCase();
  if(funnel&&Object.keys(funnel).length&&raw===0)return gate.includes('history')?'Awaiting history':'No raw signal';
  if(raw>0&&emitted===0){if(gate.includes('history'))return 'Awaiting history';if(gate.includes('source'))return 'Evidence incomplete';if(gate.includes('cost')||gate.includes('net_return')||gate.includes('hurdle'))return 'Economics rejected';return 'Signal rejected'}
  if((+r?.forward_signal_count||0)>0&&(+r?.independent_forward_outcome_count||0)===0)return 'Awaiting forward outcomes';
  if(r?.state==='certified')return 'Certified';if(r?.state==='certifying')return 'Certifying';return 'Collecting evidence';
}
function providerCardValue(r){const status=String(r?.card_truth?.provider_status||'');if(status==='connected')return 'Connected';if(status==='stale')return 'Stale';if(status==='missing')return 'Missing';return r?.provider_ready?'Connected':'Missing'}
function forwardMeanLabel(r){const outcomes=+r?.independent_forward_outcome_count||0;if(outcomes<=0)return 'Awaiting outcomes';return Number.isFinite(+r?.mean_forward_net_return)?pct(r.mean_forward_net_return):'Outcome metric pending'}
function renderEvidenceProgress(rows,requirements,observedAt,laneTruth){""",
    )
    html = _replace_once(
        html,
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,promoted=rows.filter(r=>+r.current_promoted_count>0).length,certified=rows.filter(r=>r.state==='certified').length;",
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,certified=rows.filter(r=>r.state==='certified').length,laneCount=Math.max(1,+laneTruth?.lane_count||rows.length||13),paperCapableIds=new Set(laneTruth?.paper_execution_capable_lanes||[]),paperCapableCount=laneTruth?.available?(+laneTruth.paper_execution_capable_count||0):null;",
    )
    html = _replace_once(
        html,
        "stat('Qualified now',num(qualified)),stat('Executable now',num(promoted)),stat('Certified',`${certified} / ${rows.length}`)",
        "stat('Qualified now',num(qualified)),stat('Paper-capable',paperCapableCount===null?'—':`${num(paperCapableCount)} / ${num(laneCount)}`),stat('Certified',`${certified} / ${rows.length}`)",
    )
    html = _replace_once(
        html,
        "    const stage=order[r.stage]??0,obs=+r.authoritative_observation_count||0,signals=+r.forward_signal_count||0,outcomes=+r.independent_forward_outcome_count||0,q=+r.current_statistically_qualified_count||0,p=+r.current_promoted_count||0,settled=+r.settled_allocator_outcome_count||0;",
        "    const stage=order[r.stage]??0,obs=+r.authoritative_observation_count||0,signals=+r.forward_signal_count||0,outcomes=+r.independent_forward_outcome_count||0,q=+r.current_statistically_qualified_count||0,p=+r.current_promoted_count||0,settled=+r.settled_allocator_outcome_count||0,paperReady=paperCapableIds.has(r.mechanism_id),projectionCurrent=laneTruth?.projection_current_for_execution===true,phase=sourcePhase(r);",
    )
    html = _replace_once(
        html,
        "${evidenceStep('Provider',r.provider_ready?'Ready':'Gap',`${obs} authoritative`,providerCls)}",
        "${evidenceStep('Provider',providerCardValue(r),`${phase} · ${obs} current authoritative`,providerCls)}",
    )
    html = _replace_once(
        html,
        "${evidenceStep('Executable',stage>=4?num(p):'—','current L2 / cost / capacity',executionCls)}",
        "${evidenceStep('Paper-capable',laneTruth?.available?(paperReady?'Yes':'No'):'—',!projectionCurrent?'stale or unavailable projection · fail closed':(paperReady?'decision-grade + source-connected':'not decision-grade/currently executable'),paperReady&&projectionCurrent?'done':(projectionCurrent?'active':'blocked'))}",
    )
    html = _replace_once(
        html,
        "const worker=r.forward_evidence_worker_healthy===true?'worker healthy':(r.forward_evidence_worker_healthy===false?'worker attention':'worker n/a'),persist=r.forward_evidence_persistence_healthy===true?'persistence healthy':(r.forward_evidence_persistence_healthy===false?'persistence attention':'persistence n/a');",
        """const workerState=r.forward_evidence_worker_state||'unknown';const workerLabels={healthy_current:'worker current',waiting_scheduled:'worker scheduled',late:'worker late',stalled:'worker stalled',failed:'worker failed',unknown:'worker n/a'};const worker=workerLabels[workerState]||workerState.replaceAll('_',' '),persist=r.forward_evidence_persistence_healthy===true?'persistence healthy':(r.forward_evidence_persistence_healthy===false?'persistence attention':'persistence n/a');""",
    )
    html = _replace_once(
        html,
        "const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;",
        """const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;let diagnostic='';if(r.rejection_funnel){const f=r.rejection_funnel,unit=f.economics_unit==='horizon_return'?'horizon return':'annualized return';diagnostic=`<div class=\"evidence-diagnostic\"><strong>Rejection funnel</strong> · ${num(f.raw_candidate_count||0)} raw candidates · ${num(f.emitted_candidate_count||0)} emitted · gate ${esc((f.dominant_rejection_gate||'unknown').replaceAll('_',' '))}${Number.isFinite(+f.best_net_economics)?` · best net ${pct(f.best_net_economics)} ${unit}`:''}${Number.isFinite(+f.required_net_economics)?` vs ${pct(f.required_net_economics)} required`:''}</div>`}else if(r.mechanism_id==='liquidity_provision'&&Number.isFinite(+r.maker_shadow_outcome_count)){diagnostic=`<div class=\"evidence-diagnostic\"><strong>Maker shadow</strong> · ${num(r.maker_shadow_outcome_count)} matured · ${num(r.maker_crossed_through_count||0)} crossed-through · ${num(r.maker_queue_fill_confirmed_count||0)} queue-confirmed fills. Public aggregated L2 does not prove queue priority.</div>`}else if(r.mechanism_id==='capital_location_settlement'&&Number.isFinite(+r.capital_location_mean_incremental_option_value)){diagnostic=`<div class=\"evidence-diagnostic\"><strong>Location forward test</strong> · mean incremental option value ${pct(r.capital_location_mean_incremental_option_value)} · transfer evidence remains fail-closed.</div>`}else if(r.provider_ready===false&&r.legacy_provider_admission){const p=r.legacy_provider_admission;diagnostic=`<div class=\"evidence-diagnostic\"><strong>Legacy provider probes</strong> · ${num(p.admitted_provider_count||0)} fresh admitted · diagnostic only; canonical 13-lane source state controls this card.</div>`}""",
    )
    html = _replace_once(
        html,
        "<div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div><div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
        "<div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div>${diagnostic}<div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
    )
    html = _replace_once(
        html,
        "renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class=\"muted\">${esc(r.mechanism_id)}</span></td><td>${state(r)}</td><td class=\"num\">${num(r.independent_forward_outcome_count)}</td><td class=\"num\">${num(r.settled_allocator_outcome_count)}</td><td class=\"num ${pnlClass(r.mean_forward_net_return)}\">${pct(r.mean_forward_net_return)}</td><td><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan=\"6\" class=\"muted\">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class=\"item\"><div class=\"item-top\"><div class=\"item-title\">${esc(r.name)}</div>${state(r)}</div><div class=\"item-sub\">${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}",
        """const sourceDimensions=r=>{const stage=String(r.stage||''),prefix='waiting_for_source:',sourceWait=stage.startsWith(prefix)?stage.slice(prefix.length):'';const provider=r.provider_ready===true?'provider connected':(sourceWait==='stale'?'provider stale':sourceWait==='provider_gap'?'provider missing':'provider unavailable');return `${provider} · ${sourcePhase(r).toLowerCase()}`};
renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`,dimensions=r=>`<div class=\"muted\" style=\"font-size:10px;margin-top:4px\">${esc(sourceDimensions(r))}</div>`,mean=r=>forwardMeanLabel(r);$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class=\"muted\">${esc(r.mechanism_id)}</span></td><td>${state(r)}${dimensions(r)}</td><td class=\"num\">${num(r.independent_forward_outcome_count)}</td><td class=\"num\">${num(r.settled_allocator_outcome_count)}</td><td class=\"num ${pnlClass(r.mean_forward_net_return)}\">${esc(mean(r))}</td><td><div><strong>${esc(sourcePhase(r))}</strong></div><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan=\"6\" class=\"muted\">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class=\"item\"><div class=\"item-top\"><div class=\"item-title\">${esc(r.name)}</div>${state(r)}</div>${dimensions(r)}<div class=\"item-sub\"><strong>${esc(sourcePhase(r))}</strong> · ${esc(r.primary_reason)}</div><div class=\"item-sub\">Forward mean: ${esc(mean(r))}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}""",
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
  const staleReasons=[];if(snapshot?.__stale)staleReasons.push('cached snapshot');if(snapshot?.research_projection_stale)staleReasons.push(snapshot?.research_projection_freshness?.reason||'research projection stale');if(snapshot?.operating_projection_stale)staleReasons.push(snapshot?.operating_projection_freshness?.reason||'operating projection stale');
  const current=available&&projectionCurrent&&!snapshot?.__stale&&!staleReasons.length,release=snapshot?.release_commit?` · ${String(snapshot.release_commit).slice(0,7)}`:'';
  set('cardTruthStatus',current?`Current${release}`:(available?`Stale / fail-closed${release}`:'Unavailable'),current?'#4ade80':(available?'#facc15':'#fb7185'));
  const notice=$('notice');if(staleReasons.length){notice.textContent=`Card data is not current: ${staleReasons.join(' · ')}. Paper-capable status remains fail-closed until audited projections are current.`;notice.classList.add('show')}else if(notice?.textContent?.startsWith('Card data is not current:')){notice.textContent='';notice.classList.remove('show')}
}
async function refresh(){""",
    )
    html = _replace_once(
        html,
        "renderChart(history.snapshots);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderCycleHistory(cycleHistory);renderEvidenceProgress(mechRows,mechanisms.requirements||{},mechanisms.observed_at);renderMechanisms(mechRows);renderQueue(queue.actions);",
        "window.__dashboardHistory=history.snapshots||[];window.__laneTruth=dashboardMeta.lane_executability||{};renderChart(window.__dashboardHistory);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderCycleHistory(cycleHistory);renderEvidenceProgress(mechRows,mechanisms.requirements||{},mechanisms.observed_at,window.__laneTruth);renderMechanisms(mechRows);renderQueue(queue.actions);renderDashboardTruth(dashboardMeta);",
    )
    html = _replace_once(
        html,
        "$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>refresh());refresh();setInterval(refresh,30000);",
        "$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>renderChart(window.__dashboardHistory||[]));refresh();setInterval(refresh,30000);",
    )
    return html


RESEARCH_CLOSURE_DASHBOARD_HTML = _build_research_closure_dashboard_html()


def build_research_closure_dashboard_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", include_in_schema=False, response_class=HTMLResponse)
    def dashboard_root() -> HTMLResponse:
        return HTMLResponse(RESEARCH_CLOSURE_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    @router.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
    def portfolio_dashboard() -> HTMLResponse:
        return HTMLResponse(RESEARCH_CLOSURE_DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    return router
