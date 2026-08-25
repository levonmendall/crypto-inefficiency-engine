from __future__ import annotations

from inefficiency_engine import read_api_card_history_deploy as cards
from inefficiency_engine import read_api_mobile_truth_deploy as mobile


_HISTORICAL_SECTION_MARKER = (
    '<section class="section"><div class="section-head"><div><div class="section-title">Evidence accumulation</div>'
)
_HISTORICAL_SECTION = r'''
<section class="section" id="historicalOpportunitySection"><div class="section-head"><div><div class="section-title">Historical opportunity evidence</div><div class="section-note">Recovered Aug. 21 → live observatory evidence · diagnostic only · never counted as forward qualification</div></div><div id="historicalOpportunitySummary" class="section-note">Loading historical replay…</div></div><div class="summary" id="historicalOpportunityMetrics"></div><div style="margin-top:12px"><div class="k" style="margin-bottom:8px">13-lane historical evidence coverage</div><div id="historicalLaneCoverage"></div></div><div class="grid2" style="margin-top:12px"><div><div class="k" style="margin-bottom:8px">Recovered selected candidates</div><div id="historicalSelectedCandidates"></div></div><div><div class="k" style="margin-bottom:8px">Recovered rejection funnels</div><div id="historicalFunnelRows"></div></div></div><div id="historicalOpportunityNote" class="meta" style="margin-top:10px"></div></section>
'''

_HISTORICAL_STYLE = r'''
<style id="historical-observatory-ui">
#historicalOpportunitySection .historical-row{padding:11px;border:1px solid var(--line);border-radius:12px;background:var(--panel2);margin-bottom:8px}
#historicalOpportunitySection .historical-top{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
#historicalOpportunitySection .historical-title{font-weight:850}
#historicalOpportunitySection .historical-detail{font-size:11px;color:var(--muted);margin-top:4px;line-height:1.45;overflow-wrap:anywhere}
#historicalOpportunitySection .historical-lane-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
#historicalOpportunitySection .historical-lane-grid .historical-row{margin-bottom:0}
@media(max-width:650px){#historicalOpportunitySection .historical-top{flex-wrap:wrap}#historicalOpportunitySection .historical-lane-grid{grid-template-columns:1fr}}
</style>
'''

_HISTORICAL_JS = r'''
let historicalReplayInFlight=null;
function historicalCount(p,key){return +(p?.counts?.[key]||0)}
function historicalPct(v){return Number.isFinite(+v)?pct(+v):'—'}
function historicalLaneRows(p){
  const coverage=p?.runtime?.detail?.lane_coverage||{};
  const rows=coverage?.lanes||{};
  return Object.values(rows).sort((a,b)=>String(a?.lane_name||a?.lane_id||'').localeCompare(String(b?.lane_name||b?.lane_id||'')));
}
function renderHistoricalLane(row){
  const state=String(row?.state||'unavailable');
  const badgeClass=state==='complete'?'good':state==='partial'?'warn':'bad';
  const sourceObs=+(row?.recovered_source_observations||0);
  const operating=+(row?.recovered_operating_snapshots||0);
  const funnels=+(row?.recovered_funnel_records||0);
  const classes=Array.isArray(row?.historical_evidence_classes)?row.historical_evidence_classes:[];
  const missing=Array.isArray(row?.missing_historical_evidence_classes)?row.missing_historical_evidence_classes:[];
  const metrics=[];
  metrics.push(`source observations ${num(sourceObs)}`);
  metrics.push(`operating snapshots ${num(operating)}`);
  metrics.push(`funnels ${num(funnels)}`);
  if(Number.isFinite(+row?.max_economic_candidate_count)&&+row.max_economic_candidate_count>0)metrics.push(`max candidates ${num(+row.max_economic_candidate_count)}`);
  if(Number.isFinite(+row?.max_forward_signal_count)&&+row.max_forward_signal_count>0)metrics.push(`max forward signals ${num(+row.max_forward_signal_count)}`);
  if(Number.isFinite(+row?.max_independent_forward_outcome_count)&&+row.max_independent_forward_outcome_count>0)metrics.push(`max independent outcomes ${num(+row.max_independent_forward_outcome_count)}`);
  const timing=row?.earliest_recovered_at||row?.latest_recovered_at?`history ${when(row?.earliest_recovered_at)} → ${when(row?.latest_recovered_at)}`:'no recovered historical timestamp';
  const classText=classes.length?`evidence ${classes.join(', ')}`:'no recovered evidence classes';
  const missingText=missing.length?` · missing ${missing.join(', ')}`:'';
  const reason=row?.reason?` · ${row.reason}`:'';
  return `<div class="historical-row"><div class="historical-top"><div class="historical-title">${esc(row?.lane_name||String(row?.lane_id||'').replaceAll('_',' '))}</div><span class="badge ${badgeClass}">${esc(state.toUpperCase())}</span></div><div class="historical-detail">${esc(metrics.join(' · '))}<br>${esc(timing)}<br>${esc(classText+missingText+reason)}</div></div>`;
}
function renderHistoricalCandidate(row){
  const c=row?.candidate||{};
  const identity=[c.strategy_id,c.asset,c.direction,c.venue].filter(Boolean).join(' · ')||row?.signal_id||'Recovered candidate';
  const economics=[];
  if(Number.isFinite(+c.expected_gross_return))economics.push(`gross ${pct(c.expected_gross_return)}`);
  if(Number.isFinite(+c.estimated_cost_return))economics.push(`cost ${pct(c.estimated_cost_return)}`);
  if(Number.isFinite(+c.expected_net_return))economics.push(`net ${pct(c.expected_net_return)}`);
  if(Number.isFinite(+c.expected_profit_usd))economics.push(`modeled ${money(c.expected_profit_usd)}`);
  return `<div class="historical-row"><div class="historical-top"><div class="historical-title">${esc(identity)}</div><span class="badge warn">HISTORICAL</span></div><div class="historical-detail">${esc(economics.join(' · ')||'Persisted forward-selection record')} · observed ${esc(when(row?.observed_at||c.observed_at))}</div></div>`;
}
function flattenHistoricalFunnels(p){
  const rows=[];
  for(const [kind,items] of [['alpha',p?.alpha_funnels||[]],['structural',p?.structural_funnels||[]]]){
    for(const item of items){
      for(const [name,funnel] of Object.entries(item?.funnels||{}))rows.push({kind,name,item,funnel:funnel||{}});
    }
  }
  rows.sort((a,b)=>new Date(b.item?.observed_at||0)-new Date(a.item?.observed_at||0));
  return rows;
}
function renderHistoricalFunnel(row){
  const f=row.funnel||{};
  const raw=Number.isFinite(+f.raw_candidate_count)?num(f.raw_candidate_count):'—';
  const emitted=Number.isFinite(+f.emitted_candidate_count)?num(f.emitted_candidate_count):'—';
  const parts=[`raw ${raw}`,`emitted ${emitted}`];
  if(f.dominant_rejection_gate)parts.push(`gate ${String(f.dominant_rejection_gate).replaceAll('_',' ')}`);
  if(Number.isFinite(+f.best_net_economics))parts.push(`best net ${historicalPct(f.best_net_economics)}`);
  if(Number.isFinite(+f.required_net_economics))parts.push(`hurdle ${historicalPct(f.required_net_economics)}`);
  return `<div class="historical-row"><div class="historical-top"><div class="historical-title">${esc(String(row.name||'').replaceAll('_',' '))}</div><span class="badge">${esc(row.kind)}</span></div><div class="historical-detail">${esc(parts.join(' · '))} · ${esc(when(row.item?.observed_at))}</div></div>`;
}
function renderHistoricalReplay(p){
  const selected=Array.isArray(p?.selected_candidates)?p.selected_candidates:[];
  const funnels=flattenHistoricalFunnels(p||{});
  const lanes=historicalLaneRows(p||{});
  const runtime=p?.runtime||{};
  const detail=runtime?.detail||{};
  const coverage=detail?.lane_coverage||{};
  const waiting=!!detail.waiting_for_live_observatory_boundary;
  const complete=!!p?.complete;
  const boundary=p?.live_observatory_started_at||detail.live_observatory_started_at||null;
  const completeLanes=+(coverage?.complete_lane_count||0);
  const partialLanes=+(coverage?.partial_lane_count||0);
  const unavailableLanes=+(coverage?.unavailable_lane_count||0);
  $('historicalOpportunitySummary').textContent=complete?'Backfill complete':waiting?'Backfill caught up · waiting for first live observatory record':runtime?.state?`Backfill ${String(runtime.state).replaceAll('_',' ')}`:'Historical replay status unavailable';
  $('historicalOpportunityMetrics').innerHTML=[
    stat('Historical lanes',`${completeLanes}/13 complete`,`${partialLanes} partial · ${unavailableLanes} unavailable`,completeLanes===13?'good':'warn'),
    stat('Selected candidates',num(historicalCount(p,'selected_candidate')),'exact persisted forward selections'),
    stat('Recovered funnels',num(historicalCount(p,'alpha_funnel')+historicalCount(p,'structural_funnel')),'candidate funnels only; source history shown separately'),
    stat('Replay state',complete?'Complete':waiting?'Waiting for live boundary':'Incomplete',boundary?`live boundary ${when(boundary)}`:`from ${when(p?.replay_start)}`,complete?'good':'warn')
  ].join('');
  $('historicalLaneCoverage').innerHTML=lanes.length?`<div class="historical-lane-grid">${lanes.map(renderHistoricalLane).join('')}</div>`:empty('No lane-level historical coverage has been reconstructed yet.');
  $('historicalSelectedCandidates').innerHTML=selected.length?selected.slice(0,10).map(renderHistoricalCandidate).join(''):empty('No selected alpha candidate records have been recovered from the persisted signal ledger yet.');
  $('historicalFunnelRows').innerHTML=funnels.length?funnels.slice(0,14).map(renderHistoricalFunnel).join(''):empty('No historical rejection funnels have been recovered yet.');
  const unreconstructable=p?.candidate_level_rejections_reconstructable===false;
  $('historicalOpportunityNote').textContent=unreconstructable?'Legacy rejected-candidate identities were not persisted. The app reconstructs exact lane source/operating history where available and shows aggregate funnels separately; it never invents candidate-level history. Historical replay never changes forward samples, qualification, allocation, or execution.':'Historical replay never changes forward samples, qualification, allocation, or execution.';
}
async function refreshHistoricalReplay(){
  if(historicalReplayInFlight)return historicalReplayInFlight;
  const task=(async()=>{
    try{
      const r=await fetch('/v3/research/candidate-observatory/history?limit=50',{cache:'no-store'});
      if(!r.ok)throw new Error(`HTTP ${r.status}`);
      renderHistoricalReplay(await r.json());
    }catch(e){
      $('historicalOpportunitySummary').textContent=`Historical replay unavailable · ${e?.name||'FetchError'}`;
    }
  })();
  historicalReplayInFlight=task;
  try{return await task}finally{if(historicalReplayInFlight===task)historicalReplayInFlight=null}
}
setTimeout(refreshHistoricalReplay,1500);
setInterval(()=>{if(document.visibilityState==='visible')refreshHistoricalReplay()},30000);
$('refreshBtn')?.addEventListener('click',refreshHistoricalReplay);
'''

_original_dashboard_html = cards._dashboard_html


def historical_observatory_dashboard_html() -> str:
    """Add historical observatory evidence to the existing command-center UI only."""

    html = _original_dashboard_html()
    if _HISTORICAL_SECTION_MARKER in html:
        html = html.replace(
            _HISTORICAL_SECTION_MARKER,
            _HISTORICAL_SECTION + _HISTORICAL_SECTION_MARKER,
            1,
        )
    html = html.replace("</head>", _HISTORICAL_STYLE + "</head>", 1)
    html = html.replace("</script>", _HISTORICAL_JS + "</script>", 1)
    return html


cards._dashboard_html = historical_observatory_dashboard_html
app = mobile.app


__all__ = ["app", "historical_observatory_dashboard_html"]
