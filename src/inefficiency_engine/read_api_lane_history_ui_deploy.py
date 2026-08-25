from __future__ import annotations

from fastapi import HTTPException

from inefficiency_engine import read_api_active_volume_deploy as read_plane
from inefficiency_engine import read_api_historical_observatory_ui_deploy as history_ui
from inefficiency_engine import read_api_card_history_deploy as cards
from inefficiency_engine.candidate_observatory_historical_replay import replay_start_from_env
from inefficiency_engine.durable_lane_history import read_durable_lane_history


_LANE_HISTORY_STYLE = r'''
<style id="lane-history-ui">
#cards .lane-history{margin-top:12px;padding-top:11px;border-top:1px solid var(--line)}
#cards .lane-history-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}
#cards .lane-history-title{font-size:12px;font-weight:850}
#cards .lane-history-summary{font-size:11px;color:var(--muted)}
#cards .lane-history-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:8px}
#cards .lane-history-stat{padding:8px;border:1px solid var(--line);border-radius:10px;background:var(--panel)}
#cards .lane-history-stat .v{font-size:15px}
#cards .lane-history-row{padding:8px;border:1px solid var(--line);border-radius:10px;background:var(--panel);margin-top:6px}
#cards .lane-history-row .historical-detail{font-size:11px;color:var(--muted);line-height:1.4;overflow-wrap:anywhere}
#cards .lane-history details{margin-top:8px}
#cards .lane-history details summary{font-size:11px;color:#bce9ff;font-weight:750}
#cards .lane-history-unassigned{margin-top:6px;color:var(--muted);font-size:11px}
#durableLaneHistorySummary{margin:8px 0 10px;padding:9px 10px;border:1px solid var(--line);border-radius:10px;background:var(--panel2);font-size:11px;color:var(--muted);line-height:1.45}
@media(max-width:850px){#cards .lane-history-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:650px){#cards .lane-history-grid{grid-template-columns:1fr}}
</style>
'''

_LANE_HISTORY_JS = r'''
let laneHistoricalReplay=null;
let laneHistoricalCardData=[];
let laneDurableHistory=null;
let laneDurableHistoryInFlight=null;
function laneHistoryStrategyIds(c){return new Set((c?.strategy_evidence||[]).map(s=>String(s?.strategy_id||'')).filter(Boolean))}
function laneForHistoricalCandidate(row,cards){
  const candidate=row?.candidate||{};
  const direct=String(candidate.mechanism_id||row?.mechanism_id||'');
  if(direct&&cards.some(c=>String(c.mechanism_id||'')===direct))return direct;
  const strategy=String(candidate.strategy_id||row?.strategy_id||'');
  if(strategy){for(const c of cards){if(laneHistoryStrategyIds(c).has(strategy))return String(c.mechanism_id||'')}}
  return null;
}
function laneForHistoricalFunnel(name,item,funnel,cards){
  const direct=String(funnel?.mechanism_id||item?.mechanism_id||name||'');
  if(direct&&cards.some(c=>String(c.mechanism_id||'')===direct))return direct;
  for(const c of cards){if(laneHistoryStrategyIds(c).has(String(name||'')))return String(c.mechanism_id||'')}
  return null;
}
function laneCoverageFor(p,lane){
  return p?.runtime?.detail?.lane_coverage?.lanes?.[lane]||null;
}
function durableLaneHistoryFor(lane){return laneDurableHistory?.lanes?.[lane]||null}
function laneHistoryIndex(p,cards){
  const byLane=new Map(cards.map(c=>[String(c.mechanism_id||''),{selected:[],funnels:[]}]))
  const unassigned=[];
  for(const row of (p?.selected_candidates||[])){
    const lane=laneForHistoricalCandidate(row,cards);
    if(lane&&byLane.has(lane))byLane.get(lane).selected.push(row);else unassigned.push({type:'candidate',row});
  }
  for(const [kind,items] of [['alpha',p?.alpha_funnels||[]],['structural',p?.structural_funnels||[]]]){
    for(const item of items){
      for(const [name,funnel] of Object.entries(item?.funnels||{})){
        const lane=laneForHistoricalFunnel(name,item,funnel||{},cards);
        const row={kind,name,item,funnel:funnel||{}};
        if(lane&&byLane.has(lane))byLane.get(lane).funnels.push(row);else unassigned.push({type:'funnel',row});
      }
    }
  }
  return {byLane,unassigned};
}
function laneHistoryPct(v){return Number.isFinite(+v)?pct(+v):'—'}
function laneHistoryCandidateRow(row){
  const c=row?.candidate||{};
  const parts=[];
  if(c.asset)parts.push(String(c.asset));
  if(c.direction)parts.push(String(c.direction));
  if(c.venue)parts.push(String(c.venue));
  if(Number.isFinite(+c.expected_gross_return))parts.push(`gross ${pct(c.expected_gross_return)}`);
  if(Number.isFinite(+c.estimated_cost_return))parts.push(`cost ${pct(c.estimated_cost_return)}`);
  if(Number.isFinite(+c.expected_net_return))parts.push(`net ${pct(c.expected_net_return)}`);
  if(Number.isFinite(+c.hurdle_return))parts.push(`hurdle ${pct(c.hurdle_return)}`);
  if(Number.isFinite(+c.expected_profit_usd))parts.push(`modeled ${money(c.expected_profit_usd)}`);
  return `<div class="lane-history-row"><div class="historical-detail">Selected ${esc(c.strategy_id||'candidate')} · ${esc(parts.join(' · ')||'persisted forward selection')} · ${esc(when(row?.observed_at||c.observed_at))}</div></div>`;
}
function laneHistoryFunnelRow(row){
  const f=row?.funnel||{};
  const parts=[];
  if(Number.isFinite(+f.raw_candidate_count))parts.push(`raw ${num(f.raw_candidate_count)}`);
  if(Number.isFinite(+f.emitted_candidate_count))parts.push(`emitted ${num(f.emitted_candidate_count)}`);
  if(f.dominant_rejection_gate)parts.push(`gate ${String(f.dominant_rejection_gate).replaceAll('_',' ')}`);
  if(Number.isFinite(+f.best_gross_economics))parts.push(`best gross ${laneHistoryPct(f.best_gross_economics)}`);
  if(Number.isFinite(+f.best_cost_economics))parts.push(`cost ${laneHistoryPct(f.best_cost_economics)}`);
  if(Number.isFinite(+f.best_net_economics))parts.push(`best net ${laneHistoryPct(f.best_net_economics)}`);
  if(Number.isFinite(+f.required_net_economics))parts.push(`hurdle ${laneHistoryPct(f.required_net_economics)}`);
  if(Number.isFinite(+f.gap_to_hurdle))parts.push(`gap ${laneHistoryPct(f.gap_to_hurdle)}`);
  if(f.economics_unit)parts.push(String(f.economics_unit).replaceAll('_',' '));
  return `<div class="lane-history-row"><div class="historical-detail">${esc(row.kind)} candidate-funnel history · ${esc(parts.join(' · ')||'persisted funnel')} · ${esc(when(row?.item?.observed_at))}</div></div>`;
}
function laneHistoryCoverageRow(coverage){
  if(!coverage)return '';
  const classes=Array.isArray(coverage.historical_evidence_classes)?coverage.historical_evidence_classes:[];
  const missing=Array.isArray(coverage.missing_historical_evidence_classes)?coverage.missing_historical_evidence_classes:[];
  const parts=[
    `source observations ${num(+(coverage.recovered_source_observations||0))}`,
    `operating snapshots ${num(+(coverage.recovered_operating_snapshots||0))}`,
    `candidate funnels ${num(+(coverage.recovered_funnel_records||0))}`
  ];
  if(Number.isFinite(+coverage.max_economic_candidate_count))parts.push(`max candidates ${num(+coverage.max_economic_candidate_count)}`);
  if(Number.isFinite(+coverage.max_forward_signal_count))parts.push(`max forward signals ${num(+coverage.max_forward_signal_count)}`);
  if(Number.isFinite(+coverage.max_independent_forward_outcome_count))parts.push(`max independent outcomes ${num(+coverage.max_independent_forward_outcome_count)}`);
  const timing=(coverage.earliest_recovered_at||coverage.latest_recovered_at)?`history ${when(coverage.earliest_recovered_at)} → ${when(coverage.latest_recovered_at)}`:'no recovered pre-live timestamps';
  const evidence=classes.length?`evidence ${classes.join(', ')}`:'no recovered pre-live evidence classes';
  const missingText=missing.length?` · missing ${missing.join(', ')}`:'';
  const reason=coverage.reason?` · ${coverage.reason}`:'';
  return `<div class="lane-history-row"><div class="historical-detail"><strong>Strict pre-live backfill</strong> · ${esc(parts.join(' · '))}<br>${esc(timing)}<br>${esc(evidence+missingText+reason)}</div></div>`;
}
function laneDurableHistoryRow(durable){
  if(!durable)return '';
  const classes=Array.isArray(durable.historical_evidence_classes)?durable.historical_evidence_classes:[];
  const missing=Array.isArray(durable.missing_historical_evidence_classes)?durable.missing_historical_evidence_classes:[];
  const sources=Array.isArray(durable.source_ids)?durable.source_ids:[];
  const ledgers=Array.isArray(durable.source_ledgers)?durable.source_ledgers:[];
  const recovered=+(durable.recovered_evidence_class_count||0),required=+(durable.required_evidence_class_count||0);
  const timing=(durable.earliest_recovered_at||durable.latest_recovered_at)?`history ${when(durable.earliest_recovered_at)} → ${when(durable.latest_recovered_at)}`:'no durable history timestamp';
  const evidence=classes.length?`evidence ${classes.join(', ')}`:'no durable evidence classes recovered';
  const missingText=missing.length?` · missing ${missing.join(', ')}`:'';
  const sourceText=sources.length?`sources ${sources.join(', ')}`:'no durable source ids';
  const ledgerText=ledgers.length?` · ledgers ${ledgers.join(', ')}`:'';
  return `<div class="lane-history-row"><div class="historical-detail"><strong>Total durable history since Aug. 21</strong> · evidence classes ${num(recovered)}/${num(required)} · source records ${num(+(durable.recovered_source_observations||0))} · operating snapshots ${num(+(durable.recovered_operating_snapshots||0))}<br>${esc(timing)}<br>${esc(evidence+missingText)}<br>${esc(sourceText+ledgerText)}<br>Post-boundary history is visible here but does not retroactively certify strict pre-live coverage.</div></div>`;
}
function renderGlobalDurableLaneHistory(){
  const section=document.getElementById('historicalOpportunitySection');
  const coverageHost=document.getElementById('historicalLaneCoverage');
  if(!section||!coverageHost||!laneDurableHistory)return;
  const label=coverageHost.previousElementSibling;
  if(label&&label.classList.contains('k'))label.textContent='13-lane strict pre-live backfill certification';
  let box=document.getElementById('durableLaneHistorySummary');
  if(!box){box=document.createElement('div');box.id='durableLaneHistorySummary';coverageHost.parentElement.insertBefore(box,coverageHost)}
  const withHistory=+(laneDurableHistory.lanes_with_durable_history||0);
  const full=+(laneDurableHistory.lanes_with_all_required_evidence_classes||0);
  const without=+(laneDurableHistory.lanes_without_durable_history||0);
  box.innerHTML=`<strong>Total durable lane history</strong> · ${num(withHistory)}/13 lanes have trustworthy persisted history since Aug. 21 · ${num(full)} have seen every required evidence class · ${num(without)} have no recoverable durable history. This is separate from the frozen pre-live certification window.`;
}
function laneHistoryCardNode(lane){return Array.from(document.querySelectorAll('#cards .card[data-mechanism-id]')).find(node=>String(node.dataset.mechanismId||'')===lane)||null}
function renderLaneHistoricalEvidence(cards,p){
  if(!Array.isArray(cards)||!cards.length||!p)return;
  const indexed=laneHistoryIndex(p,cards);
  for(const c of cards){
    const lane=String(c.mechanism_id||'');
    const node=laneHistoryCardNode(lane);
    if(!node)continue;
    let host=node.querySelector('.lane-history');
    if(!host){host=document.createElement('div');host.className='lane-history';node.appendChild(host)}
    const data=indexed.byLane.get(lane)||{selected:[],funnels:[]};
    const coverage=laneCoverageFor(p,lane);
    const durable=durableLaneHistoryFor(lane);
    const funnels=[...data.funnels].sort((a,b)=>new Date(b.item?.observed_at||0)-new Date(a.item?.observed_at||0));
    const selected=[...data.selected].sort((a,b)=>new Date(b.observed_at||b?.candidate?.observed_at||0)-new Date(a.observed_at||a?.candidate?.observed_at||0));
    const rawTotal=funnels.reduce((sum,row)=>sum+(Number.isFinite(+row?.funnel?.raw_candidate_count)?+row.funnel.raw_candidate_count:0),0);
    const state=String(coverage?.state||'unavailable');
    const coverageHtml=laneHistoryCoverageRow(coverage);
    const durableHtml=laneDurableHistoryRow(durable);
    const recent=[...selected.slice(0,2).map(row=>({kind:'candidate',row})),...funnels.slice(0,2).map(row=>({kind:'funnel',row}))];
    const allRows=[...selected.map(row=>({kind:'candidate',row})),...funnels.map(row=>({kind:'funnel',row}))];
    const recentHtml=recent.map(item=>item.kind==='candidate'?laneHistoryCandidateRow(item.row):laneHistoryFunnelRow(item.row)).join('');
    const allHtml=allRows.map(item=>item.kind==='candidate'?laneHistoryCandidateRow(item.row):laneHistoryFunnelRow(item.row)).join('');
    const durableRecovered=+(durable?.recovered_evidence_class_count||0);
    const durableRequired=+(durable?.required_evidence_class_count||0);
    const durableSourceRecords=+(durable?.recovered_source_observations||0);
    const hasDurable=!!durable?.history_available;
    const sourceObs=+(coverage?.recovered_source_observations||0);
    const operating=+(coverage?.recovered_operating_snapshots||0);
    const hasRecoveredCoverage=!!coverage&&(sourceObs>0||operating>0||+(coverage?.recovered_funnel_records||0)>0||coverage?.earliest_recovered_at||coverage?.latest_recovered_at);
    const emptyMessage=hasDurable?'No candidate-level historical selections or funnels were persisted for this lane. Trustworthy durable source/operating history is shown separately above.':hasRecoveredCoverage?'No candidate-level historical selections or funnels were persisted for this lane; strict pre-live source/operating history is shown above.':'No trustworthy persisted lane history has been recovered yet.';
    host.innerHTML=`<div class="lane-history-head"><div><div class="lane-history-title">History & backfill evidence</div><div class="lane-history-summary">Strict pre-live backfill and total durable history are separate · diagnostic only</div></div><span class="badge">PRE-LIVE ${esc(state.toUpperCase())}</span></div><div class="lane-history-grid"><div class="lane-history-stat"><div class="k">Durable evidence classes</div><div class="v">${num(durableRecovered)}/${num(durableRequired)}</div></div><div class="lane-history-stat"><div class="k">Durable source records</div><div class="v">${num(durableSourceRecords)}</div></div><div class="lane-history-stat"><div class="k">Historical candidate raw</div><div class="v">${num(rawTotal)}</div></div><div class="lane-history-stat"><div class="k">Forward selections</div><div class="v">${num(selected.length)}</div></div></div>${durableHtml}${coverageHtml}${recentHtml||`<div class="lane-history-unassigned">${esc(emptyMessage)}</div>`}${allRows.length>4?`<details><summary>All mapped historical candidate evidence (${num(allRows.length)})</summary>${allHtml}</details>`:''}`;
  }
  renderGlobalDurableLaneHistory();
  const globalNote=document.getElementById('historicalOpportunityNote');
  if(globalNote&&indexed.unassigned.length){globalNote.textContent=`${globalNote.textContent} ${indexed.unassigned.length} historical record(s) could not be mapped to a lane from persisted identifiers and remain visible in the global history section.`}
}
async function refreshLaneDurableHistory(){
  if(laneDurableHistoryInFlight)return laneDurableHistoryInFlight;
  const task=(async()=>{
    try{
      const r=await fetch('/v3/dashboard/durable-lane-history',{cache:'no-store'});
      if(!r.ok)throw new Error(`HTTP ${r.status}`);
      laneDurableHistory=await r.json();
      renderLaneHistoricalEvidence(laneHistoricalCardData,laneHistoricalReplay||{});
      renderGlobalDurableLaneHistory();
    }catch(_e){laneDurableHistory=null}
  })();
  laneDurableHistoryInFlight=task.finally(()=>{laneDurableHistoryInFlight=null});
  return laneDurableHistoryInFlight;
}
const _laneHistoryOriginalRenderStableMechanismCards=window.renderStableMechanismCards;
if(typeof _laneHistoryOriginalRenderStableMechanismCards==='function'){
  window.renderStableMechanismCards=function(rows){
    laneHistoricalCardData=Array.isArray(rows)?rows:[];
    const result=_laneHistoryOriginalRenderStableMechanismCards(rows);
    if(laneHistoricalReplay)renderLaneHistoricalEvidence(laneHistoricalCardData,laneHistoricalReplay);
    return result;
  };
}
const _laneHistoryOriginalRenderHistoricalReplay=window.renderHistoricalReplay;
if(typeof _laneHistoryOriginalRenderHistoricalReplay==='function'){
  window.renderHistoricalReplay=function(p){
    laneHistoricalReplay=p||{};
    const result=_laneHistoryOriginalRenderHistoricalReplay(p);
    renderLaneHistoricalEvidence(laneHistoricalCardData,laneHistoricalReplay);
    renderGlobalDurableLaneHistory();
    return result;
  };
}
refreshLaneDurableHistory();
setInterval(refreshLaneDurableHistory,300000);
'''

_original_dashboard_html = cards._dashboard_html
app = history_ui.app


@app.get("/v3/dashboard/durable-lane-history")
def durable_lane_history():
    """Expose total trustworthy lane history without changing pre-live certification."""

    store = read_plane._store()  # noqa: SLF001 - deployment read-plane composition
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_durable_lane_history(store, start=replay_start_from_env())
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "durable lane history is temporarily unavailable",
                "error_type": type(exc).__name__,
            },
        ) from exc


def lane_history_dashboard_html() -> str:
    """Reflect strict pre-live and total durable history inside each mechanism card.

    Post-boundary durable history is visible but never retroactively certifies the
    pre-live backfill. Candidate-level history remains distinct from source evidence.
    Historical reads never change forward samples, qualification, allocation, or execution.
    """

    html = _original_dashboard_html()
    html = html.replace(
        "/v3/research/candidate-observatory/history?limit=50",
        "/v3/research/candidate-observatory/history?limit=500",
        1,
    )
    html = html.replace("</head>", _LANE_HISTORY_STYLE + "</head>", 1)
    html = html.replace("</script>", _LANE_HISTORY_JS + "</script>", 1)
    return html


cards._dashboard_html = lane_history_dashboard_html


__all__ = ["app", "durable_lane_history", "lane_history_dashboard_html"]
