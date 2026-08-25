from __future__ import annotations

from inefficiency_engine import read_api_historical_observatory_ui_deploy as history_ui
from inefficiency_engine import read_api_card_history_deploy as cards


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
@media(max-width:850px){#cards .lane-history-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:650px){#cards .lane-history-grid{grid-template-columns:1fr}}
</style>
'''

_LANE_HISTORY_JS = r'''
let laneHistoricalReplay=null;
let laneHistoricalCardData=[];
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
  return `<div class="lane-history-row"><div class="historical-detail">${esc(row.kind)} history · ${esc(parts.join(' · ')||'persisted funnel')} · ${esc(when(row?.item?.observed_at))}</div></div>`;
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
    const funnels=[...data.funnels].sort((a,b)=>new Date(b.item?.observed_at||0)-new Date(a.item?.observed_at||0));
    const selected=[...data.selected].sort((a,b)=>new Date(b.observed_at||b?.candidate?.observed_at||0)-new Date(a.observed_at||a?.candidate?.observed_at||0));
    const rawTotal=funnels.reduce((sum,row)=>sum+(Number.isFinite(+row?.funnel?.raw_candidate_count)?+row.funnel.raw_candidate_count:0),0);
    const emittedTotal=funnels.reduce((sum,row)=>sum+(Number.isFinite(+row?.funnel?.emitted_candidate_count)?+row.funnel.emitted_candidate_count:0),0);
    const hurdleClears=funnels.filter(row=>Number.isFinite(+row?.funnel?.gap_to_hurdle)&&+row.funnel.gap_to_hurdle>0).length;
    const state=p?.complete?'complete':p?.runtime?.detail?.waiting_for_live_observatory_boundary?'caught up / waiting':'running';
    const recent=[...selected.slice(0,2).map(row=>({kind:'candidate',row})),...funnels.slice(0,2).map(row=>({kind:'funnel',row}))];
    const allRows=[...selected.map(row=>({kind:'candidate',row})),...funnels.map(row=>({kind:'funnel',row}))];
    const recentHtml=recent.map(item=>item.kind==='candidate'?laneHistoryCandidateRow(item.row):laneHistoryFunnelRow(item.row)).join('');
    const allHtml=allRows.map(item=>item.kind==='candidate'?laneHistoryCandidateRow(item.row):laneHistoryFunnelRow(item.row)).join('');
    host.innerHTML=`<div class="lane-history-head"><div><div class="lane-history-title">Historical opportunity evidence</div><div class="lane-history-summary">Aug. 21 → live boundary · diagnostic only · mapped by persisted lane/strategy identifiers</div></div><span class="badge">${esc(state.toUpperCase())}</span></div><div class="lane-history-grid"><div class="lane-history-stat"><div class="k">Historical raw</div><div class="v">${num(rawTotal)}</div></div><div class="lane-history-stat"><div class="k">Historical emitted</div><div class="v">${num(emittedTotal)}</div></div><div class="lane-history-stat"><div class="k">Forward selections</div><div class="v">${num(selected.length)}</div></div><div class="lane-history-stat"><div class="k">Hurdle-clearing snapshots</div><div class="v">${num(hurdleClears)} / ${num(funnels.length)}</div></div></div>${recentHtml||'<div class="lane-history-unassigned">No persisted historical evidence has been mapped to this lane yet.</div>'}${allRows.length>4?`<details><summary>All mapped historical evidence (${num(allRows.length)})</summary>${allHtml}</details>`:''}`;
  }
  const globalNote=document.getElementById('historicalOpportunityNote');
  if(globalNote&&indexed.unassigned.length){globalNote.textContent=`${globalNote.textContent} ${indexed.unassigned.length} historical record(s) could not be mapped to a lane from persisted identifiers and remain visible in the global history section.`}
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
    return result;
  };
}
'''

_original_dashboard_html = cards._dashboard_html


def lane_history_dashboard_html() -> str:
    """Reflect historical observatory evidence inside its canonical mechanism card.

    This is presentation-only. Historical replay remains a separate diagnostic source
    and is never merged into live forward, qualification, allocation, or execution
    state. Records without a durable lane identifier remain in the global historical
    section rather than being guessed into a card.
    """

    html = _original_dashboard_html()
    html = html.replace("</head>", _LANE_HISTORY_STYLE + "</head>", 1)
    html = html.replace("</script>", _LANE_HISTORY_JS + "</script>", 1)
    return html


cards._dashboard_html = lane_history_dashboard_html
app = history_ui.app


__all__ = ["app", "lane_history_dashboard_html"]
