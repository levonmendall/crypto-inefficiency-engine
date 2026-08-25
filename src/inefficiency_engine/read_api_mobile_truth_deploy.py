from __future__ import annotations

from inefficiency_engine import read_api_bounded_heartbeat_deploy as bounded
from inefficiency_engine import read_api_card_history_deploy as cards


_MOBILE_TRUTH_STYLE = r'''
<style id="mobile-truth-repair">
html,body{max-width:100%;overflow-x:hidden}
.wrap,.section,.hero-card,.card,.metric,.stat,.runtime-card,.item,.queue-item,.history-item{min-width:0;max-width:100%}
.item-title,.item-sub,.d,.reason,.next,.meta,.section-note{overflow-wrap:anywhere;word-break:break-word}
#sourceProblems.source-board{display:grid;gap:12px}
#sourceProblems .source-card{min-height:154px}
#sourceProblems .source-evidence{min-height:2.8em}
#sourceProblems .source-refresh{min-height:2.8em}
#sourceProblems .source-refresh-warning{font-weight:700}
@media(max-width:650px){
  .cardmetrics,.strip{grid-template-columns:1fr}
  .item-top,.cardhead,.section-head{flex-wrap:wrap;min-width:0}
  .badge{white-space:normal;max-width:100%;overflow-wrap:anywhere;text-align:center}
  .status-row{align-items:flex-start}
  .status-val{max-width:58%;overflow-wrap:anywhere}
}
</style>
'''

_STABLE_SOURCE_CONNECTIVITY_JS = r'''
let lastGoodSourceConnectivity=null;
let sourceConnectivityInFlight=null;
let sourceConnectivityRequestSequence=0;
let sourceConnectivityAppliedSequence=0;
const sourceCardNodes=new Map();
function cloneSourceConnectivity(p){try{return JSON.parse(JSON.stringify(p))}catch(_e){return p}}
function sourceStateLabel(state){const labels={healthy:'connected',not_applicable:'policy disabled'};return labels[state]||String(state||'unknown').replaceAll('_',' ')}
function createSourceCard(row){
  const card=document.createElement('div');card.className='item source-card';card.dataset.sourceId=String(row.source_id||'');
  const top=document.createElement('div');top.className='item-top';
  const copy=document.createElement('div');
  const title=document.createElement('div');title.className='item-title';
  const sub=document.createElement('div');sub.className='item-sub source-identity';
  copy.append(title,sub);
  const badge=document.createElement('span');badge.className='badge';
  top.append(copy,badge);
  const evidence=document.createElement('div');evidence.className='item-sub source-evidence';
  const refresh=document.createElement('div');refresh.className='item-sub source-refresh';
  card.append(top,evidence,refresh);
  return {card,title,sub,badge,evidence,refresh};
}
function ensureSourceCards(rows){
  const host=$('sourceProblems');
  if(!host.classList.contains('source-board')){host.textContent='';host.classList.add('source-board')}
  for(const row of rows){
    const sourceId=String(row.source_id||row.name||'');
    if(!sourceId||sourceCardNodes.has(sourceId))continue;
    const nodes=createSourceCard(row);sourceCardNodes.set(sourceId,nodes);host.appendChild(nodes.card);
  }
}
function patchSourceCard(row){
  const sourceId=String(row.source_id||row.name||'');
  const nodes=sourceCardNodes.get(sourceId);if(!nodes)return;
  const state=String(row.state||'unknown');
  nodes.title.textContent=String(row.name||sourceId);
  const lanes=(row.lane_ids||[]).join(' · '),classes=(row.classes||[]).join(', ');
  nodes.sub.textContent=[lanes,classes].filter(Boolean).join(' · ');
  nodes.badge.className=`badge ${clsStatus(state)}`;nodes.badge.textContent=sourceStateLabel(state);
  let evidence='No persisted observation';
  if(state==='awaiting_endogenous')evidence='Awaiting governed activity';
  else if(state==='not_applicable')evidence='Disabled by runtime provider policy';
  else if(row.observed_at)evidence=`Current evidence ${when(row.observed_at)} · ${age(row.age_seconds)} · TTL ${age(row.freshness_ttl_seconds)}`;
  if(row.credential_env&&!row.credential_configured)evidence+=` · configure ${row.credential_env}`;
  if(row.cache_expired_during_read_failure)evidence+=' · cached evidence crossed TTL while diagnostic read was unavailable';
  nodes.evidence.textContent=evidence;
  const latestState=String(row.latest_attempt_state||state||'unknown');
  const latestWhen=row.latest_attempt_observed_at?` · ${when(row.latest_attempt_observed_at)}`:'';
  const latestError=row.latest_attempt_error_type?` · ${row.latest_attempt_error_type}`:'';
  if(row.refresh_degraded){
    nodes.refresh.className='item-sub source-refresh source-refresh-warning';
    nodes.refresh.textContent=`Latest refresh warning${latestError}${latestWhen} · prior evidence remains valid`;
  }else{
    nodes.refresh.className='item-sub source-refresh';
    nodes.refresh.textContent=`Latest refresh ${sourceStateLabel(latestState)}${latestError}${latestWhen}`;
  }
}
function renderSourceConnectivity(payload){
  let p=payload||{};
  let rows=Array.isArray(p.sources)?p.sources:[];
  if(!p.available&&!rows.length&&lastGoodSourceConnectivity){
    const retained=cloneSourceConnectivity(lastGoodSourceConnectivity)||{};
    p={...retained,available:false,diagnostic_read_degraded:true,served_last_successful_snapshot:true,read_error_type:p.read_error_type||'DiagnosticReadUnavailable',last_successful_observed_at:retained.observed_at};
    rows=Array.isArray(p.sources)?p.sources:[];
  }
  if(!p.available&&!rows.length){
    $('sourceSummary').textContent=`Source diagnostic unavailable${p.read_error_type?` · ${p.read_error_type}`:''} · retaining existing source board`;
    return;
  }
  if(rows.length&&(p.available||p.served_last_successful_snapshot))lastGoodSourceConnectivity=cloneSourceConnectivity(p);
  ensureSourceCards(rows);
  for(const row of rows)patchSourceCard(row);
  const s=p.summary||{};
  const parts=[];
  if((+s.refresh_degraded||0)>0)parts.push(`${num(s.refresh_degraded)} refresh warning${(+s.refresh_degraded||0)===1?'':'s'}`);
  parts.push(`${num(s.stale)} stale`,`${num(s.failed)} failed`,`${num(s.unobserved)} unobserved`,`${num(s.credential_required)} credential-gated`,`${num(s.awaiting_endogenous)} endogenous waiting`,`${num(s.not_applicable)} policy-disabled`);
  const readPrefix=!p.available?`Last known source status${p.last_successful_observed_at?` from ${when(p.last_successful_observed_at)}`:''} · diagnostic read unavailable${p.read_error_type?` (${p.read_error_type})`:''} · `:'';
  $('sourceSummary').textContent=`${readPrefix}${num(s.healthy)} / ${num(s.connectivity_configured??s.configured)} connected · ${parts.join(' · ')}`;
}
async function refreshSourceConnectivity(){
  if(sourceConnectivityInFlight)return sourceConnectivityInFlight;
  const sequence=++sourceConnectivityRequestSequence;
  const task=(async()=>{
    try{
      const r=await fetch('/v3/dashboard/source-connectivity',{cache:'no-store'});
      if(!r.ok)throw new Error(`HTTP ${r.status}`);
      const payload=await r.json();
      if(sequence<sourceConnectivityAppliedSequence)return;
      sourceConnectivityAppliedSequence=sequence;renderSourceConnectivity(payload);
    }catch(e){
      if(sequence<sourceConnectivityAppliedSequence)return;
      sourceConnectivityAppliedSequence=sequence;
      renderSourceConnectivity({available:false,read_error_type:e?.name||'FetchError',sources:[]});
    }
  })();
  sourceConnectivityInFlight=task;
  try{return await task}finally{if(sourceConnectivityInFlight===task)sourceConnectivityInFlight=null}
}
'''

_STAGGERED_BOOT_JS = r'''$('refreshBtn').addEventListener('click',()=>{refresh().finally(()=>refreshSourceConnectivity())});window.addEventListener('resize',()=>renderChart(window.__history||[]));refresh();setTimeout(()=>refreshSourceConnectivity(),5000);setInterval(()=>{if(document.visibilityState==='visible')refresh()},30000);setTimeout(()=>setInterval(()=>{if(document.visibilityState==='visible')refreshSourceConnectivity()},30000),5000);'''

_OLD_FAMILY_LABEL = '<span class="muted">Opportunity families</span>'
_NEW_FAMILY_LABEL = '<span class="muted">Allocation family gates</span>'
_OLD_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'Healthy';"
_NEW_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'No family-level failures';"
_FORWARD_STAT_HELPER_MARKER = "function researchTimeline(c){"
_FORWARD_STAT_HELPER = "function forwardStatPct(outcomes,value){return (+outcomes||0)>0?pct(value):'—'}\n"
_FORWARD_STAT_REPLACEMENTS = (
    (
        "mean ${pct(s.mean_forward_net_return)}",
        "mean ${forwardStatPct(s.forward_outcomes,s.mean_forward_net_return)}",
    ),
    (
        "CI ${pct(s.mean_forward_net_return_ci_lower)}",
        "CI ${forwardStatPct(s.forward_outcomes,s.mean_forward_net_return_ci_lower)}",
    ),
    (
        "hit CI ${pct(s.forward_hit_rate_ci_lower)}",
        "hit CI ${forwardStatPct(s.forward_outcomes,s.forward_hit_rate_ci_lower)}",
    ),
    (
        "${metric('Forward mean',pct(c.mean_forward_net_return),'net return')}",
        "${metric('Forward mean',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return),'net return')}",
    ),
    (
        "${metric('CI lower',pct(c.mean_forward_net_return_ci_lower),'forward mean lower bound')}",
        "${metric('CI lower',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return_ci_lower),'forward mean lower bound')}",
    ),
    (
        "${metric('Hit rate',pct(c.forward_hit_rate),'forward outcomes')}",
        "${metric('Hit rate',forwardStatPct(c.forward_outcome_count,c.forward_hit_rate),'forward outcomes')}",
    ),
)

# The final mobile deploy owns the browser stability contract. Source cards are
# permanent keyed DOM nodes; status refreshes patch those nodes rather than replacing
# the problem list. Polling is serialized and staggered from the main dashboard read.
cards._SOURCE_CONNECTIVITY_JS = _STABLE_SOURCE_CONNECTIVITY_JS
cards._NEW_BOOT_JS = _STAGGERED_BOOT_JS
_original_dashboard_html = cards._dashboard_html


def repaired_dashboard_html() -> str:
    """Keep dashboard truth explicit and make dense diagnostic cards fit mobile."""

    html = _original_dashboard_html()
    html = html.replace(_OLD_FAMILY_LABEL, _NEW_FAMILY_LABEL, 1)
    html = html.replace(_OLD_FAMILY_STATE, _NEW_FAMILY_STATE, 1)
    html = html.replace(
        _FORWARD_STAT_HELPER_MARKER,
        _FORWARD_STAT_HELPER + _FORWARD_STAT_HELPER_MARKER,
        1,
    )
    for old, new in _FORWARD_STAT_REPLACEMENTS:
        html = html.replace(old, new, 1)
    html = html.replace("</head>", _MOBILE_TRUTH_STYLE + "</head>", 1)
    return html


cards._dashboard_html = repaired_dashboard_html
app = bounded.app


__all__ = ["app", "repaired_dashboard_html"]
