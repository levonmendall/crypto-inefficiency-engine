from __future__ import annotations

from inefficiency_engine import dashboard_cards_v5 as card_model
from inefficiency_engine import dashboard_source_truth as source_truth
from inefficiency_engine import read_api_bounded_heartbeat_deploy as bounded
from inefficiency_engine import read_api_card_history_deploy as cards
from inefficiency_engine.dashboard_source_truth_stable import (
    read_current_source_truth as stable_read_current_source_truth,
)


# Source Connectivity already resolves transient acquisition failures against the
# newest still-fresh usable evidence. The mechanism cards must consume that exact
# source truth as well; otherwise two independent source resolvers disagree and card
# source counts flap whenever the newest provider attempt happens to fail.
source_truth.read_current_source_truth = stable_read_current_source_truth

_original_card_builder = card_model._card


def _stable_card_builder(row, payload, now):
    result = _original_card_builder(row, payload, now)
    lane_id = str(result.get("mechanism_id") or row.get("mechanism_id") or "")
    source = card_model._source_truth(payload, lane_id)
    covered = [str(value) for value in list(source.get("covered_evidence_classes") or [])]
    missing = [str(value) for value in list(source.get("missing_evidence_classes") or [])]
    admitted = [str(value) for value in list(source.get("admitted_source_ids") or [])]
    refresh_degraded = [
        str(value) for value in list(source.get("refresh_degraded_source_ids") or [])
    ]
    result.update(
        {
            "current_source_count": int(
                source.get("current_authoritative_source_count") or len(admitted)
            ),
            "covered_evidence_class_count": int(
                source.get("covered_evidence_class_count") or len(covered)
            ),
            "required_evidence_class_count": int(
                source.get("required_evidence_class_count") or (len(covered) + len(missing))
            ),
            "source_refresh_degraded": bool(source.get("source_refresh_degraded")),
            "refresh_degraded_source_ids": refresh_degraded,
            "latest_refresh_error_types": dict(
                source.get("latest_refresh_error_types") or {}
            ),
            # Raw item counts remain available in the API for diagnostics, but are
            # intentionally not a primary card display value. They can legitimately
            # vary between successful acquisitions and are not connectivity truth.
            "source_item_count_display_authority": False,
            "source_truth_model": source.get("source_truth_model"),
        }
    )
    return result


card_model._card = _stable_card_builder


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
#cards .mechanism-source-refresh{min-height:1.4em;margin-top:4px}
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

_STABLE_MECHANISM_CARDS_JS = r'''
const mechanismCardNodes=new Map();
const mechanismCardSignatures=new Map();
function mechanismSourceCoverage(c){
  const groups=Number.isFinite(+c.independent_source_count)?+c.independent_source_count:0;
  const covered=Array.isArray(c.covered_evidence_classes)?c.covered_evidence_classes.length:(+c.covered_evidence_class_count||0);
  const missing=Array.isArray(c.missing_evidence_classes)?c.missing_evidence_classes.length:0;
  const required=Math.max(+c.required_evidence_class_count||0,covered+missing);
  if(c.provider_status==='connected')return {value:`${num(groups)} source${groups===1?'':'s'}`,detail:`${num(covered)} / ${num(required)} evidence classes · ${(c.source_ids||[]).join(', ')||'current admitted sources'}`};
  if(c.provider_status==='stale')return {value:'stale',detail:`${num((c.stale_source_ids||[]).length)} stale configured sources · ${num(covered)} / ${num(required)} evidence classes`};
  return {value:'missing',detail:`0 admitted sources · ${num(covered)} / ${num(required)} evidence classes`};
}
function stableMechanismTimeline(c){const x=[];x.push(`Source ${c.provider_status||'unknown'} · evidence ${String(c.evidence_status||'unknown').replaceAll('_',' ')}`);if(c.research_last_at)x.push(`Research evidence ${when(c.research_last_at)}`);if(c.research_due_at)x.push(c.research_due_state==='overdue'?`Research overdue since ${when(c.research_due_at)}`:`Next research expected ${when(c.research_due_at)}`);return x.join(' · ')}
function stableMechanismCardBody(c){
  const src=mechanismSourceCoverage(c);
  const classes=(c.missing_evidence_classes||[]).length?`Missing: ${(c.missing_evidence_classes||[]).join(', ')}`:`Covered: ${(c.covered_evidence_classes||[]).join(', ')||'n/a'}`;
  return `<div class="cardhead"><div><div class="name">${esc(c.name)}</div><div class="meta">Provider ${esc(c.provider_status)} · evidence ${esc(String(c.evidence_status||'').replaceAll('_',' '))} · research ${esc(c.research_status)} · last conclusion ${esc(c.last_conclusion)}</div></div><span class="badge ${clsStatus(c.status)}">${esc(c.status)}</span></div><div class="cardmetrics">${metric('Source coverage',src.value,`${src.detail} · ${classes}`,clsStatus(c.provider_status))}${metric('Raw / emitted',candidates(c),c.dominant_rejection_gate?`dominant gate: ${String(c.dominant_rejection_gate).replaceAll('_',' ')}`:'candidate funnel not published')}${metric('Signals',num(c.signal_count),'research signals')}${metric('Forward',`${num(c.forward_outcome_count)} / ${num(c.forward_target)}`,'independent outcomes',c.forward_outcome_count>=c.forward_target?'good':'')}${metric('Qualified',num(c.qualified_count),'current statistical gate',c.qualified_count>0?'good':'')}${metric('Paper-capable',c.paper_capable?'Yes':'No',c.paper_capable?'current source + decision grade':'fail-closed',c.paper_capable?'good':'')}${metric('Settled',`${num(c.settled_count)} / ${num(c.settled_target)}`,'allocator outcomes',c.settled_count>=c.settled_target?'good':'')}${metric('Certified',c.certified?'Yes':'No','profitability certification',c.certified?'good':'')}</div><div class="mechanism-source-refresh d" data-field="source-refresh"></div><div class="strip">${metric('Forward mean',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return),'net return')}${metric('CI lower',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return_ci_lower),'forward mean lower bound')}${metric('Hit rate',forwardStatPct(c.forward_outcome_count,c.forward_hit_rate),'forward outcomes')}${metric('Realized P&L',money(c.allocator_realized_profit_usd),'paper allocator')}</div>${special(c)}<div class="reason"><strong>Current blocker:</strong> ${esc(c.primary_blocker)}</div><div class="next"><strong>Next:</strong> ${esc(c.next_action)}</div><div class="time">${esc(stableMechanismTimeline(c))}</div>${strategyDetails(c.strategy_evidence)}`;
}
function mechanismCardSignature(c){
  const rendered={
    mechanism_id:c.mechanism_id,name:c.name,status:c.status,provider_status:c.provider_status,evidence_status:c.evidence_status,research_status:c.research_status,last_conclusion:c.last_conclusion,
    independent_source_count:+c.independent_source_count||0,source_ids:[...(c.source_ids||[])].sort(),stale_source_ids:[...(c.stale_source_ids||[])].sort(),covered_evidence_classes:[...(c.covered_evidence_classes||[])].sort(),missing_evidence_classes:[...(c.missing_evidence_classes||[])].sort(),required_evidence_class_count:+c.required_evidence_class_count||0,
    raw_candidate_count:c.raw_candidate_count,emitted_candidate_count:c.emitted_candidate_count,dominant_rejection_gate:c.dominant_rejection_gate,signal_count:c.signal_count,forward_outcome_count:c.forward_outcome_count,forward_target:c.forward_target,qualified_count:c.qualified_count,paper_capable:c.paper_capable,settled_count:c.settled_count,settled_target:c.settled_target,certified:c.certified,
    mean_forward_net_return:c.mean_forward_net_return,mean_forward_net_return_ci_lower:c.mean_forward_net_return_ci_lower,forward_hit_rate:c.forward_hit_rate,allocator_realized_profit_usd:c.allocator_realized_profit_usd,best_net_economics:c.best_net_economics,required_net_economics:c.required_net_economics,gap_to_hurdle:c.gap_to_hurdle,maker_shadow:c.maker_shadow,capital_location:c.capital_location,primary_blocker:c.primary_blocker,next_action:c.next_action,research_last_at:c.research_last_at,research_due_at:c.research_due_at,research_due_state:c.research_due_state,strategy_evidence:c.strategy_evidence
  };
  return JSON.stringify(rendered);
}
function ensureMechanismCard(c){
  const id=String(c.mechanism_id||c.name||'');if(!id)return null;
  let node=mechanismCardNodes.get(id);
  if(node)return node;
  const host=$('cards');if(mechanismCardNodes.size===0)host.textContent='';
  node=document.createElement('article');node.className='card';node.dataset.mechanismId=id;host.appendChild(node);mechanismCardNodes.set(id,node);return node;
}
function patchMechanismRefresh(node,c){
  const target=node.querySelector('[data-field="source-refresh"]');if(!target)return;
  const degraded=!!c.source_refresh_degraded;
  const ids=(c.refresh_degraded_source_ids||[]).join(', ');
  const errors=c.latest_refresh_error_types||{};
  const detail=Object.entries(errors).filter(([id])=>!ids||ids.includes(id)).map(([id,error])=>`${id}: ${error}`).join(' · ');
  target.className=`mechanism-source-refresh d ${degraded?'warntext':''}`;
  target.textContent=degraded?`Latest source refresh warning${ids?` · ${ids}`:''}${detail?` · ${detail}`:''} · prior evidence remains valid`:'Source refreshes currently healthy';
}
function renderStableMechanismCards(rows){
  const cards=Array.isArray(rows)?rows:[];
  if(!cards.length&&mechanismCardNodes.size===0){$('cards').innerHTML=empty('No mechanism cards are available.');return}
  for(const c of cards){
    const node=ensureMechanismCard(c);if(!node)continue;
    const id=String(c.mechanism_id||c.name||''),signature=mechanismCardSignature(c);
    if(mechanismCardSignatures.get(id)!==signature){node.innerHTML=stableMechanismCardBody(c);mechanismCardSignatures.set(id,signature)}
    patchMechanismRefresh(node,c);
  }
}
'''

_STAGGERED_BOOT_JS = r'''$('refreshBtn').addEventListener('click',()=>{refresh().finally(()=>refreshSourceConnectivity())});window.addEventListener('resize',()=>renderChart(window.__history||[]));refresh();setTimeout(()=>refreshSourceConnectivity(),5000);setInterval(()=>{if(document.visibilityState==='visible')refresh()},30000);setTimeout(()=>setInterval(()=>{if(document.visibilityState==='visible')refreshSourceConnectivity()},30000),5000);'''

_OLD_FAMILY_LABEL = '<span class="muted">Opportunity families</span>'
_NEW_FAMILY_LABEL = '<span class="muted">Allocation family gates</span>'
_OLD_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'Healthy';"
_NEW_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'No family-level failures';"
_FORWARD_STAT_HELPER_MARKER = "function researchTimeline(c){"
_FORWARD_STAT_HELPER = "function forwardStatPct(outcomes,value){return (+outcomes||0)>0?pct(value):'—'}\n"
_MECHANISM_HELPER_MARKER = "function renderSummary(s){"
_OLD_MECHANISM_RENDER = "$('cards').innerHTML=(p.cards||[]).length?p.cards.map(renderCard).join(''):empty('No mechanism cards are available.');"
_NEW_MECHANISM_RENDER = "renderStableMechanismCards(p.cards||[]);"
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

# The final mobile deploy owns the browser stability contract. Source cards and
# mechanism cards are permanent keyed DOM nodes. Transient source attempts update a
# small warning line but do not rebuild mechanism cards or change source coverage
# while still-fresh evidence remains admitted.
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
    html = html.replace(
        _MECHANISM_HELPER_MARKER,
        _STABLE_MECHANISM_CARDS_JS + _MECHANISM_HELPER_MARKER,
        1,
    )
    html = html.replace(_OLD_MECHANISM_RENDER, _NEW_MECHANISM_RENDER, 1)
    html = html.replace("</head>", _MOBILE_TRUTH_STYLE + "</head>", 1)
    return html


cards._dashboard_html = repaired_dashboard_html
app = bounded.app


__all__ = ["app", "repaired_dashboard_html"]
