from __future__ import annotations

from inefficiency_engine import read_api_bounded_heartbeat_deploy as bounded
from inefficiency_engine import read_api_card_history_deploy as cards


_MOBILE_TRUTH_STYLE = r'''
<style id="mobile-truth-repair">
html,body{max-width:100%;overflow-x:hidden}
.wrap,.section,.hero-card,.card,.metric,.stat,.runtime-card,.item,.queue-item,.history-item{min-width:0;max-width:100%}
.item-title,.item-sub,.d,.reason,.next,.meta,.section-note{overflow-wrap:anywhere;word-break:break-word}
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
function cloneSourceConnectivity(p){try{return JSON.parse(JSON.stringify(p))}catch(_e){return p}}
function renderSourceConnectivity(payload){
  let p=payload||{};
  let rows=Array.isArray(p.sources)?p.sources:[];
  if(!p.available&&!rows.length&&lastGoodSourceConnectivity){
    const retained=cloneSourceConnectivity(lastGoodSourceConnectivity)||{};
    p={...retained,available:false,diagnostic_read_degraded:true,served_last_successful_snapshot:true,read_error_type:p.read_error_type||'DiagnosticReadUnavailable',last_successful_observed_at:retained.observed_at};
    rows=Array.isArray(p.sources)?p.sources:[];
  }
  if(!p.available&&!rows.length){
    $('sourceSummary').textContent=`Source diagnostic unavailable${p.read_error_type?` · ${p.read_error_type}`:''}`;
    if(!$('sourceProblems').children.length)$('sourceProblems').innerHTML=empty('Could not read persisted source status.');
    return;
  }
  if(p.available)lastGoodSourceConnectivity=cloneSourceConnectivity(p);
  const s=p.summary||{};
  const attention=rows.filter(x=>!['healthy','not_applicable'].includes(x.state));
  const parts=[];
  if((+s.refresh_degraded||0)>0)parts.push(`${num(s.refresh_degraded)} refresh warning${(+s.refresh_degraded||0)===1?'':'s'}`);
  parts.push(`${num(s.stale)} stale`,`${num(s.failed)} failed`,`${num(s.unobserved)} unobserved`,`${num(s.credential_required)} credential-gated`,`${num(s.awaiting_endogenous)} endogenous waiting`,`${num(s.not_applicable)} policy-disabled`);
  const readPrefix=!p.available?`Last known source status${p.last_successful_observed_at?` from ${when(p.last_successful_observed_at)}`:''} · diagnostic read unavailable${p.read_error_type?` (${p.read_error_type})`:''} · `:'';
  $('sourceSummary').textContent=`${readPrefix}${num(s.healthy)} / ${num(s.connectivity_configured??s.configured)} connected · ${parts.join(' · ')}`;
  const rank={failed:0,stale:1,unobserved:2,credential_required:3,awaiting_endogenous:4};
  attention.sort((a,b)=>(rank[a.state]??9)-(rank[b.state]??9)||String(a.name).localeCompare(String(b.name)));
  $('sourceProblems').innerHTML=attention.length?attention.map(x=>{
    const observation=x.state==='awaiting_endogenous'?'Awaiting governed activity':x.observed_at?`Last observation ${when(x.observed_at)} · ${age(x.age_seconds)} · TTL ${age(x.freshness_ttl_seconds)}`:'No persisted observation';
    const cacheNote=x.cache_expired_during_read_failure?' · cached evidence crossed TTL while diagnostic read was unavailable':'';
    return `<div class="item"><div class="item-top"><div><div class="item-title">${esc(x.name||x.source_id)}</div><div class="item-sub">${esc((x.lane_ids||[]).join(' · '))} · ${esc((x.classes||[]).join(', '))}</div></div><span class="badge ${clsStatus(x.state)}">${esc(String(x.state||'unknown').replaceAll('_',' '))}</span></div><div class="item-sub">${observation}${x.error_type?` · error ${esc(x.error_type)}`:''}${x.credential_env&&!x.credential_configured?` · configure ${esc(x.credential_env)}`:''}${cacheNote}</div></div>`;
  }).join(''):empty((+s.refresh_degraded||0)>0?'Current evidence remains valid; one or more latest refresh attempts were transiently degraded.':'All applicable provider source surfaces currently report healthy.');
}
async function refreshSourceConnectivity(){
  try{
    const r=await fetch('/v3/dashboard/source-connectivity',{cache:'no-store'});
    if(!r.ok)throw new Error(`HTTP ${r.status}`);
    renderSourceConnectivity(await r.json());
  }catch(e){
    renderSourceConnectivity({available:false,read_error_type:e?.name||'FetchError',sources:[]});
  }
}
'''

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

# The final mobile deploy owns the browser stability contract. Keep the independent
# diagnostic endpoint truthful, but do not let a transient read failure erase the
# last successfully rendered source cards from the phone UI.
cards._SOURCE_CONNECTIVITY_JS = _STABLE_SOURCE_CONNECTIVITY_JS
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
