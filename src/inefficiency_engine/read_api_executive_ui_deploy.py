from __future__ import annotations

from inefficiency_engine import read_api_card_history_deploy as cards
from inefficiency_engine import read_api_mobile_truth_deploy as mobile


_EXECUTIVE_STYLE = r'''
<style id="executive-scorecard-ui">
.hero{grid-template-columns:1fr}.hero .hero-card:nth-child(2){display:none}
.executive-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
.executive-card{background:var(--panel2);border:1px solid var(--line);border-radius:15px;padding:14px;min-height:132px}
.executive-card.good{border-color:#1e6139;background:#092418}.executive-card.warn{border-color:#6d5d19;background:#211e0b}.executive-card.bad{border-color:#71323b;background:#241016}
.executive-status{font-size:19px;font-weight:900;margin-top:6px;letter-spacing:-.02em}.executive-value{font-size:14px;font-weight:800;margin-top:8px}.executive-detail{font-size:11px;color:var(--muted);line-height:1.45;margin-top:5px}
.effectiveness-table td{vertical-align:middle}.effectiveness-table .strategy-name{font-weight:850}.effectiveness-table .strategy-sub{font-size:10px;color:var(--muted);margin-top:3px}.effectiveness-table .status-cell{max-width:260px}
.funnel-grid{display:grid;grid-template-columns:repeat(8,minmax(96px,1fr));gap:8px}.funnel-stage{background:var(--panel2);border:1px solid var(--line);border-radius:13px;padding:11px;min-height:92px}.funnel-stage .v{font-size:18px}.funnel-stage.good{border-color:#1e6139}.funnel-stage.warn{border-color:#6d5d19}.funnel-stage.bad{border-color:#71323b}
.funnel-note{margin-top:10px;color:var(--muted);font-size:11px;line-height:1.45}
.diagnostics-drawer{background:var(--panel);border:1px solid var(--line);border-radius:20px;margin-bottom:16px;overflow:hidden}.diagnostics-drawer>summary{cursor:pointer;padding:16px 18px;font-weight:900;list-style:none;display:flex;justify-content:space-between;gap:12px;align-items:center}.diagnostics-drawer>summary::-webkit-details-marker{display:none}.diagnostics-drawer[open]>summary{border-bottom:1px solid var(--line)}#diagnosticSections{padding:14px}#diagnosticSections>.section,#diagnosticSections>.grid2{margin-bottom:12px}
.executive-section .section-note{max-width:760px}.executive-caption{color:var(--muted);font-size:11px;line-height:1.45;margin-top:8px}
@media(max-width:1050px){.executive-grid{grid-template-columns:repeat(2,1fr)}.funnel-grid{grid-template-columns:repeat(4,1fr)}}
@media(max-width:650px){.executive-grid{grid-template-columns:1fr}.funnel-grid{grid-template-columns:repeat(2,1fr)}.executive-card{min-height:0}.diagnostics-drawer>summary{padding:14px}.effectiveness-table-wrap{display:none}#strategyEffectivenessMobile{display:block}}
@media(min-width:651px){#strategyEffectivenessMobile{display:none}}
</style>
'''

_EXECUTIVE_SECTIONS = r'''
<section id="executiveScorecard" class="section executive-section"><div class="section-head"><div><div class="section-title">Executive scorecard</div><div class="section-note">The six things that matter most: system health, data quality, research progress, strategy effectiveness, portfolio performance, and execution readiness.</div></div><div id="executiveUpdated" class="section-note">Loading…</div></div><div id="executiveScoreGrid" class="executive-grid"></div></section>
<section id="strategyEffectivenessSection" class="section executive-section"><div class="section-head"><div><div class="section-title">Strategy effectiveness</div><div class="section-note">One row per mechanism. Forward evidence and realized paper outcomes matter more here than implementation detail.</div></div><div id="strategyEffectivenessSummary" class="section-note">Loading…</div></div><div id="strategyTableWrap" class="table-wrap effectiveness-table-wrap"><table class="effectiveness-table"><thead><tr><th>Strategy</th><th>Data</th><th class="num">Forward</th><th class="num">Mean net</th><th class="num">Hit rate</th><th>Confidence</th><th>Status</th></tr></thead><tbody id="strategyEffectivenessBody"></tbody></table></div><div id="strategyEffectivenessMobile" class="mobile-list"></div></section>
<section id="opportunityFunnelSection" class="section executive-section"><div class="section-head"><div><div class="section-title">Opportunity funnel</div><div class="section-note">Where the engine is converting market evidence into validated paper opportunities.</div></div><div class="section-note">Current persisted research snapshot</div></div><div id="opportunityFunnel" class="funnel-grid"></div><div id="opportunityFunnelNote" class="funnel-note"></div></section>
<details id="diagnosticsDrawer" class="diagnostics-drawer"><summary><span>Diagnostics & implementation detail</span><span class="section-note">Sources, workers, evidence internals, full mechanism cards</span></summary><div id="diagnosticSections"></div></details>
'''

_EXECUTIVE_JS = r'''
function scoreTone(state){state=String(state||'').toLowerCase();if(['healthy','ready','qualified','certified','tracking','active'].includes(state))return 'good';if(['blocked','unavailable','failed'].includes(state))return 'bad';return 'warn'}
function scoreCard(label,state,value,detail){const tone=scoreTone(state);return `<div class="executive-card ${tone}"><div class="k">${esc(label)}</div><div class="executive-status">${esc(state)}</div><div class="executive-value">${esc(value)}</div><div class="executive-detail">${esc(detail)}</div></div>`}
function finitePct(v){return Number.isFinite(+v)?pct(v):'—'}
function forwardPct(c,v){return (+c.forward_outcome_count||0)>0&&Number.isFinite(+v)?pct(v):'—'}
function strategyConfidence(c){if(c.certified)return 'Certified';if(c.paper_capable)return 'Paper-capable';if((+c.qualified_count||0)>0)return 'Qualified';if((+c.forward_outcome_count||0)>=(+c.forward_target||1))return 'Mature · not qualified';if((+c.forward_outcome_count||0)>0)return 'Building evidence';return 'No forward sample'}
function strategyConfidenceTone(c){if(c.certified||c.paper_capable||(+c.qualified_count||0)>0)return 'good';if(c.provider_status==='missing'||c.provider_status==='stale')return 'bad';return 'warn'}
function renderStrategyEffectiveness(cards){
  const rows=Array.isArray(cards)?cards:[];
  const tested=rows.filter(c=>(+c.forward_outcome_count||0)>0),qualified=rows.filter(c=>(+c.qualified_count||0)>0),paper=rows.filter(c=>c.paper_capable),certified=rows.filter(c=>c.certified);
  const best=tested.filter(c=>Number.isFinite(+c.mean_forward_net_return)).sort((a,b)=>(+b.mean_forward_net_return)-(+a.mean_forward_net_return))[0];
  $('strategyEffectivenessSummary').textContent=`${num(tested.length)} with forward evidence · ${num(qualified.length)} qualified · ${num(paper.length)} paper-capable · ${num(certified.length)} certified${best?` · best observed mean ${pct(best.mean_forward_net_return)}`:''}`;
  $('strategyEffectivenessBody').innerHTML=rows.map(c=>{const data=`${String(c.provider_status||'unknown').replaceAll('_',' ')} · ${String(c.evidence_status||'unknown').replaceAll('_',' ')}`;const blocker=c.primary_blocker&&String(c.primary_blocker)!=='No current blocker recorded.'?String(c.primary_blocker):'';return `<tr><td><div class="strategy-name">${esc(c.name||c.mechanism_id)}</div><div class="strategy-sub">${esc(String(c.mechanism_id||'').replaceAll('_',' '))}</div></td><td><span class="badge ${clsStatus(c.provider_status)}">${esc(data)}</span></td><td class="num">${num(c.forward_outcome_count)} / ${num(c.forward_target)}</td><td class="num">${esc(forwardPct(c,c.mean_forward_net_return))}</td><td class="num">${esc(forwardPct(c,c.forward_hit_rate))}</td><td><span class="badge ${strategyConfidenceTone(c)}">${esc(strategyConfidence(c))}</span></td><td class="status-cell"><span class="badge ${clsStatus(c.status)}">${esc(c.status||'collecting')}</span>${blocker?`<div class="strategy-sub">${esc(blocker)}</div>`:''}</td></tr>`}).join('');
  $('strategyEffectivenessMobile').innerHTML=rows.map(c=>`<div class="item"><div class="item-top"><div><div class="item-title">${esc(c.name||c.mechanism_id)}</div><div class="item-sub">${esc(String(c.provider_status||'unknown').replaceAll('_',' '))} · ${esc(String(c.evidence_status||'unknown').replaceAll('_',' '))}</div></div><span class="badge ${strategyConfidenceTone(c)}">${esc(strategyConfidence(c))}</span></div><div class="item-sub">Forward ${num(c.forward_outcome_count)} / ${num(c.forward_target)} · mean ${esc(forwardPct(c,c.mean_forward_net_return))} · hit ${esc(forwardPct(c,c.forward_hit_rate))}</div><div class="item-sub">${esc(c.status||'collecting')}</div></div>`).join('');
}
function dominantGate(cards){const counts=new Map();for(const c of cards||[]){const gate=String(c.dominant_rejection_gate||'').trim();if(gate)counts.set(gate,(counts.get(gate)||0)+1)}let best=null,bestCount=0;for(const [gate,count] of counts){if(count>bestCount){best=gate;bestCount=count}}return best?{gate:best,count:bestCount}:null}
function renderOpportunityFunnel(p){const s=p.summary||{},rows=p.cards||[];const forward=rows.reduce((n,c)=>n+(+c.forward_outcome_count||0),0);const stages=[['Source coverage',`${num(s.provider_connected)} / ${num(s.lane_count)}`,'mechanisms with current providers',s.provider_connected===s.lane_count?'good':'warn'],['Raw candidates',num(s.raw_candidates),'current research candidates',''],['Emitted',num(s.emitted_candidates),'passed initial funnel',''],['Signals',num(s.signals),'research signals',''],['Forward outcomes',num(forward),'matured independent outcomes',''],['Qualified',num(s.qualified_lanes),'statistical gate',s.qualified_lanes>0?'good':'warn'],['Paper-capable',num(s.paper_capable_lanes),'eligible for paper allocation',s.paper_capable_lanes>0?'good':'warn'],['Certified',num(s.certified_lanes),'profitability certification',s.certified_lanes>0?'good':'warn']];$('opportunityFunnel').innerHTML=stages.map(([k,v,d,c])=>`<div class="funnel-stage ${c}"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div><div class="d">${esc(d)}</div></div>`).join('');const gate=dominantGate(rows);$('opportunityFunnelNote').textContent=gate?`Most common published rejection gate: ${gate.gate.replaceAll('_',' ')} (${gate.count} mechanism${gate.count===1?'':'s'}).`:'No dominant candidate rejection gate is currently published.'}
function renderExecutiveDashboard(p){
  const cc=p.command_center||{},rt=cc.runtime||{},research=p.system?.research_runtime||{},portfolio=p.system?.portfolio_runtime||{},s=p.summary||{},perf=cc.performance||{},rows=p.cards||[];
  const runtimeHealthy=!!rt.operational&&!!research.current&&!!portfolio.current;const runtimeState=runtimeHealthy?'Healthy':rt.operational?'Attention':'Blocked';
  const runtimeValue=`Research ${research.current?'current':'not current'} · portfolio ${portfolio.current?'current':'not current'}`;const runtimeDetail=`Projection ${p.system?.projection_current_for_execution?'current for paper decisions':'fail-closed'}${rt.degraded?' · runtime degraded':''}`;
  const dataState=s.provider_connected===s.lane_count&&s.evidence_complete===s.lane_count?'Healthy':(+s.provider_connected||0)>0?'Attention':'Blocked';const dataValue=`${num(s.provider_connected)} / ${num(s.lane_count)} mechanisms sourced`;const dataDetail=`${num(s.evidence_complete)} / ${num(s.lane_count)} evidence-complete · source diagnostics update independently`;
  const totalForward=rows.reduce((n,c)=>n+(+c.forward_outcome_count||0),0);const researchState=(+s.signals||0)>0||totalForward>0?'Active':(+s.raw_candidates||0)>0?'Building':'Attention';const researchValue=`${num(s.signals)} signals · ${num(totalForward)} forward outcomes`;const researchDetail=`${num(s.raw_candidates)} raw → ${num(s.emitted_candidates)} emitted · ${num(s.forward_mature)} mechanisms met forward target`;
  const tested=rows.filter(c=>(+c.forward_outcome_count||0)>0);const best=tested.filter(c=>Number.isFinite(+c.mean_forward_net_return)).sort((a,b)=>(+b.mean_forward_net_return)-(+a.mean_forward_net_return))[0];const effectivenessState=(+s.certified_lanes||0)>0?'Certified':(+s.qualified_lanes||0)>0?'Qualified':tested.length?'Building evidence':'Awaiting evidence';const effectivenessValue=`${num(tested.length)} tested · ${num(s.qualified_lanes)} qualified · ${num(s.certified_lanes)} certified`;const effectivenessDetail=best?`Best observed forward mean ${pct(best.mean_forward_net_return)} · confidence gates remain unchanged`:'No mature forward return sample yet';
  const nav=Number.isFinite(+perf.current_nav_usd)?money(perf.current_nav_usd):'Awaiting NAV';const totalReturn=Number.isFinite(+perf.total_return)?pct(perf.total_return):'—';const portfolioState=Number.isFinite(+perf.current_nav_usd)?'Tracking':'Attention';const portfolioValue=`${nav} · ${totalReturn} total return`;const portfolioDetail=`Realized ${money(perf.realized_pnl_usd)} · unrealized ${money(perf.unrealized_pnl_usd)} · max drawdown ${finitePct(perf.max_drawdown_fraction)}`;
  const readinessState=(+s.paper_capable_lanes||0)>0?'Ready':(+s.qualified_lanes||0)>0?'Qualified':'Not ready';const readinessValue=`${num(s.qualified_lanes)} qualified · ${num(s.paper_capable_lanes)} paper-capable`;const readinessDetail=`${num(s.certified_lanes)} certified · paper-only · qualification and allocation remain fail-closed`;
  $('executiveScoreGrid').innerHTML=[scoreCard('System health',runtimeState,runtimeValue,runtimeDetail),scoreCard('Data quality',dataState,dataValue,dataDetail),scoreCard('Research progress',researchState,researchValue,researchDetail),scoreCard('Strategy effectiveness',effectivenessState,effectivenessValue,effectivenessDetail),scoreCard('Portfolio performance',portfolioState,portfolioValue,portfolioDetail),scoreCard('Execution readiness',readinessState,readinessValue,readinessDetail)].join('');
  $('executiveUpdated').textContent=`Updated ${when(p.generated_at||cc.portfolio?.observed_at)}`;renderStrategyEffectiveness(rows);renderOpportunityFunnel(p);
}
function updateExecutiveDataQuality(p){if(!p||!$('executiveScoreGrid'))return;const s=p.summary||{};const card=[...$('executiveScoreGrid').children][1];if(!card)return;const configured=+(s.connectivity_configured??s.configured)||0,healthy=+s.healthy||0,warnings=+s.refresh_degraded||0,failed=+s.failed||0,stale=+s.stale||0,credential=+s.credential_required||0;const state=configured>0&&healthy===configured?'Healthy':healthy>0?'Attention':'Blocked';card.className=`executive-card ${scoreTone(state)}`;card.querySelector('.executive-status').textContent=state;card.querySelector('.executive-value').textContent=`${num(healthy)} / ${num(configured)} configured sources usable`;card.querySelector('.executive-detail').textContent=`${num(warnings)} refresh warnings · ${num(failed)} failed · ${num(stale)} stale · ${num(credential)} credential-gated`;}
function installExecutiveLayout(){const host=$('diagnosticSections');if(!host||host.dataset.installed)return;host.dataset.installed='true';const ids=['runtimeGrid','sourceProblems','cycleHistoryList','summary','cards','actionQueue','volumeUniverse'];const moved=new Set();for(const id of ids){const node=$(id);if(!node)continue;const section=node.closest('section');if(section&&!moved.has(section)){host.appendChild(section);moved.add(section)}}}
'''

_HEADER_TITLE_OLD = '<div class="title">Portfolio Command Center</div>'
_HEADER_TITLE_NEW = '<div class="title">Strategy Performance Dashboard</div>'
_HEADER_SUB_OLD = '<div class="sub">Canonical paper portfolio + current mechanism truth. Portfolio, runtime, evidence, and research conclusions are read from the same persisted production snapshot.</div>'
_HEADER_SUB_NEW = '<div class="sub">Is the system working correctly, and is the strategy proving effective? The main view prioritizes health, evidence conversion, forward results, and paper portfolio performance.</div>'
_INSERT_MARKER = '<section class="section"><div class="section-head"><div><div class="section-title">Runtime health</div>'
_JS_MARKER = 'function renderRuntime(p){'
_RENDER_MARKER = 'renderRuntime(p);renderChart(history.snapshots);'
_RENDER_REPLACEMENT = 'renderRuntime(p);renderExecutiveDashboard(p);renderChart(history.snapshots);'
_SOURCE_PATCH_MARKER = 'for(const row of rows)patchSourceCard(row);'
_SOURCE_PATCH_REPLACEMENT = 'for(const row of rows)patchSourceCard(row);window.__sourceConnectivity=p;updateExecutiveDataQuality(p);'


_original_dashboard_html = cards._dashboard_html


def executive_dashboard_html() -> str:
    """Prioritize operational health and strategy effectiveness over diagnostics."""

    html = _original_dashboard_html()
    html = html.replace(_HEADER_TITLE_OLD, _HEADER_TITLE_NEW, 1)
    html = html.replace(_HEADER_SUB_OLD, _HEADER_SUB_NEW, 1)
    html = html.replace(_INSERT_MARKER, _EXECUTIVE_SECTIONS + _INSERT_MARKER, 1)
    html = html.replace(_JS_MARKER, _EXECUTIVE_JS + _JS_MARKER, 1)
    html = html.replace(_RENDER_MARKER, _RENDER_REPLACEMENT, 1)
    html = html.replace(_SOURCE_PATCH_MARKER, _SOURCE_PATCH_REPLACEMENT, 1)
    html = html.replace('</head>', _EXECUTIVE_STYLE + '</head>', 1)
    html = html.replace('</body>', '<script>installExecutiveLayout();</script></body>', 1)
    return html


cards._dashboard_html = executive_dashboard_html
app = mobile.app


__all__ = ['app', 'executive_dashboard_html']
