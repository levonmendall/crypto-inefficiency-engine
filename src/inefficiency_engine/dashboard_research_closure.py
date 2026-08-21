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
        "const worker=r.forward_evidence_worker_healthy===true?'worker healthy':(r.forward_evidence_worker_healthy===false?'worker attention':'worker n/a'),persist=r.forward_evidence_persistence_healthy===true?'persistence healthy':(r.forward_evidence_persistence_healthy===false?'persistence attention':'persistence n/a');",
        """const workerState=r.forward_evidence_worker_state||'unknown';const workerLabels={healthy_current:'worker current',waiting_scheduled:'worker scheduled',late:'worker late',stalled:'worker stalled',failed:'worker failed',unknown:'worker n/a'};const worker=workerLabels[workerState]||workerState.replaceAll('_',' '),persist=r.forward_evidence_persistence_healthy===true?'persistence healthy':(r.forward_evidence_persistence_healthy===false?'persistence attention':'persistence n/a');""",
    )
    html = _replace_once(
        html,
        "const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;",
        """const last=r.forward_evidence_last_outcome_at||r.forward_evidence_last_signal_at||r.forward_evidence_last_cycle_at,next=r.forward_evidence_next_expected_at;let diagnostic='';if(r.rejection_funnel){const f=r.rejection_funnel,unit=f.economics_unit==='horizon_return'?'horizon return':'annualized return';diagnostic=`<div class=\"evidence-diagnostic\"><strong>Rejection funnel</strong> · ${num(f.raw_candidate_count||0)} raw candidates · ${num(f.emitted_candidate_count||0)} emitted · gate ${esc((f.dominant_rejection_gate||'unknown').replaceAll('_',' '))}${Number.isFinite(+f.best_net_economics)?` · best net ${pct(f.best_net_economics)} ${unit}`:''}${Number.isFinite(+f.required_net_economics)?` vs ${pct(f.required_net_economics)} required`:''}</div>`}else if(r.mechanism_id==='liquidity_provision'&&Number.isFinite(+r.maker_shadow_outcome_count)){diagnostic=`<div class=\"evidence-diagnostic\"><strong>Maker shadow</strong> · ${num(r.maker_shadow_outcome_count)} matured · ${num(r.maker_crossed_through_count||0)} crossed-through · ${num(r.maker_queue_fill_confirmed_count||0)} queue-confirmed fills. Public aggregated L2 does not prove queue priority.</div>`}else if(r.mechanism_id==='capital_location_settlement'&&Number.isFinite(+r.capital_location_mean_incremental_option_value)){diagnostic=`<div class=\"evidence-diagnostic\"><strong>Location forward test</strong> · mean incremental option value ${pct(r.capital_location_mean_incremental_option_value)} · transfer evidence remains fail-closed.</div>`}else if(r.provider_admission&&!r.provider_admission.authoritative_provider_connected){diagnostic=`<div class=\"evidence-diagnostic\"><strong>Provider admission ready</strong> · ${esc(r.provider_admission.observation_contract||'observation contract')} · authoritative/commercial/point-in-time evidence required before activation.</div>`}""",
    )
    html = _replace_once(
        html,
        "<div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div><div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
        "<div class=\"evidence-reason\">${esc(r.primary_reason||'No current reason recorded')}</div>${diagnostic}<div class=\"evidence-next\">Next: ${esc(r.next_action||'Continue evidence collection')}</div><div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
    )
    html = _replace_once(
        html,
        "renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class=\"muted\">${esc(r.mechanism_id)}</span></td><td>${state(r)}</td><td class=\"num\">${num(r.independent_forward_outcome_count)}</td><td class=\"num\">${num(r.settled_allocator_outcome_count)}</td><td class=\"num ${pnlClass(r.mean_forward_net_return)}\">${pct(r.mean_forward_net_return)}</td><td><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan=\"6\" class=\"muted\">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class=\"item\"><div class=\"item-top\"><div class=\"item-title\">${esc(r.name)}</div>${state(r)}</div><div class=\"item-sub\">${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}",
        """const sourceDimensions=r=>{const stage=String(r.stage||''),prefix='waiting_for_source:',sourceWait=stage.startsWith(prefix)?stage.slice(prefix.length):'sufficient';const provider=r.provider_ready===true?'provider healthy':(sourceWait==='stale'?'provider stale':sourceWait==='provider_gap'?'provider missing':'provider degraded');const source=sourceWait==='sufficient'?'source sufficient':`source ${sourceWait.replaceAll('_',' ')}`;const qualification=sourceWait==='sufficient'?(stage?stage.replaceAll('_',' '):'qualification pending'):'qualification blocked';return `${provider} · ${source} · ${qualification}`};
renderMechanisms=function(rows){rows=rows||[];const state=r=>`<span class=\"state ${esc(r.state)}\">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`,dimensions=r=>`<div class=\"muted\" style=\"font-size:10px;margin-top:4px\">${esc(sourceDimensions(r))}</div>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class=\"muted\">${esc(r.mechanism_id)}</span></td><td>${state(r)}${dimensions(r)}</td><td class=\"num\">${num(r.independent_forward_outcome_count)}</td><td class=\"num\">${num(r.settled_allocator_outcome_count)}</td><td class=\"num ${pnlClass(r.mean_forward_net_return)}\">${pct(r.mean_forward_net_return)}</td><td><div>${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</td></tr>`).join(''):`<tr><td colspan=\"6\" class=\"muted\">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class=\"item\"><div class=\"item-top\"><div class=\"item-title\">${esc(r.name)}</div>${state(r)}</div>${dimensions(r)}<div class=\"item-sub\">${esc(r.primary_reason)}</div>${strategyEvidenceDetail(r)}</div>`).join(''):itemEmpty('No certification snapshot yet.')}""",
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
