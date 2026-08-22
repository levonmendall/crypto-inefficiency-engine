from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


DASHBOARD_UI_CONTRACT_VERSION = "v5_mechanism_truth"
CARD_VIEW_VERSION = "v5"
_HEALTHY_RESEARCH_STATES = {"starting", "running", "success"}


def _int(value: object | None) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _float(value: object | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _time(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif value:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(value: object | None) -> str | None:
    dt = _time(value)
    return dt.isoformat() if dt is not None else None


def _age_seconds(value: object | None, now: datetime) -> float | None:
    dt = _time(value)
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds())


def _worker(payload: dict[str, Any], name: str) -> dict[str, Any]:
    runtime = payload.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    worker = workers.get(name) if isinstance(workers, dict) else None
    return dict(worker) if isinstance(worker, dict) else {}


def _research_runtime(payload: dict[str, Any]) -> dict[str, Any]:
    worker = _worker(payload, "research")
    available = bool(worker.get("available"))
    state = str(worker.get("state") or ("unavailable" if not available else "unknown"))
    stale = bool(worker.get("stale")) if available else True
    error_type = worker.get("error_type")
    current = available and not stale and state in _HEALTHY_RESEARCH_STATES and not error_type
    if current:
        status = "current"
    elif available and stale:
        status = "stale"
    elif available and error_type:
        status = "degraded"
    elif available:
        status = "degraded"
    else:
        status = "unavailable"
    return {
        "status": status,
        "current": current,
        "state": state,
        "error_type": error_type,
        "observed_at": _iso(worker.get("observed_at")),
        "age_seconds": _float(worker.get("age_seconds")),
    }


def _operating_runtime(payload: dict[str, Any]) -> dict[str, Any]:
    worker = _worker(payload, "portfolio")
    available = bool(worker.get("available"))
    state = str(worker.get("state") or ("unavailable" if not available else "unknown"))
    stale = bool(worker.get("stale")) if available else True
    current = available and not stale and state in _HEALTHY_RESEARCH_STATES
    return {
        "status": "current" if current else ("stale" if available and stale else "degraded" if available else "unavailable"),
        "current": current,
        "state": state,
        "error_type": worker.get("error_type"),
        "observed_at": _iso(worker.get("observed_at")),
        "age_seconds": _float(worker.get("age_seconds")),
    }


def _source_truth(payload: dict[str, Any], lane_id: str) -> dict[str, Any]:
    truth = payload.get("current_source_truth")
    lane = truth.get(lane_id) if isinstance(truth, dict) else None
    return dict(lane) if isinstance(lane, dict) else {}


def _provider_status(source: dict[str, Any]) -> str:
    raw = str(source.get("provider_status") or "").lower()
    if raw in {"connected", "stale", "missing"}:
        return raw
    if bool(source.get("connected")):
        return "connected"
    if str(source.get("source_state") or "") == "stale":
        return "stale"
    return "missing"


def _evidence_status(source: dict[str, Any]) -> str:
    state = str(source.get("source_state") or "")
    if state == "sufficient":
        return "complete"
    if state == "redundancy_gap":
        return "redundancy_pending"
    if state == "evidence_class_gap":
        return "incomplete"
    if state == "stale":
        return "stale"
    if state == "provider_gap":
        return "missing"
    if bool(source.get("evidence_complete")):
        return "complete"
    return "unknown"


def _paper_capable(payload: dict[str, Any], lane_id: str, research_current: bool) -> bool:
    lane = payload.get("lane_executability")
    if not isinstance(lane, dict) or not research_current:
        return False
    if lane.get("projection_current_for_execution") is not True:
        return False
    capable = lane.get("paper_execution_capable_lanes")
    return lane_id in capable if isinstance(capable, list) else False


def _strategy_evidence(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("strategy_evidence")
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        result.append(
            {
                "strategy_id": item.get("strategy_id"),
                "name": item.get("name") or item.get("strategy_id"),
                "state": item.get("state"),
                "forward_outcomes": _int(item.get("independent_forward_outcome_count")),
                "forward_required": _int(item.get("required_forward_outcomes")),
                "mean_forward_net_return": _float(item.get("mean_forward_net_return")),
                "mean_forward_net_return_ci_lower": _float(item.get("mean_forward_net_return_ci_lower")),
                "required_mean_return_ci_lower": _float(item.get("required_mean_return_ci_lower")),
                "forward_hit_rate": _float(item.get("forward_hit_rate")),
                "forward_hit_rate_ci_lower": _float(item.get("forward_hit_rate_ci_lower")),
                "required_hit_rate_ci_lower": _float(item.get("required_hit_rate_ci_lower")),
                "observed_regime_count": _int(item.get("observed_regime_count")),
                "required_regimes": _int(item.get("required_regimes")),
                "failed_gates": [str(value) for value in list(item.get("failed_gates") or [])],
                "primary_reason": item.get("primary_reason"),
            }
        )
    return result


def _blocker(
    *,
    provider_status: str,
    evidence_status: str,
    missing_classes: list[str],
    research_status: str,
    row: dict[str, Any],
) -> tuple[str, str]:
    if provider_status == "missing":
        return (
            "No current admitted authoritative source is available.",
            "Connect or restore a current authoritative source; keep qualification and allocation fail-closed.",
        )
    if provider_status == "stale":
        return (
            "Authoritative source integration exists, but the latest usable source evidence is stale.",
            "Refresh the admitted authoritative source evidence; do not infer current economics from stale inputs.",
        )
    if evidence_status == "incomplete":
        classes = ", ".join(missing_classes) if missing_classes else "required evidence classes"
        return (
            f"Current authoritative sources are connected, but evidence is incomplete: {classes}.",
            "Collect the missing canonical evidence classes before forward qualification or paper allocation.",
        )
    if evidence_status == "redundancy_pending":
        return (
            "Current evidence is connected and complete for research, but independent-source redundancy is not satisfied.",
            "Restore independent authoritative-source redundancy while continuing research-only evidence collection.",
        )
    if research_status in {"stale", "degraded", "unavailable"}:
        return (
            f"Source truth is usable, but the research runtime is {research_status}; downstream conclusions are not current.",
            "Restore successful current research publication before treating qualification or paper capability as current.",
        )
    reason = str(row.get("primary_reason") or "No current blocker recorded.")
    action = str(row.get("next_action") or "Continue governed evidence collection.")
    return reason, action


def _status_label(
    *,
    provider_status: str,
    evidence_status: str,
    research_status: str,
    state: str,
    paper_capable: bool,
    qualified_count: int,
) -> str:
    if provider_status == "missing":
        return "SOURCE MISSING"
    if provider_status == "stale":
        return "SOURCE STALE"
    if evidence_status == "incomplete":
        return "EVIDENCE INCOMPLETE"
    if evidence_status == "redundancy_pending":
        return "REDUNDANCY PENDING"
    if research_status == "stale":
        return "RESEARCH STALE"
    if research_status in {"degraded", "unavailable"}:
        return "RESEARCH DEGRADED"
    if state == "certified":
        return "CERTIFIED"
    if paper_capable:
        return "PAPER CAPABLE"
    if qualified_count > 0:
        return "QUALIFIED"
    if state == "statistical_failure":
        return "STATISTICAL FAILURE"
    if state == "poor_economics":
        return "POOR ECONOMICS"
    if state == "execution_blocked":
        return "EXECUTION BLOCKED"
    if state == "settlement_blocked":
        return "SETTLEMENT BLOCKED"
    if state == "certifying":
        return "CERTIFYING"
    return "COLLECTING"


def _card(row: dict[str, Any], payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    lane_id = str(row.get("mechanism_id") or "")
    source = _source_truth(payload, lane_id)
    provider_status = _provider_status(source)
    evidence_status = _evidence_status(source)
    research = _research_runtime(payload)
    research_status = str(research["status"])
    forward_target = max(
        1,
        _int((payload.get("mechanisms") or {}).get("requirements", {}).get("independent_forward_outcomes"))
        if isinstance(payload.get("mechanisms"), dict)
        else 30,
    )
    if forward_target <= 1:
        forward_target = 30
    settled_target = max(
        1,
        _int((payload.get("mechanisms") or {}).get("requirements", {}).get("settled_allocator_outcomes"))
        if isinstance(payload.get("mechanisms"), dict)
        else 20,
    )
    if settled_target <= 1:
        settled_target = 20

    source_items = _int(source.get("current_authoritative_item_count"))
    source_at = source.get("latest_authoritative_observation_at") or source.get("latest_seen_source_observation_at")
    last_research_at = (
        row.get("forward_evidence_last_outcome_at")
        or row.get("forward_evidence_last_signal_at")
        or row.get("forward_evidence_last_cycle_at")
    )
    due_at = row.get("forward_evidence_next_expected_at")
    due_time = _time(due_at)
    due_state = "overdue" if due_time is not None and due_time <= now else "scheduled" if due_time is not None else "unknown"

    funnel = row.get("rejection_funnel") if isinstance(row.get("rejection_funnel"), dict) else {}
    raw_candidates = _int(row.get("raw_candidate_count") or funnel.get("raw_candidate_count"))
    emitted_candidates = _int(row.get("emitted_candidate_count") or funnel.get("emitted_candidate_count"))
    signals = _int(row.get("forward_signal_count"))
    forward = _int(row.get("independent_forward_outcome_count"))
    qualified = _int(row.get("current_statistically_qualified_count"))
    settled = _int(row.get("settled_allocator_outcome_count"))
    state = str(row.get("state") or "collecting")
    paper_capable = _paper_capable(payload, lane_id, bool(research.get("current")))
    missing_classes = [str(value) for value in list(source.get("missing_evidence_classes") or [])]
    blocker, next_action = _blocker(
        provider_status=provider_status,
        evidence_status=evidence_status,
        missing_classes=missing_classes,
        research_status=research_status,
        row=row,
    )

    return {
        "mechanism_id": lane_id,
        "name": row.get("name") or lane_id,
        "status": _status_label(
            provider_status=provider_status,
            evidence_status=evidence_status,
            research_status=research_status,
            state=state,
            paper_capable=paper_capable,
            qualified_count=qualified,
        ),
        "provider_status": provider_status,
        "evidence_status": evidence_status,
        "source_state": source.get("source_state"),
        "source_item_count": source_items,
        "source_observed_at": _iso(source_at),
        "source_age_seconds": _age_seconds(source_at, now),
        "source_ids": [str(value) for value in list(source.get("admitted_source_ids") or [])],
        "stale_source_ids": [str(value) for value in list(source.get("stale_source_ids") or [])],
        "covered_evidence_classes": [str(value) for value in list(source.get("covered_evidence_classes") or [])],
        "missing_evidence_classes": missing_classes,
        "independent_source_count": _int(source.get("independent_authoritative_source_count")),
        "research_status": research_status,
        "research_worker_state": research.get("state"),
        "research_worker_error": research.get("error_type"),
        "research_worker_observed_at": research.get("observed_at"),
        "research_last_at": _iso(last_research_at),
        "research_due_at": _iso(due_at),
        "research_due_state": due_state,
        "persisted_state": state,
        "stage": row.get("stage"),
        "last_conclusion": state.replace("_", " "),
        "raw_candidate_count": raw_candidates,
        "emitted_candidate_count": emitted_candidates,
        "signal_count": signals,
        "forward_outcome_count": forward,
        "forward_target": forward_target,
        "qualified_count": qualified,
        "current_candidate_count": _int(row.get("current_candidate_count")),
        "paper_capable": paper_capable,
        "settled_count": settled,
        "settled_target": settled_target,
        "certified": bool(row.get("profitability_certified")) or state == "certified",
        "mean_forward_net_return": _float(row.get("mean_forward_net_return")),
        "mean_forward_net_return_ci_lower": _float(row.get("mean_forward_net_return_ci_lower")),
        "forward_hit_rate": _float(row.get("forward_hit_rate")),
        "forward_hit_rate_ci_lower": _float(row.get("forward_hit_rate_ci_lower")),
        "best_net_economics": _float(row.get("best_net_economics") or funnel.get("best_net_economics")),
        "required_net_economics": _float(row.get("required_net_economics") or funnel.get("required_net_economics")),
        "gap_to_hurdle": _float(row.get("gap_to_hurdle") or funnel.get("gap_to_hurdle")),
        "economics_unit": row.get("economics_unit") or funnel.get("economics_unit"),
        "dominant_rejection_gate": row.get("dominant_rejection_gate") or funnel.get("dominant_rejection_gate"),
        "allocator_realized_profit_usd": _float(row.get("allocator_realized_profit_usd")),
        "maker_shadow": {
            "trials": _int(row.get("maker_shadow_trial_count")),
            "outcomes": _int(row.get("maker_shadow_outcome_count")),
            "queue_confirmed_fills": _int(row.get("maker_queue_fill_confirmed_count")),
            "crossed_through": _int(row.get("maker_crossed_through_count")),
            "adverse_selection_observations": _int(row.get("maker_adverse_selection_observation_count")),
        },
        "capital_location": {
            "mean_incremental_option_value": _float(row.get("capital_location_mean_incremental_option_value")),
            "positive_incremental_rate": _float(row.get("capital_location_positive_incremental_rate")),
        },
        "strategy_evidence": _strategy_evidence(row),
        "primary_blocker": blocker,
        "next_action": next_action,
        "paper_only": True,
        "live_execution_authority": False,
    }


def build_dashboard_v5_snapshot(payload: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Build a single unambiguous read model for the 13 mechanism cards.

    This function has no investment, provider, qualification, or allocation authority.
    It only reconciles already-persisted source, research, forward, qualification,
    settlement, certification, and runtime truth into a presentation contract.
    """

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    mechanisms = payload.get("mechanisms")
    rows = mechanisms.get("mechanisms") if isinstance(mechanisms, dict) else None
    raw_rows = [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    cards = [_card(row, payload, current) for row in raw_rows]
    research = _research_runtime(payload)
    portfolio = _operating_runtime(payload)
    lane = payload.get("lane_executability") if isinstance(payload.get("lane_executability"), dict) else {}

    provider_connected = sum(card["provider_status"] == "connected" for card in cards)
    evidence_complete = sum(card["evidence_status"] == "complete" for card in cards)
    current_source_data = sum(_int(card["source_item_count"]) > 0 for card in cards)
    forward_mature = sum(_int(card["forward_outcome_count"]) >= _int(card["forward_target"]) for card in cards)
    qualified_lanes = sum(_int(card["qualified_count"]) > 0 for card in cards)
    paper_capable_lanes = sum(bool(card["paper_capable"]) for card in cards)
    certified_lanes = sum(bool(card["certified"]) for card in cards)

    projection_at = None
    if isinstance(mechanisms, dict):
        projection_at = mechanisms.get("observed_at")
    projection_at = projection_at or payload.get("research_projection_observed_at")

    return {
        "dashboard_contract_active": True,
        "dashboard_ui_contract_version": DASHBOARD_UI_CONTRACT_VERSION,
        "card_view_version": CARD_VIEW_VERSION,
        "generated_at": current.isoformat(),
        "release_commit": payload.get("release_commit"),
        "paper_only": True,
        "live_execution_authority": False,
        "system": {
            "research_runtime": research,
            "portfolio_runtime": portfolio,
            "research_projection_stale": bool(payload.get("research_projection_stale")),
            "operating_projection_stale": bool(payload.get("operating_projection_stale")),
            "projection_observed_at": _iso(projection_at),
            "projection_age_seconds": _age_seconds(projection_at, current),
            "projection_current_for_execution": bool(lane.get("projection_current_for_execution")) and bool(research.get("current")),
            "volume_universe": payload.get("volume_universe") if isinstance(payload.get("volume_universe"), dict) else {},
        },
        "summary": {
            "lane_count": len(cards),
            "provider_connected": provider_connected,
            "evidence_complete": evidence_complete,
            "current_source_data": current_source_data,
            "raw_candidates": sum(_int(card["raw_candidate_count"]) for card in cards),
            "emitted_candidates": sum(_int(card["emitted_candidate_count"]) for card in cards),
            "signals": sum(_int(card["signal_count"]) for card in cards),
            "forward_mature": forward_mature,
            "qualified_lanes": qualified_lanes,
            "paper_capable_lanes": paper_capable_lanes,
            "certified_lanes": certified_lanes,
        },
        "cards": cards,
        "legacy_dashboard_payload": {
            "available": bool(raw_rows),
            "mechanism_count": len(raw_rows),
            "not_rendered_by_v5": True,
        },
    }


DASHBOARD_V5_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark">
<title>Portfolio Command Center</title>
<style>
:root{--bg:#06111a;--panel:#0b1a25;--panel2:#08151e;--line:#213b4c;--text:#edf7fb;--muted:#91aabb;--good:#39d47d;--warn:#f4cf4f;--bad:#ff7884;--blue:#65c7ff}
*{box-sizing:border-box}body{margin:0;background:linear-gradient(#051019,#081722);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1180px;margin:auto;padding:18px}.hero,.section{background:var(--panel);border:1px solid var(--line);border-radius:20px;padding:18px;margin-bottom:16px}.title{font-size:28px;font-weight:850}.sub{color:var(--muted);margin-top:6px;line-height:1.45}.topline{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}.badge{border:1px solid var(--line);border-radius:999px;padding:7px 11px;font-size:11px;font-weight:850;letter-spacing:.06em;white-space:nowrap}.badge.good{border-color:#1f7041;color:#a9f5c8}.badge.warn{border-color:#76651c;color:#ffe780}.badge.bad{border-color:#7a3240;color:#ffc0c6}.summary{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:9px;margin-top:16px}.stat,.metric{background:var(--panel2);border:1px solid var(--line);border-radius:13px;padding:11px}.k{text-transform:uppercase;color:var(--muted);font-size:10px;font-weight:800;letter-spacing:.07em}.v{font-size:20px;font-weight:850;margin-top:4px}.d{font-size:10px;color:var(--muted);margin-top:3px;line-height:1.3}.cards{display:grid;gap:14px}.card{background:var(--panel2);border:1px solid var(--line);border-radius:18px;padding:16px}.cardhead{display:flex;justify-content:space-between;gap:12px}.name{font-size:20px;font-weight:850}.meta{color:var(--muted);font-size:12px;margin-top:5px;line-height:1.4}.metrics{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:8px;margin-top:13px}.metric.good{border-color:#1e6139;background:#092418}.metric.warn{border-color:#6d5d19;background:#211e0b}.metric.bad{border-color:#71323b;background:#241016}.strip{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:9px}.strip .metric .v{font-size:15px}.reason{margin-top:12px;color:#c7dce7;line-height:1.5}.next{margin-top:6px;color:#e4f8ff;line-height:1.45}.time{margin-top:8px;color:var(--muted);font-size:11px;line-height:1.45}.details{margin-top:10px;border-top:1px solid var(--line);padding-top:9px}details summary{cursor:pointer;color:#bce9ff;font-weight:750}.strategy{margin-top:8px;padding:9px;border:1px solid var(--line);border-radius:10px}.badtext{color:#ff9da6}.goodtext{color:#94f0b7}.muted{color:var(--muted)}#error{display:none;border-color:#7a3240;color:#ffc0c6}.footer{color:var(--muted);font-size:11px;text-align:center;padding:8px}
@media(max-width:850px){.summary{grid-template-columns:repeat(3,1fr)}.metrics,.strip{grid-template-columns:repeat(2,1fr)}}
@media(max-width:520px){.wrap{padding:10px}.hero,.section{padding:14px;border-radius:16px}.title{font-size:24px}.summary{grid-template-columns:repeat(2,1fr)}.name{font-size:18px}.metric .v{font-size:18px}}
</style>
</head>
<body><main class="wrap">
<section class="hero"><div class="topline"><div><div class="title">Portfolio Command Center</div><div class="sub">Mechanism truth · current sources → candidates → forward evidence → qualification → paper capability → certification</div></div><span id="contract" class="badge">V5</span></div><div id="system" class="sub">Loading current persisted truth…</div><div id="summary" class="summary"></div></section>
<section id="error" class="section"></section>
<section class="section"><div class="topline"><div><div class="name">Profit mechanism cards</div><div class="sub">Each card is a direct rendering of one server-built read model. The browser does not infer provider, qualification, or execution state.</div></div><button id="refresh" class="badge" style="background:transparent;color:var(--text)">REFRESH</button></div><div id="cards" class="cards" style="margin-top:14px"></div></section>
<div class="footer">Paper-only research surface · no live execution authority</div>
</main>
<script>
const $=id=>document.getElementById(id),esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),num=v=>Number(v||0).toLocaleString();
const pct=v=>Number.isFinite(+v)?`${(+v*100).toFixed(2)}%`:'—',money=v=>Number.isFinite(+v)?new Intl.NumberFormat(undefined,{style:'currency',currency:'USD',maximumFractionDigits:2}).format(+v):'—';
function when(v){if(!v)return '—';const d=new Date(v);return Number.isNaN(d.getTime())?'—':d.toLocaleString()}
function age(seconds){if(!Number.isFinite(+seconds))return 'age unknown';const s=+seconds;if(s<120)return `${Math.round(s)}s old`;if(s<7200)return `${Math.round(s/60)}m old`;if(s<172800)return `${(s/3600).toFixed(1)}h old`;return `${(s/86400).toFixed(1)}d old`}
function clsStatus(s){s=String(s||'').toLowerCase();if(s.includes('certified')||s.includes('paper capable')||s==='connected'||s==='complete'||s==='current')return 'good';if(s.includes('missing')||s.includes('failure')||s.includes('poor')||s.includes('blocked')||s.includes('degraded'))return 'bad';return 'warn'}
function stat(k,v,d=''){return `<div class="stat"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div><div class="d">${esc(d)}</div></div>`}
function metric(k,v,d='',c=''){return `<div class="metric ${esc(c)}"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div><div class="d">${esc(d)}</div></div>`}
function candidates(c){const has=(+c.raw_candidate_count||0)>0||(+c.emitted_candidate_count||0)>0||c.dominant_rejection_gate;return has?`${num(c.raw_candidate_count)} / ${num(c.emitted_candidate_count)}`:'—'}
function researchTimeline(c){const p=[];if(c.source_observed_at)p.push(`${c.provider_status==='stale'?'Last':'Current'} source ${when(c.source_observed_at)} (${age(c.source_age_seconds)})`);else p.push('No current source timestamp');if(c.research_last_at)p.push(`Research evidence ${when(c.research_last_at)}`);else p.push('No forward research timestamp');if(c.research_due_at)p.push(c.research_due_state==='overdue'?`Research overdue since ${when(c.research_due_at)}`:`Next research expected ${when(c.research_due_at)}`);return p.join(' · ')}
function strategyDetails(rows){if(!rows?.length)return '';return `<details class="details"><summary>Strategy-level qualification evidence (${rows.length})</summary>${rows.map(s=>`<div class="strategy"><strong>${esc(s.name||s.strategy_id)}</strong> · ${esc(String(s.state||'unknown').replaceAll('_',' '))}<div class="d">Forward ${num(s.forward_outcomes)} / ${num(s.forward_required)} · mean ${pct(s.mean_forward_net_return)} · CI ${pct(s.mean_forward_net_return_ci_lower)} vs ${pct(s.required_mean_return_ci_lower)} required · hit CI ${pct(s.forward_hit_rate_ci_lower)} vs ${pct(s.required_hit_rate_ci_lower)} required · regimes ${num(s.observed_regime_count)} / ${num(s.required_regimes)}</div>${s.failed_gates?.length?`<div class="badtext" style="font-size:11px;margin-top:5px">Failed: ${esc(s.failed_gates.join(' · '))}</div>`:`<div class="goodtext" style="font-size:11px;margin-top:5px">No strategy-specific diagnostic gate currently failing</div>`}</div>`).join('')}</details>`}
function special(c){const rows=[];if(c.dominant_rejection_gate||Number.isFinite(+c.best_net_economics))rows.push(`Rejection gate: ${c.dominant_rejection_gate||'—'} · best net ${pct(c.best_net_economics)} · hurdle ${pct(c.required_net_economics)} · gap ${pct(c.gap_to_hurdle)}`);if(c.maker_shadow?.trials||c.maker_shadow?.outcomes)rows.push(`Maker shadow: ${num(c.maker_shadow.trials)} trials · ${num(c.maker_shadow.outcomes)} matured · ${num(c.maker_shadow.queue_confirmed_fills)} queue-confirmed fills · ${num(c.maker_shadow.adverse_selection_observations)} adverse-selection observations`);if(Number.isFinite(+c.capital_location?.mean_incremental_option_value))rows.push(`Location forward: mean incremental option value ${pct(c.capital_location.mean_incremental_option_value)} · positive rate ${pct(c.capital_location.positive_incremental_rate)}`);return rows.length?`<div class="details d">${rows.map(esc).join('<br>')}</div>`:''}
function renderCard(c){const srcDetail=c.provider_status==='connected'?`${num(c.source_item_count)} admitted items · ${num(c.independent_source_count)} independent source groups`:(c.provider_status==='stale'?`${(c.stale_source_ids||[]).length} stale configured sources`:'no admitted current source');const missing=(c.missing_evidence_classes||[]).length?`Missing: ${c.missing_evidence_classes.join(', ')}`:`Covered: ${(c.covered_evidence_classes||[]).join(', ')||'n/a'}`;return `<article class="card"><div class="cardhead"><div><div class="name">${esc(c.name)}</div><div class="meta">Provider ${esc(c.provider_status)} · evidence ${esc(c.evidence_status.replaceAll('_',' '))} · research ${esc(c.research_status)} · last conclusion ${esc(c.last_conclusion)}</div></div><span class="badge ${clsStatus(c.status)}">${esc(c.status)}</span></div><div class="metrics">${metric('Current source',num(c.source_item_count),`${srcDetail} · ${missing}`,clsStatus(c.provider_status))}${metric('Raw / emitted',candidates(c),c.dominant_rejection_gate?`dominant gate: ${c.dominant_rejection_gate.replaceAll('_',' ')}`:'candidate funnel not published')}${metric('Signals',num(c.signal_count),'research signals')}${metric('Forward',`${num(c.forward_outcome_count)} / ${num(c.forward_target)}`,'independent outcomes',c.forward_outcome_count>=c.forward_target?'good':'')}${metric('Qualified',num(c.qualified_count),'current statistical gate',c.qualified_count>0?'good':'')}${metric('Paper-capable',c.paper_capable?'Yes':'No',c.paper_capable?'current source + decision grade':'fail-closed',c.paper_capable?'good':'')}${metric('Settled',`${num(c.settled_count)} / ${num(c.settled_target)}`,'allocator outcomes',c.settled_count>=c.settled_target?'good':'')}${metric('Certified',c.certified?'Yes':'No','profitability certification',c.certified?'good':'')}</div><div class="strip">${metric('Forward mean',pct(c.mean_forward_net_return),'net return')}${metric('CI lower',pct(c.mean_forward_net_return_ci_lower),'forward mean lower bound')}${metric('Hit rate',pct(c.forward_hit_rate),'forward outcomes')}${metric('Realized P&L',money(c.allocator_realized_profit_usd),'paper allocator')}</div>${special(c)}<div class="reason"><strong>Current blocker:</strong> ${esc(c.primary_blocker)}</div><div class="next"><strong>Next:</strong> ${esc(c.next_action)}</div><div class="time">${esc(researchTimeline(c))}</div>${strategyDetails(c.strategy_evidence)}</article>`}
function render(p){$('contract').textContent=`${p.dashboard_ui_contract_version} · ${(p.release_commit||'unknown').slice(0,7)}`;const s=p.summary||{},sys=p.system||{},r=sys.research_runtime||{},po=sys.portfolio_runtime||{};$('system').innerHTML=`Research runtime <strong>${esc(r.status||'unknown')}</strong>${r.observed_at?` · ${esc(age(r.age_seconds))}`:''} · portfolio runtime <strong>${esc(po.status||'unknown')}</strong> · projection ${sys.projection_current_for_execution?'current for paper decisions':'fail-closed'}${sys.projection_observed_at?` · refreshed ${esc(when(sys.projection_observed_at))}`:''}`;$('summary').innerHTML=[stat('Provider connected',`${num(s.provider_connected)} / ${num(s.lane_count)}`,'current canonical source truth'),stat('Evidence complete',`${num(s.evidence_complete)} / ${num(s.lane_count)}`,'required classes satisfied'),stat('Current source data',`${num(s.current_source_data)} / ${num(s.lane_count)}`,'lanes with admitted current items'),stat('Raw / emitted candidates',`${num(s.raw_candidates)} / ${num(s.emitted_candidates)}`,'persisted candidate funnel'),stat('Forward mature',`${num(s.forward_mature)} / ${num(s.lane_count)}`,'met lane forward target'),stat('Qualified / paper / certified',`${num(s.qualified_lanes)} / ${num(s.paper_capable_lanes)} / ${num(s.certified_lanes)}`,'current governed conclusions')].join('');$('cards').innerHTML=(p.cards||[]).length?p.cards.map(renderCard).join(''):'<div class="muted">No mechanism cards are available.</div>'}
async function refresh(){try{const r=await fetch('/v3/dashboard/snapshot',{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);const p=await r.json();if(p.dashboard_ui_contract_version!=='v5_mechanism_truth')throw new Error(`Unexpected dashboard contract ${p.dashboard_ui_contract_version||'missing'}`);render(p);$('error').style.display='none'}catch(e){$('error').textContent=`Dashboard truth unavailable: ${e.message}`;$('error').style.display='block'}}
$('refresh').addEventListener('click',refresh);refresh();setInterval(refresh,30000);
</script></body></html>"""
