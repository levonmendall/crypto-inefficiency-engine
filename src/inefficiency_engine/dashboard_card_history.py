from __future__ import annotations

from typing import Any

from inefficiency_engine.dashboard_research_closure import RESEARCH_CLOSURE_DASHBOARD_HTML


CARD_TRUTH_RESOLVER_VERSION = "v4_current_source_truth"
DASHBOARD_UI_CONTRACT_VERSION = "v4_truthful_source_runtime"
_HEALTHY_RESEARCH_STATES = {"starting", "running", "success"}


def _int(value: object | None) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _research_runtime_stale(payload: dict[str, Any]) -> bool:
    runtime = payload.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    research = workers.get("research") if isinstance(workers, dict) else None
    if not isinstance(research, dict) or not research.get("available"):
        return True
    return bool(research.get("stale")) or str(research.get("state") or "") not in _HEALTHY_RESEARCH_STATES


def restore_card_history_truth(payload: dict[str, Any]) -> dict[str, Any]:
    """Finalize card truth without reviving diagnostic table high-water marks.

    Current canonical source truth owns provider state, source freshness, evidence
    completeness, and the displayed source-item count. Persisted research continues
    to own signals, forward outcomes, qualification, settlement, and certification.

    Older projections used append-only primary-key tails as cheap progress markers and
    those values later leaked into the UI as if they were lane-specific observation
    counts. They remain available only as explicitly non-authoritative diagnostics;
    they can never regain card display authority here.
    """

    result = dict(payload)
    runtime_stale = _research_runtime_stale(result)
    mechanisms = result.get("mechanisms")
    if isinstance(mechanisms, dict):
        mechanism_payload = dict(mechanisms)
        rows = mechanism_payload.get("mechanisms")
        if isinstance(rows, list):
            repaired_rows: list[object] = []
            for raw in rows:
                if not isinstance(raw, dict):
                    repaired_rows.append(raw)
                    continue

                row = dict(raw)
                card_truth = (
                    dict(row.get("card_truth") or {})
                    if isinstance(row.get("card_truth"), dict)
                    else {}
                )
                source_truth = (
                    dict(row.get("current_source_truth") or {})
                    if isinstance(row.get("current_source_truth"), dict)
                    else {}
                )
                current_items = _int(
                    row.get("current_authoritative_item_count")
                    or card_truth.get("current_authoritative_item_count")
                    or source_truth.get("current_authoritative_item_count")
                    or source_truth.get("authoritative_observation_count")
                )
                legacy_high_water = _int(row.get("legacy_projected_observation_count"))

                # The visible count is current admitted source material only.  A
                # legacy database-id high-water mark is retained for diagnostics but
                # is deliberately not described or rendered as historical evidence.
                row["authoritative_observation_count"] = current_items
                row["authoritative_observation_count_semantics"] = "current_admitted_source_items"
                row["current_authoritative_item_count"] = current_items
                row["current_authoritative_item_count_semantics"] = "current_admitted_source_items"
                row["historical_input_record_count"] = None
                row["historical_input_record_count_available"] = False
                if legacy_high_water > 0:
                    row["legacy_table_high_water_mark"] = legacy_high_water
                    row["legacy_table_high_water_mark_display_authority"] = False

                source_current_at = source_truth.get("latest_authoritative_observation_at")
                source_seen_at = source_truth.get("latest_seen_source_observation_at")
                if source_current_at:
                    row["current_source_observation_at"] = source_current_at
                if source_seen_at:
                    row["latest_seen_source_observation_at"] = source_seen_at

                card_truth.update(
                    {
                        "current_authoritative_item_count": current_items,
                        "current_authoritative_item_count_semantics": "current_admitted_source_items",
                        "current_source_observation_at": source_current_at,
                        "latest_seen_source_observation_at": source_seen_at,
                        "research_status": "stale" if runtime_stale else card_truth.get("research_status", "current"),
                        "legacy_high_water_display_authority": False,
                        "paper_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                    }
                )
                row["card_truth"] = card_truth
                row["research_runtime_stale"] = runtime_stale
                repaired_rows.append(row)

            mechanism_payload["mechanisms"] = repaired_rows
            mechanism_payload["card_truth_resolver"] = CARD_TRUTH_RESOLVER_VERSION
            mechanism_payload["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
            result["mechanisms"] = mechanism_payload

    lane = result.get("lane_executability")
    if runtime_stale and isinstance(lane, dict):
        lane_truth = dict(lane)
        lane_truth["projection_current_for_execution"] = False
        lane_truth["paper_execution_capable_count"] = 0
        lane_truth["paper_execution_capable_lanes"] = []
        lane_truth["all_lanes_paper_execution_capable"] = False
        lane_truth["research_runtime_current_for_execution"] = False
        result["lane_executability"] = lane_truth

    result["research_runtime_stale"] = runtime_stale
    result["card_truth_resolver"] = CARD_TRUTH_RESOLVER_VERSION
    result["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
    return result


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError("card-history dashboard overlay target changed unexpectedly")
    return source.replace(old, new, 1)


def _build_dashboard_html() -> str:
    html = RESEARCH_CLOSURE_DASHBOARD_HTML

    html = _replace_once(
        html,
        "function forwardMeanLabel(r){",
        """function currentSourceItems(r){return +r?.current_authoritative_item_count||+r?.card_truth?.current_authoritative_item_count||0}
function sourceEvidenceAt(r){return r?.current_source_observation_at||r?.card_truth?.current_source_observation_at||r?.current_source_truth?.latest_authoritative_observation_at||r?.latest_seen_source_observation_at||r?.current_source_truth?.latest_seen_source_observation_at||null}
function evidenceTimeline(r,last,next){const parts=[],sourceAt=sourceEvidenceAt(r),provider=String(r?.card_truth?.provider_status||'');if(sourceAt)parts.push(`${provider==='stale'?'Last source evidence':'Current source evidence'} ${when(sourceAt)}`);if(last)parts.push(`Research evidence ${when(last)}`);else parts.push('No forward research evidence yet');if(next){const t=new Date(next).getTime(),overdue=Number.isFinite(t)&&t<=Date.now();parts.push(overdue?`Research overdue since ${when(next)}`:`Next research expected ${when(next)}`)}return parts.join(' · ')}
function forwardMeanLabel(r){""",
    )

    html = _replace_once(
        html,
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>+r.authoritative_observation_count>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,certified=rows.filter(r=>r.state==='certified').length,laneCount=Math.max(1,+laneTruth?.lane_count||rows.length||13),paperCapableIds=new Set(laneTruth?.paper_execution_capable_lanes||[]),paperCapableCount=laneTruth?.available?(+laneTruth.paper_execution_capable_count||0):null;",
        "  const providerReady=rows.filter(r=>r.provider_ready).length,observed=rows.filter(r=>currentSourceItems(r)>0).length,forwardMature=rows.filter(r=>+r.independent_forward_outcome_count>=forwardTarget).length,qualified=rows.filter(r=>+r.current_statistically_qualified_count>0).length,certified=rows.filter(r=>r.state==='certified').length,laneCount=Math.max(1,+laneTruth?.lane_count||rows.length||13),paperCapableIds=new Set(laneTruth?.paper_execution_capable_lanes||[]),paperCapableCount=laneTruth?.available?(+laneTruth.paper_execution_capable_count||0):null;",
    )

    html = _replace_once(
        html,
        "${evidenceStep('Provider',providerCardValue(r),`${phase} · ${obs} current authoritative`,providerCls)}${evidenceStep('Observations',num(obs),`${signals} signals`,obsCls)}",
        "${evidenceStep('Provider',providerCardValue(r),`${phase}`,providerCls)}${evidenceStep('Current source',num(currentSourceItems(r)),`${signals} research signals`,obsCls)}",
    )

    html = _replace_once(
        html,
        "$('evidenceSnapshot').textContent=observedAt?`Certification evidence snapshot ${new Date(observedAt).toLocaleString()} · common forward target ${forwardTarget} · settled-trial target ${settledTarget}`:`Awaiting certification evidence snapshot · forward target ${forwardTarget} · settled-trial target ${settledTarget}`;",
        "$('evidenceSnapshot').textContent=observedAt?`Projection refreshed ${new Date(observedAt).toLocaleString()}${laneTruth?.projection_current_for_execution===false?' · research/operating truth fail-closed':''} · source and research timestamps shown per card · forward target ${forwardTarget} · settled-trial target ${settledTarget}`:`Awaiting research projection · source and research timestamps shown per card · forward target ${forwardTarget} · settled-trial target ${settledTarget}`;",
    )

    html = _replace_once(
        html,
        "<div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
        "<div class=\"evidence-time\">${esc(evidenceTimeline(r,last,next))}</div></div>`",
    )

    # Summary language must describe current source truth, not a historical/table-id
    # proxy. These are simple literal labels inherited from the base dashboard.
    html = html.replace("Provider ready", "Provider connected")
    html = html.replace("Authoritative data", "Current source data")
    return html


CARD_HISTORY_DASHBOARD_HTML = _build_dashboard_html()
