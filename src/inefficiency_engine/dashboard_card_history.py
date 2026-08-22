from __future__ import annotations

from typing import Any

from inefficiency_engine.dashboard_research_closure import RESEARCH_CLOSURE_DASHBOARD_HTML


CARD_TRUTH_RESOLVER_VERSION = "v3_history_preserving"
DASHBOARD_UI_CONTRACT_VERSION = "v3_history_timestamps"


def _int(value: object | None) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def restore_card_history_truth(payload: dict[str, Any]) -> dict[str, Any]:
    """Restore durable card history without weakening current source truth.

    The v2 source resolver intentionally separated provider status from stale research,
    but it also replaced each durable/cumulative card count with the latest admitted
    source snapshot item count. That made persisted evidence appear to disappear.

    This final presentation reconciliation keeps those domains separate:
    - current source truth owns provider status, source completeness, and current items;
    - the persisted research projection keeps the durable/cumulative input-record count;
    - signals, forward outcomes, qualification, settlement, and certification are
      untouched and remain fail-closed when their projections are stale.
    """

    result = dict(payload)
    mechanisms = result.get("mechanisms")
    if not isinstance(mechanisms, dict):
        result["card_truth_resolver"] = CARD_TRUTH_RESOLVER_VERSION
        result["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
        return result

    mechanism_payload = dict(mechanisms)
    rows = mechanism_payload.get("mechanisms")
    if not isinstance(rows, list):
        result["card_truth_resolver"] = CARD_TRUTH_RESOLVER_VERSION
        result["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
        return result

    restored: list[object] = []
    for raw in rows:
        if not isinstance(raw, dict):
            restored.append(raw)
            continue

        row = dict(raw)
        card_truth = dict(row.get("card_truth") or {}) if isinstance(row.get("card_truth"), dict) else {}
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
        current_display = _int(row.get("authoritative_observation_count"))
        legacy_count = _int(row.get("legacy_projected_observation_count"))

        # When v2 saved a pre-overlay count, restore it as the durable/cumulative
        # display history. If there was no prior history, retain the current snapshot
        # count rather than manufacturing a zero.
        display_count = legacy_count if legacy_count > 0 else current_display
        row["authoritative_observation_count"] = display_count
        row["authoritative_observation_count_semantics"] = (
            "persisted_cumulative_source_records"
            if legacy_count > 0
            else str(
                row.get("authoritative_observation_count_semantics")
                or source_truth.get("observation_count_semantics")
                or "current_admitted_source_items"
            )
        )
        row["current_authoritative_item_count"] = current_items
        row["current_authoritative_item_count_semantics"] = "current_admitted_source_items"

        source_current_at = source_truth.get("latest_authoritative_observation_at")
        source_seen_at = source_truth.get("latest_seen_source_observation_at")
        if source_current_at:
            row["current_source_observation_at"] = source_current_at
        if source_seen_at:
            row["latest_seen_source_observation_at"] = source_seen_at

        card_truth.update(
            {
                "display_input_record_count": display_count,
                "display_input_record_count_semantics": row[
                    "authoritative_observation_count_semantics"
                ],
                "current_authoritative_item_count": current_items,
                "current_authoritative_item_count_semantics": "current_admitted_source_items",
                "current_source_observation_at": source_current_at,
                "latest_seen_source_observation_at": source_seen_at,
                "history_preserved": True,
                "history_preservation_allocation_authority": False,
                "history_preservation_live_execution_authority": False,
            }
        )
        row["card_truth"] = card_truth
        restored.append(row)

    mechanism_payload["mechanisms"] = restored
    mechanism_payload["card_truth_resolver"] = CARD_TRUTH_RESOLVER_VERSION
    mechanism_payload["dashboard_ui_contract_version"] = DASHBOARD_UI_CONTRACT_VERSION
    result["mechanisms"] = mechanism_payload
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
        "${evidenceStep('Provider',providerCardValue(r),`${phase} · ${obs} current authoritative`,providerCls)}${evidenceStep('Observations',num(obs),`${signals} signals`,obsCls)}",
        "${evidenceStep('Provider',providerCardValue(r),`${num(currentSourceItems(r))} current source items`,providerCls)}${evidenceStep('Input records',num(obs),`${num(currentSourceItems(r))} current · ${signals} signals`,obsCls)}",
    )

    html = _replace_once(
        html,
        "$('evidenceSnapshot').textContent=observedAt?`Certification evidence snapshot ${new Date(observedAt).toLocaleString()} · common forward target ${forwardTarget} · settled-trial target ${settledTarget}`:`Awaiting certification evidence snapshot · forward target ${forwardTarget} · settled-trial target ${settledTarget}`;",
        "$('evidenceSnapshot').textContent=observedAt?`Research projection ${new Date(observedAt).toLocaleString()}${laneTruth?.projection_current_for_execution===false?' · stale / fail-closed':''} · source timestamps shown per card · forward target ${forwardTarget} · settled-trial target ${settledTarget}`:`Awaiting research projection · source timestamps shown per card · forward target ${forwardTarget} · settled-trial target ${settledTarget}`;",
    )

    html = _replace_once(
        html,
        "<div class=\"evidence-time\">${last?`Last evidence ${when(last)}`:'No forward evidence timestamp yet'}${next?` · Next expected ${when(next)}`:''}</div></div>`",
        "<div class=\"evidence-time\">${esc(evidenceTimeline(r,last,next))}</div></div>`",
    )

    return html


CARD_HISTORY_DASHBOARD_HTML = _build_dashboard_html()
