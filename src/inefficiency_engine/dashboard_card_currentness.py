from __future__ import annotations

from typing import Any


_MEANINGFUL_CONCLUSIONS = {
    "statistical_failure": "STATISTICAL FAILURE",
    "poor_economics": "POOR ECONOMICS",
    "execution_blocked": "EXECUTION BLOCKED",
    "settlement_blocked": "SETTLEMENT BLOCKED",
    "certifying": "CERTIFYING",
    "certified": "CERTIFIED",
}
_RUNTIME_STALE_STATUSES = {"stale", "degraded", "unavailable"}


def preserve_meaningful_card_conclusions(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Keep the last substantive lane conclusion visible without making it current.

    The V5 builder deliberately fails closed when the research heartbeat is stale.
    That is correct for qualification and paper capability, but replacing an already
    persisted statistical/economic conclusion with only ``RESEARCH STALE`` removes
    the most useful diagnostic fact from the card. This presentation-only adapter
    keeps both truths: the last substantive conclusion and the fact that it is not
    current. It never changes qualification, paper capability, certification, source
    truth, sizing, or execution authority.
    """

    result = dict(snapshot)
    system = dict(result.get("system") or {}) if isinstance(result.get("system"), dict) else {}
    projection_current = bool(system.get("projection_current_for_execution"))
    raw_cards = result.get("cards")
    cards = [dict(card) for card in raw_cards if isinstance(card, dict)] if isinstance(raw_cards, list) else []

    historical_count = 0
    for card in cards:
        research_status = str(card.get("research_status") or "unavailable")
        conclusion_current = research_status == "current"
        card["research_conclusion_current"] = conclusion_current
        card["paper_decision_current"] = projection_current and conclusion_current
        card["runtime_warning"] = None

        state = str(card.get("persisted_state") or "")
        label = _MEANINGFUL_CONCLUSIONS.get(state)
        current_status = str(card.get("status") or "")
        source_blocks_first = current_status in {
            "SOURCE MISSING",
            "SOURCE STALE",
            "EVIDENCE INCOMPLETE",
            "REDUNDANCY PENDING",
        }

        if research_status not in _RUNTIME_STALE_STATUSES:
            continue

        card["runtime_warning"] = (
            f"Research runtime is {research_status}; the last persisted conclusion is historical until fresh research replaces it."
        )
        if label is None or source_blocks_first:
            continue

        historical_count += 1
        suffix = "STALE" if research_status == "stale" else "RUNTIME DEGRADED"
        card["status"] = f"{label} · {suffix}"
        card["primary_blocker"] = (
            f"Last persisted conclusion: {state.replace('_', ' ')}. Research runtime is {research_status}, "
            "so that conclusion is diagnostic history rather than a current allocation-grade decision."
        )
        card["next_action"] = (
            "Restore successful current research publication and re-evaluate with fresh evidence; preserve all existing qualification and execution thresholds."
        )

    summary = dict(result.get("summary") or {}) if isinstance(result.get("summary"), dict) else {}
    summary["historical_substantive_conclusion_lanes"] = historical_count
    result["summary"] = summary
    result["cards"] = cards
    result["card_currentness_contract"] = "substantive_conclusion_plus_explicit_runtime_currentness_v1"
    result["card_currentness_presentation_only"] = True
    result["live_execution_authority"] = False
    return result
