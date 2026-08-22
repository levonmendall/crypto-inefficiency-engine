from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text

from inefficiency_engine.evidence_velocity import evidence_freshness_seconds
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


_SAFE_TABLES = {
    "market_quotes",
    "funding_quotes",
    "order_books",
    "opportunities",
    "maker_shadow_outcomes",
    "capital_transfer_outcomes",
}
_SOURCE_WAIT_PREFIX = "waiting_for_source:"
_TERMINAL_RESEARCH_STATES = {
    "poor_economics",
    "statistical_failure",
    "execution_blocked",
    "settlement_blocked",
    "certifying",
    "certified",
}


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fallback_freshness_seconds() -> float:
    try:
        hours = float(os.getenv("CIE_SOURCE_COVERAGE_MAX_AGE_HOURS", "24"))
    except ValueError:
        hours = 24.0
    return max(60.0, hours * 3600.0)


def _json_payload(raw: object) -> dict[str, Any] | None:
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _candidate(
    *,
    observed_at: object | None,
    healthy: object,
    item_count: object,
    classes: list[str],
    authoritative: object = True,
    commercial: object = True,
    point_in_time: object = True,
    error_type: object | None = None,
    source_reference: object | None = None,
) -> dict[str, object]:
    try:
        count = max(0, int(item_count or 0))
    except (TypeError, ValueError):
        count = 0
    return {
        "observed_at": _parse_time(observed_at),
        "healthy": bool(healthy),
        "item_count": count,
        "classes": classes,
        "authoritative": bool(authoritative),
        "commercial": bool(commercial),
        "point_in_time": bool(point_in_time),
        "error_type": error_type,
        "source_reference": source_reference,
    }


def _latest_candidate(candidates: list[dict[str, object]]) -> dict[str, object] | None:
    if not candidates:
        return None

    def key(row: dict[str, object]) -> datetime:
        observed_at = row.get("observed_at")
        if isinstance(observed_at, datetime):
            return observed_at
        return datetime.min.replace(tzinfo=timezone.utc)

    return max(candidates, key=key)


def _table_candidate(db, spec: dict[str, object], available: set[str]) -> dict[str, object] | None:
    probe = spec.get("table")
    if not isinstance(probe, tuple) or len(probe) != 3:
        return None
    table_name, column, value = probe
    if table_name not in _SAFE_TABLES or table_name not in available:
        return None
    clause = ""
    params: dict[str, object] = {}
    if column is not None:
        if column not in {"venue", "asset", "strategy"}:
            return None
        clause = f" WHERE {column}=:value"
        params["value"] = value
    try:
        raw = db.execute(
            text(
                f"SELECT observed_at FROM {table_name}{clause} "
                "ORDER BY observed_at DESC LIMIT 1"
            ),
            params,
        ).scalar_one_or_none()
    except Exception:
        return None
    if raw is None:
        return None
    return _candidate(
        observed_at=raw,
        healthy=True,
        item_count=1,
        classes=[str(value) for value in list(spec.get("classes") or [])],
        authoritative=bool(spec.get("authoritative", True)),
        commercial=True,
        point_in_time=True,
        source_reference=f"durable:{table_name}",
    )


def _read_source_inputs(
    store,
) -> tuple[
    set[str],
    dict[tuple[str, str], dict[str, Any]],
    list[dict[str, object]],
    list[dict[str, Any]],
    dict[str, dict[str, object]],
]:
    available = set(inspect(store.engine).get_table_names())
    direct: dict[tuple[str, str], dict[str, Any]] = {}
    providers: list[dict[str, object]] = []
    admissions: list[dict[str, Any]] = []
    table_candidates: dict[str, dict[str, object]] = {}

    with store.engine.begin() as db:
        if getattr(store, "backend", "") == "postgresql":
            db.execute(text("SET LOCAL statement_timeout = '1200ms'"))
            db.execute(text("SET LOCAL lock_timeout = '300ms'"))

        if "source_coverage_observations" in available:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM source_coverage_observations "
                        "ORDER BY id DESC LIMIT 3000"
                    )
                ).scalars()
            )
            for raw in raws:
                payload = _json_payload(raw)
                if payload is None:
                    continue
                lane_id = str(payload.get("lane_id") or "")
                source_id = str(payload.get("source_id") or "")
                if lane_id in LANES and source_id:
                    direct.setdefault((lane_id, source_id), payload)

        if "provider_statuses" in available:
            rows = list(
                db.execute(
                    text(
                        "SELECT provider,ok,item_count,error_type,observed_at "
                        "FROM provider_statuses ORDER BY id DESC LIMIT 1000"
                    )
                ).mappings()
            )
            latest_provider: dict[str, dict[str, object]] = {}
            for row in rows:
                latest_provider.setdefault(str(row["provider"]), dict(row))
            providers = list(latest_provider.values())

        if "provider_gap_admissions" in available:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM provider_gap_admissions "
                        "ORDER BY id DESC LIMIT 1000"
                    )
                ).scalars()
            )
            latest_admission: dict[tuple[str, str], dict[str, Any]] = {}
            for raw in raws:
                payload = _json_payload(raw)
                if payload is None:
                    continue
                mechanism_id = str(payload.get("mechanism_id") or "")
                provider = str(payload.get("provider") or "")
                if mechanism_id and provider:
                    latest_admission.setdefault((mechanism_id, provider), payload)
            admissions = list(latest_admission.values())

        for spec in SOURCES:
            source_id = str(spec["id"])
            candidate = _table_candidate(db, spec, available)
            if candidate is not None:
                table_candidates[source_id] = candidate

    return available, direct, providers, admissions, table_candidates


def read_current_source_truth(
    store,
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, object]]:
    """Read current canonical source truth without provider calls or service construction.

    The dashboard uses the same persisted source surfaces as the canonical Source
    Coverage Plane: direct source observations, provider status/admission rows, and
    bounded durable-table fallbacks. This is presentation-only and never runs research
    or grants allocation/execution authority.
    """

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    try:
        available, direct, providers, admissions, table_candidates = _read_source_inputs(store)
    except Exception:
        return {}

    relevant_surfaces = {
        "source_coverage_observations",
        "provider_statuses",
        "provider_gap_admissions",
    } | _SAFE_TABLES
    if not (available & relevant_surfaces):
        return {}

    fallback = _fallback_freshness_seconds()
    result: dict[str, dict[str, object]] = {}

    for lane_id, lane_spec in LANES.items():
        required = {str(value) for value in list(lane_spec.get("required") or [])}
        covered: set[str] = set()
        admitted_groups: set[str] = set()
        admitted_source_ids: list[str] = []
        current_items = 0
        newest_admitted: datetime | None = None
        newest_seen: datetime | None = None
        stale_source_ids: list[str] = []
        seen_source_ids: list[str] = []

        for spec in SOURCES:
            lanes = {str(value) for value in list(spec.get("lanes") or [])}
            if lane_id not in lanes:
                continue

            source_id = str(spec["id"])
            classes = [str(value) for value in list(spec.get("classes") or [])]
            candidates: list[dict[str, object]] = []

            direct_row = direct.get((lane_id, source_id))
            if direct_row is not None:
                candidates.append(
                    _candidate(
                        observed_at=direct_row.get("observed_at"),
                        healthy=direct_row.get("healthy"),
                        item_count=direct_row.get("item_count"),
                        classes=[
                            str(value)
                            for value in list(
                                direct_row.get("evidence_classes") or spec.get("classes") or []
                            )
                        ],
                        authoritative=direct_row.get(
                            "authoritative", spec.get("authoritative", True)
                        ),
                        commercial=direct_row.get("commercial_use_permitted", True),
                        point_in_time=direct_row.get("point_in_time", True),
                        error_type=direct_row.get("error_type"),
                        source_reference=direct_row.get("source_reference"),
                    )
                )

            prefixes = [str(value) for value in list(spec.get("provider") or [])]
            if prefixes:
                for admission in admissions:
                    if str(admission.get("mechanism_id") or "") != lane_id:
                        continue
                    provider = str(admission.get("provider") or "")
                    if any(provider.startswith(prefix) for prefix in prefixes):
                        candidates.append(
                            _candidate(
                                observed_at=admission.get("observed_at"),
                                healthy=admission.get("healthy"),
                                item_count=admission.get("item_count"),
                                classes=classes,
                                authoritative=admission.get(
                                    "authoritative", spec.get("authoritative", True)
                                ),
                                commercial=admission.get("commercial_use_permitted", True),
                                point_in_time=admission.get("point_in_time", True),
                                error_type=admission.get("error_type"),
                                source_reference=admission.get("source_reference"),
                            )
                        )
                for provider_row in providers:
                    provider = str(provider_row.get("provider") or "")
                    if any(provider.startswith(prefix) for prefix in prefixes):
                        candidates.append(
                            _candidate(
                                observed_at=provider_row.get("observed_at"),
                                healthy=provider_row.get("ok"),
                                item_count=provider_row.get("item_count"),
                                classes=classes,
                                authoritative=bool(spec.get("authoritative", True)),
                                commercial=True,
                                point_in_time=True,
                                error_type=provider_row.get("error_type"),
                                source_reference=f"provider_status:{provider}",
                            )
                        )

            table_candidate = table_candidates.get(source_id)
            if table_candidate is not None:
                candidates.append(table_candidate)

            latest = _latest_candidate(candidates)
            if latest is None:
                continue

            seen_source_ids.append(source_id)
            observed_at = latest.get("observed_at")
            if isinstance(observed_at, datetime):
                if newest_seen is None or observed_at > newest_seen:
                    newest_seen = observed_at
            effective_classes = [str(value) for value in list(latest.get("classes") or classes)]
            max_age = evidence_freshness_seconds(
                effective_classes,
                fallback_seconds=fallback,
            )
            fresh = bool(
                isinstance(observed_at, datetime)
                and max(0.0, (current - observed_at).total_seconds()) <= max_age
            )
            governance_usable = bool(
                latest.get("healthy")
                and latest.get("authoritative")
                and latest.get("commercial")
                and latest.get("point_in_time")
            )
            admitted = governance_usable and fresh

            if governance_usable and not fresh:
                stale_source_ids.append(source_id)
            if not admitted:
                continue

            covered.update(effective_classes)
            admitted_source_ids.append(source_id)
            admitted_groups.add(str(spec.get("group") or source_id))
            try:
                current_items += max(1, int(latest.get("item_count") or 0))
            except (TypeError, ValueError):
                current_items += 1
            if isinstance(observed_at, datetime):
                if newest_admitted is None or observed_at > newest_admitted:
                    newest_admitted = observed_at

        missing = sorted(required - covered)
        evidence_complete = bool(required) and not missing
        connected = bool(admitted_source_ids)
        redundancy = len(admitted_groups) >= 2

        if connected:
            if not evidence_complete:
                source_state = "evidence_class_gap"
            elif not redundancy:
                source_state = "redundancy_gap"
            else:
                source_state = "sufficient"
        elif stale_source_ids:
            source_state = "stale"
        else:
            source_state = "provider_gap"

        result[lane_id] = {
            "lane_id": lane_id,
            "connected": connected,
            "provider_status": (
                "connected" if connected else ("stale" if source_state == "stale" else "missing")
            ),
            "evidence_complete": evidence_complete,
            "source_redundancy_satisfied": redundancy,
            "source_state": source_state,
            "covered_evidence_classes": sorted(covered),
            "missing_evidence_classes": missing,
            "independent_authoritative_source_count": len(admitted_groups),
            "admitted_source_ids": sorted(admitted_source_ids),
            "stale_source_ids": sorted(set(stale_source_ids)),
            "seen_source_ids": sorted(set(seen_source_ids)),
            "current_authoritative_item_count": current_items,
            "authoritative_observation_count": current_items,
            "observation_count_semantics": "current_admitted_source_items",
            "latest_authoritative_observation_at": (
                newest_admitted.isoformat() if newest_admitted else None
            ),
            "latest_seen_source_observation_at": (
                newest_seen.isoformat() if newest_seen else None
            ),
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }

    return result


def _source_stage(source_state: str, old_stage: str) -> str:
    if source_state == "provider_gap":
        return "waiting_for_source:provider_gap"
    if source_state == "stale":
        return "waiting_for_source:stale"
    if source_state == "evidence_class_gap":
        return "waiting_for_source:evidence_class_gap"
    if source_state == "redundancy_gap":
        return "forward_learning_active_redundancy_pending"
    if old_stage.startswith(_SOURCE_WAIT_PREFIX) or old_stage in {
        "forward_learning_active_redundancy_pending",
        "research_active_waiting_for_complete_forward_evidence",
    }:
        return "research_active_waiting_for_complete_forward_evidence"
    return old_stage


def _source_reason(lane: dict[str, object]) -> str:
    state = str(lane.get("source_state") or "provider_gap")
    if state == "provider_gap":
        return "no current admitted authoritative source is available"
    if state == "stale":
        return "authoritative source integration exists, but the latest usable evidence is stale"
    if state == "evidence_class_gap":
        missing = ", ".join(str(value) for value in lane.get("missing_evidence_classes") or [])
        return (
            "current authoritative sources are connected, but required evidence classes "
            f"remain incomplete: {missing or 'unspecified'}"
        )
    if state == "redundancy_gap":
        return (
            "current authoritative evidence is connected and complete for research, "
            "but independent-source redundancy remains pending"
        )
    return "current canonical authoritative source coverage is connected and sufficient"


def _source_next_action(lane: dict[str, object]) -> str:
    state = str(lane.get("source_state") or "provider_gap")
    if state == "provider_gap":
        return "connect or restore a current authoritative source"
    if state == "stale":
        return "refresh the admitted authoritative source evidence"
    if state == "evidence_class_gap":
        return "collect the missing canonical evidence classes"
    if state == "redundancy_gap":
        return "restore independent-source redundancy before allocation"
    return "continue downstream forward and statistical evidence collection"


def _resolve_card_truth(
    row: dict[str, Any],
    lane: dict[str, object],
) -> dict[str, Any]:
    resolved = dict(row)
    source_state = str(lane.get("source_state") or "provider_gap")
    connected = bool(lane.get("connected"))
    research_stale = bool(resolved.get("research_projection_stale"))
    operating_stale = bool(resolved.get("operating_projection_stale"))
    old_stage = str(resolved.get("stage") or "")
    old_state = str(resolved.get("state") or "")
    try:
        legacy_count = max(0, int(resolved.get("authoritative_observation_count") or 0))
    except (TypeError, ValueError):
        legacy_count = 0
    try:
        current_count = max(0, int(lane.get("current_authoritative_item_count") or 0))
    except (TypeError, ValueError):
        current_count = 0

    resolved["current_source_truth"] = lane
    resolved["source_state_authority"] = "canonical_current_source_truth"
    resolved["current_source_truth_presentation_only"] = True
    resolved["current_source_truth_allocation_authority"] = False
    resolved["current_source_truth_live_execution_authority"] = False

    # Current source truth owns provider/source fields. A database primary-key tail is
    # never allowed to masquerade as an observation count on the card.
    resolved["provider_ready"] = connected
    resolved["authoritative_observation_count"] = current_count
    resolved["authoritative_observation_count_semantics"] = "current_admitted_source_items"
    if legacy_count != current_count:
        resolved["legacy_projected_observation_count"] = legacy_count
        resolved["legacy_projected_observation_count_display_authority"] = False

    latest = lane.get("latest_authoritative_observation_at")
    if latest:
        resolved["authoritative_observation_last_at"] = latest

    resolved["stage"] = _source_stage(source_state, old_stage)
    if source_state == "provider_gap":
        if old_state not in _TERMINAL_RESEARCH_STATES:
            resolved["state"] = "provider_gap"
    elif source_state == "stale":
        if old_state == "provider_gap":
            resolved["state"] = "collecting"
    elif old_state == "provider_gap":
        resolved["state"] = "collecting"

    source_reason = _source_reason(lane)
    stale_parts: list[str] = []
    if research_stale:
        stale_parts.append("research runtime projection is stale")
    if operating_stale:
        stale_parts.append("operating certification snapshot is stale")
    if stale_parts:
        resolved["primary_reason"] = (
            f"{source_reason} · {'; '.join(stale_parts)}; downstream signal, forward, "
            "qualification, execution, settlement, and certification fields remain fail-closed"
        )
        resolved["next_action"] = (
            "restore successful current research publication; "
            + _source_next_action(lane)
            + "; do not relax qualification or execution thresholds"
        )
    else:
        original_reason = str(resolved.get("primary_reason") or "")
        lower_reason = original_reason.lower()
        contradictory = connected and any(
            phrase in lower_reason
            for phrase in (
                "provider gap",
                "required authoritative input evidence is not currently available",
                "no fresh admitted authoritative provider",
                "restore or connect the required authoritative provider",
            )
        )
        if contradictory or old_state == "provider_gap":
            resolved["primary_reason"] = source_reason
            resolved["next_action"] = _source_next_action(lane)

    resolved["card_truth"] = {
        "source_status": (
            "connected"
            if source_state == "sufficient"
            else (
                "incomplete"
                if source_state == "evidence_class_gap"
                else (
                    "redundancy_pending" if source_state == "redundancy_gap" else source_state
                )
            )
        ),
        "provider_status": lane.get("provider_status"),
        "research_status": "stale" if research_stale else "current",
        "operating_status": "stale" if operating_stale else "current",
        "current_authoritative_item_count": current_count,
        "observation_count_semantics": "current_admitted_source_items",
        "forward_outcome_count": resolved.get("independent_forward_outcome_count"),
        "qualified_count": resolved.get("current_statistically_qualified_count"),
        "promoted_count": resolved.get("current_promoted_count"),
        "settled_count": resolved.get("settled_allocator_outcome_count"),
        "certified": bool(
            resolved.get("profitability_certified")
            or str(resolved.get("state") or "") == "certified"
        ),
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
    return resolved


def overlay_dashboard_source_truth(
    store,
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Resolve every dashboard card from one current source-truth contract.

    Source coverage owns provider/source status and displayed current evidence items.
    Persisted research still owns signals, forward outcomes, qualification, promotion,
    settlement, and certification. Stale research remains fail-closed and is surfaced
    independently instead of corrupting provider status.
    """

    truth = read_current_source_truth(store, now=now)
    if not truth:
        return payload

    result = dict(payload)
    mechanisms = result.get("mechanisms")
    if not isinstance(mechanisms, dict):
        result["current_source_truth"] = truth
        return result

    mechanism_payload = dict(mechanisms)
    rows = mechanism_payload.get("mechanisms")
    if not isinstance(rows, list):
        result["current_source_truth"] = truth
        return result

    updated: list[object] = []
    for raw in rows:
        if not isinstance(raw, dict):
            updated.append(raw)
            continue
        lane_id = str(raw.get("mechanism_id") or "")
        lane = truth.get(lane_id)
        updated.append(_resolve_card_truth(raw, lane) if lane is not None else dict(raw))

    mechanism_payload["mechanisms"] = updated
    mechanism_payload["current_source_truth_overlay"] = True
    mechanism_payload["current_source_truth_authority"] = "canonical_current_source_truth"
    mechanism_payload["card_truth_resolver"] = "v2"
    result["mechanisms"] = mechanism_payload
    result["current_source_truth"] = truth
    result["current_source_truth_overlay"] = True
    result["current_source_truth_presentation_only"] = True
    result["card_truth_resolver"] = "v2"
    return result
