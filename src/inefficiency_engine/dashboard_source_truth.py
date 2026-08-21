from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text

from inefficiency_engine.evidence_velocity import evidence_freshness_seconds
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


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


def _source_specs() -> dict[str, dict[str, object]]:
    return {str(row["id"]): row for row in SOURCES}


def _fallback_freshness_seconds() -> float:
    try:
        hours = float(os.getenv("CIE_SOURCE_COVERAGE_MAX_AGE_HOURS", "24"))
    except ValueError:
        hours = 24.0
    return max(60.0, hours * 3600.0)


def read_current_source_truth(store, *, now: datetime | None = None) -> dict[str, dict[str, object]]:
    """Read current canonical source-coverage observations without constructing services.

    The dashboard request path must stay bounded. This reader therefore consumes only
    the append-only ``source_coverage_observations`` ledger already published by the
    source plane. It never calls providers, creates schema, runs research, or grants
    allocation/execution authority.
    """

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    try:
        if "source_coverage_observations" not in set(inspect(store.engine).get_table_names()):
            return {}
        with store.engine.begin() as db:
            if getattr(store, "backend", "") == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '1000ms'"))
                db.execute(text("SET LOCAL lock_timeout = '300ms'"))
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM source_coverage_observations "
                        "ORDER BY id DESC LIMIT 3000"
                    )
                ).scalars()
            )
    except Exception:
        return {}

    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in raws:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        lane_id = str(payload.get("lane_id") or "")
        source_id = str(payload.get("source_id") or "")
        if lane_id not in LANES or not source_id:
            continue
        latest.setdefault((lane_id, source_id), payload)

    specs = _source_specs()
    fallback = _fallback_freshness_seconds()
    result: dict[str, dict[str, object]] = {}
    for lane_id, lane_spec in LANES.items():
        required = {str(value) for value in list(lane_spec.get("required") or [])}
        covered: set[str] = set()
        admitted_groups: set[str] = set()
        admitted_source_ids: list[str] = []
        authoritative_items = 0
        newest: datetime | None = None

        for (observed_lane, source_id), payload in latest.items():
            if observed_lane != lane_id:
                continue
            source_spec = specs.get(source_id, {})
            classes = [
                str(value)
                for value in list(payload.get("evidence_classes") or source_spec.get("classes") or [])
            ]
            observed_at = _parse_time(payload.get("observed_at"))
            max_age = evidence_freshness_seconds(classes, fallback_seconds=fallback)
            fresh = bool(
                observed_at is not None
                and max(0.0, (current - observed_at).total_seconds()) <= max_age
            )
            admitted = bool(
                payload.get("healthy")
                and payload.get("authoritative", True)
                and payload.get("commercial_use_permitted", True)
                and payload.get("point_in_time", True)
                and fresh
            )
            if not admitted:
                continue

            covered.update(classes)
            admitted_source_ids.append(source_id)
            group = str(source_spec.get("group") or source_id)
            admitted_groups.add(group)
            try:
                authoritative_items += max(1, int(payload.get("item_count") or 0))
            except (TypeError, ValueError):
                authoritative_items += 1
            if observed_at is not None and (newest is None or observed_at > newest):
                newest = observed_at

        missing = sorted(required - covered)
        evidence_complete = bool(required) and not missing
        connected = bool(admitted_source_ids)
        redundancy = len(admitted_groups) >= 2
        if not connected:
            source_state = "provider_gap"
        elif not evidence_complete:
            source_state = "evidence_class_gap"
        elif not redundancy:
            source_state = "redundancy_gap"
        else:
            source_state = "sufficient"

        result[lane_id] = {
            "lane_id": lane_id,
            "connected": connected,
            "evidence_complete": evidence_complete,
            "source_redundancy_satisfied": redundancy,
            "source_state": source_state,
            "covered_evidence_classes": sorted(covered),
            "missing_evidence_classes": missing,
            "independent_authoritative_source_count": len(admitted_groups),
            "admitted_source_ids": sorted(admitted_source_ids),
            "authoritative_observation_count": authoritative_items,
            "latest_authoritative_observation_at": newest.isoformat() if newest else None,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }
    return result


def overlay_dashboard_source_truth(
    store,
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Overlay current canonical source truth onto stale mechanism presentation rows.

    Only source/presentation fields may advance. Statistical qualification, promoted
    counts, settlement, certification, sizing, allocation, and execution authority are
    never changed. A stale research projection therefore remains fail-closed while the
    card can still truthfully say that current authoritative sources are connected.
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
        row = dict(raw)
        lane_id = str(row.get("mechanism_id") or "")
        lane = truth.get(lane_id)
        if lane is None:
            updated.append(row)
            continue

        row["current_source_truth"] = lane
        row["source_state_authority"] = "canonical_source_coverage_observations"
        row["current_source_truth_presentation_only"] = True
        row["current_source_truth_allocation_authority"] = False
        row["current_source_truth_live_execution_authority"] = False
        row["provider_ready"] = bool(lane["connected"])
        row["authoritative_observation_count"] = max(
            int(row.get("authoritative_observation_count") or 0),
            int(lane.get("authoritative_observation_count") or 0),
        )
        if lane.get("latest_authoritative_observation_at"):
            row["authoritative_observation_last_at"] = lane["latest_authoritative_observation_at"]

        # Repair only an obsolete source-gap presentation state. Do not overwrite a
        # statistical/economic/execution failure produced by the research engine.
        if bool(lane["connected"]) and str(row.get("state") or "") == "provider_gap":
            source_state = str(lane["source_state"])
            row["state"] = "collecting"
            if source_state == "evidence_class_gap":
                row["stage"] = "waiting_for_source:evidence_class_gap"
                row["primary_reason"] = (
                    "current authoritative source evidence is connected, but required evidence "
                    "classes are incomplete: " + ", ".join(lane.get("missing_evidence_classes") or [])
                )
                row["next_action"] = (
                    "collect the missing canonical evidence classes; keep downstream qualification fail-closed"
                )
            elif source_state == "redundancy_gap":
                row["stage"] = "forward_learning_active_redundancy_pending"
                row["primary_reason"] = (
                    "current authoritative source evidence is connected and research-complete; "
                    "independent-source redundancy remains pending"
                )
                row["next_action"] = (
                    "continue forward learning and restore independent-source redundancy before allocation"
                )
            else:
                row["stage"] = "research_active_waiting_for_complete_forward_evidence"
                row["primary_reason"] = (
                    "current canonical source coverage is connected; downstream forward/statistical "
                    "qualification remains independently gated"
                )
                row["next_action"] = (
                    "continue forward evidence collection under unchanged qualification thresholds"
                )
        updated.append(row)

    mechanism_payload["mechanisms"] = updated
    mechanism_payload["current_source_truth_overlay"] = True
    mechanism_payload["current_source_truth_authority"] = "source_coverage_observations"
    result["mechanisms"] = mechanism_payload
    result["current_source_truth"] = truth
    result["current_source_truth_overlay"] = True
    result["current_source_truth_presentation_only"] = True
    return result
