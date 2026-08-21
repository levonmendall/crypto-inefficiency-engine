from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text


PROVIDER_DEPENDENT_MECHANISMS = {
    "fundamental_onchain",
    "event_driven",
    "yield",
    "volatility",
    "liquidation_distress",
}
DEFAULT_PROVIDER_MAX_AGE_HOURS = 24.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: object | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _latest_provider_rows(store) -> dict[str, list[dict[str, Any]]]:
    """Read the newest durable admission for each mechanism/provider pair.

    This is deliberately read-only and bounded. Provider admission is evidence of a
    connected authoritative surface only; it is never strategy qualification,
    source-sufficiency authority, or allocation authority.
    """
    if "provider_gap_admissions" not in set(inspect(store.engine).get_table_names()):
        return {}

    with store.engine.connect() as db:
        raws = list(
            db.execute(
                text(
                    "SELECT payload_json FROM provider_gap_admissions "
                    "ORDER BY id DESC LIMIT 500"
                )
            ).scalars()
        )

    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in raws:
        try:
            payload = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        mechanism_id = str(payload.get("mechanism_id") or "")
        provider = str(payload.get("provider") or "")
        if mechanism_id not in PROVIDER_DEPENDENT_MECHANISMS or not provider:
            continue
        latest.setdefault((mechanism_id, provider), payload)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for (mechanism_id, _), payload in latest.items():
        grouped.setdefault(mechanism_id, []).append(payload)
    return grouped


def provider_readiness_snapshot(
    store,
    *,
    now: datetime | None = None,
    max_age_hours: float = DEFAULT_PROVIDER_MAX_AGE_HOURS,
) -> dict[str, dict[str, Any]]:
    current = now or _now()
    max_age = max(0.0, float(max_age_hours))
    grouped = _latest_provider_rows(store)
    result: dict[str, dict[str, Any]] = {}

    for mechanism_id, rows in grouped.items():
        providers: list[dict[str, Any]] = []
        for row in rows:
            observed_at = _parse_time(row.get("observed_at"))
            age_hours = (
                max(0.0, (current - observed_at).total_seconds() / 3600.0)
                if observed_at is not None
                else None
            )
            governance_admitted = bool(
                row.get("healthy")
                and row.get("authoritative", True)
                and row.get("commercial_use_permitted", True)
                and row.get("point_in_time", True)
            )
            fresh = age_hours is not None and age_hours <= max_age
            providers.append(
                {
                    "provider": row.get("provider"),
                    "observed_at": observed_at.isoformat() if observed_at is not None else None,
                    "healthy": bool(row.get("healthy")),
                    "item_count": int(row.get("item_count") or 0),
                    "admitted": bool(governance_admitted and fresh),
                    "fresh": fresh,
                    "age_hours": age_hours,
                    "error_type": row.get("error_type"),
                    "source_reference": row.get("source_reference"),
                }
            )
        providers.sort(key=lambda item: str(item.get("provider") or ""))
        result[mechanism_id] = {
            "mechanism_id": mechanism_id,
            "admitted_provider_count": sum(bool(row["admitted"]) for row in providers),
            "providers": providers,
            "source": "provider_gap_admissions",
            "max_age_hours": max_age,
            "paper_only": True,
            "allocation_authority_unchanged": True,
            "live_execution_authority": False,
        }
    return result


def reconcile_provider_readiness(
    store,
    mechanism_payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Attach legacy provider-probe telemetry without changing lane source truth.

    The canonical 13-lane Source Coverage Plane is the sole authority for displayed
    source state. Its persisted operating row owns ``state``, ``stage``,
    ``provider_ready``, source-sufficiency reasons, blockers, and authoritative
    observation counts. The older provider-admission ledger is intentionally narrower:
    it covers only five provider-dependent mechanisms and can disagree when an
    alternate authoritative source is carrying a lane.

    The research-closure plane also publishes a ``provider_admission`` object with a
    different schema. Preserve that object verbatim. Legacy probe telemetry is exposed
    under ``legacy_provider_admission`` so presentation code can inspect it without a
    field collision or accidental source-state override.

    A fresh legacy probe cannot close a canonical source gap, and a failed or stale
    legacy probe cannot reopen one. Qualification, allocation, and execution authority
    remain unchanged and fail closed through the canonical source plane.
    """
    readiness = provider_readiness_snapshot(store, now=now)
    result = dict(mechanism_payload or {})
    mechanisms: list[dict[str, Any]] = []

    for source in list(result.get("mechanisms") or []):
        if not isinstance(source, dict):
            continue
        row = dict(source)
        mechanism_id = str(row.get("mechanism_id") or "")
        status = readiness.get(mechanism_id)
        if status is None:
            mechanisms.append(row)
            continue

        row["legacy_provider_admission"] = status
        row["provider_admission_ready"] = bool(
            int(status.get("admitted_provider_count") or 0) > 0
        )
        row["provider_readiness_reconciled_from_admission_ledger"] = True
        row["provider_readiness_presentation_only"] = True
        row["provider_readiness_state_override_applied"] = False
        row["source_state_authority"] = "canonical_13_lane_source_coverage"
        mechanisms.append(row)

    result["mechanisms"] = mechanisms
    result["provider_readiness_reconciled"] = bool(readiness)
    result["provider_readiness_source"] = "provider_gap_admissions"
    result["provider_readiness_presentation_only"] = True
    result["provider_readiness_state_override_applied"] = False
    result["source_state_authority"] = "canonical_13_lane_source_coverage"
    return result
