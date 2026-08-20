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
    connected authoritative surface only; it is never strategy qualification or
    allocation authority.
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
    """Correct stale provider-gap presentation from the admission ledger.

    A fresh admitted provider may move a stale lane from PROVIDER GAP to COLLECTING.
    It cannot move a lane to certifying/certified, create an economic candidate,
    increment a forward sample, or grant paper/live allocation authority.
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

        row["provider_admission"] = status
        admitted_count = int(status.get("admitted_provider_count") or 0)
        if admitted_count > 0:
            row["provider_ready"] = True
            row["authoritative_observation_count"] = max(
                int(row.get("authoritative_observation_count") or 0),
                admitted_count,
            )
            if row.get("state") == "provider_gap":
                row["state"] = "collecting"
                row["primary_reason"] = (
                    "authoritative provider is connected and fresh; economic/forward evidence "
                    "is now accumulating under the existing qualification gates"
                )
                row["next_action"] = (
                    "continue point-in-time economic and forward evidence collection; do not lower "
                    "profitability, statistical, execution, or settlement thresholds"
                )
        else:
            # If this provider surface has actually been probed, its newest durable
            # admission is a more current provider-health fact than an older dashboard
            # projection. Keep the economic lane fail-closed and expose the real probe.
            row["provider_ready"] = False
            row["state"] = "provider_gap"
            row["primary_reason"] = (
                "provider is configured but no fresh admitted provider observation is currently healthy"
            )
            row["next_action"] = (
                "restore the failing/stale provider probe; economic and forward gates remain unchanged"
            )
        row["provider_readiness_reconciled_from_admission_ledger"] = True
        row["provider_readiness_presentation_only"] = True
        mechanisms.append(row)

    result["mechanisms"] = mechanisms
    result["provider_readiness_reconciled"] = bool(readiness)
    result["provider_readiness_source"] = "provider_gap_admissions"
    result["provider_readiness_presentation_only"] = True
    return result
