from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from inefficiency_engine.dashboard_source_connectivity import read_source_connectivity
from inefficiency_engine.source_coverage_catalog import LANES


def _utc(value: object | None) -> datetime | None:
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


def _newest(values: list[object | None]) -> datetime | None:
    parsed = [dt for dt in (_utc(value) for value in values) if dt is not None]
    return max(parsed) if parsed else None


def read_current_source_truth(
    store,
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, object]]:
    """Build lane source truth from the stable per-source connectivity read model.

    Source Connectivity already owns the bounded history semantics required for a
    truthful UI: the latest failed acquisition attempt is kept separate from the
    newest still-fresh usable observation. Mechanism cards must consume that same
    read model rather than independently collapsing each provider to its newest
    attempt, otherwise a transient timeout makes source counts disappear and then
    reappear on the next successful refresh.

    This adapter performs no provider calls, changes no evidence TTLs, and has no
    allocation or execution authority. It only aggregates the already-resolved
    per-source rows into the lane-level contract expected by the card resolver.
    """

    payload = read_source_connectivity(store, now=now)
    rows = [dict(row) for row in list(payload.get("sources") or []) if isinstance(row, dict)]
    if not rows:
        return {}

    result: dict[str, dict[str, object]] = {}
    diagnostic_read_degraded = bool(payload.get("diagnostic_read_degraded"))
    served_last_successful_snapshot = bool(payload.get("served_last_successful_snapshot"))

    for lane_id, lane_spec in LANES.items():
        required = {str(value) for value in list(lane_spec.get("required") or [])}
        lane_rows = [
            row
            for row in rows
            if lane_id in {str(value) for value in list(row.get("lane_ids") or [])}
        ]

        admitted_rows = [row for row in lane_rows if bool(row.get("admitted"))]
        covered: set[str] = set()
        admitted_groups: set[str] = set()
        admitted_source_ids: list[str] = []
        current_items = 0

        for row in admitted_rows:
            source_id = str(row.get("source_id") or "")
            if source_id:
                admitted_source_ids.append(source_id)
            covered.update(str(value) for value in list(row.get("classes") or []))
            admitted_groups.add(str(row.get("group") or source_id))
            try:
                current_items += max(1, int(row.get("item_count") or 0))
            except (TypeError, ValueError):
                current_items += 1

        stale_source_ids = sorted(
            {
                str(row.get("source_id") or "")
                for row in lane_rows
                if row.get("state") == "stale" and row.get("source_id")
            }
        )
        seen_source_ids = sorted(
            {
                str(row.get("source_id") or "")
                for row in lane_rows
                if row.get("source_id")
                and (
                    row.get("observed_at")
                    or row.get("latest_attempt_observed_at")
                    or row.get("state") not in {"unobserved", "credential_required"}
                )
            }
        )
        refresh_degraded_source_ids = sorted(
            {
                str(row.get("source_id") or "")
                for row in lane_rows
                if bool(row.get("refresh_degraded")) and row.get("source_id")
            }
        )
        latest_refresh_errors = {
            str(row.get("source_id")): str(row.get("latest_attempt_error_type"))
            for row in lane_rows
            if row.get("source_id") and row.get("latest_attempt_error_type")
        }

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

        newest_admitted = _newest([row.get("observed_at") for row in admitted_rows])
        newest_seen = _newest(
            [
                row.get("latest_attempt_observed_at") or row.get("observed_at")
                for row in lane_rows
            ]
        )

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
            "covered_evidence_class_count": len(covered),
            "required_evidence_class_count": len(required),
            "independent_authoritative_source_count": len(admitted_groups),
            "current_authoritative_source_count": len(admitted_source_ids),
            "admitted_source_ids": sorted(admitted_source_ids),
            "stale_source_ids": stale_source_ids,
            "seen_source_ids": seen_source_ids,
            "refresh_degraded_source_ids": refresh_degraded_source_ids,
            "source_refresh_degraded": bool(refresh_degraded_source_ids),
            "latest_refresh_error_types": latest_refresh_errors,
            "current_authoritative_item_count": current_items,
            "authoritative_observation_count": current_items,
            "observation_count_semantics": "current_admitted_source_items_diagnostic_only",
            "latest_authoritative_observation_at": (
                newest_admitted.isoformat() if newest_admitted else None
            ),
            "latest_seen_source_observation_at": (
                newest_seen.isoformat() if newest_seen else None
            ),
            "diagnostic_read_degraded": diagnostic_read_degraded,
            "served_last_successful_snapshot": served_last_successful_snapshot,
            "source_truth_model": "stable_connectivity_history_v1",
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }

    return result


__all__ = ["read_current_source_truth"]
