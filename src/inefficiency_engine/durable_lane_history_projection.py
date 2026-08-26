from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from inefficiency_engine.candidate_observatory_historical_replay import (
    REPLAY_WORKER_ID,
    replay_start_from_env,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.durable_lane_history import build_durable_lane_history
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_catalog import LANES


DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID = "durable-lane-history-projection"
MATERIALIZED_HEARTBEAT_LIMIT = 64


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _iso_min(left: object | None, right: object | None) -> str | None:
    values = [value for value in (_parse_time(left), _parse_time(right)) if value is not None]
    return min(values).isoformat() if values else None


def _iso_max(left: object | None, right: object | None) -> str | None:
    values = [value for value in (_parse_time(left), _parse_time(right)) if value is not None]
    return max(values).isoformat() if values else None


def _latest_materialized_lane_coverage(store: Any) -> tuple[dict[str, object] | None, dict[str, object]]:
    """Read one previously materialized strict-history aggregate from worker heartbeats."""

    meta: dict[str, object] = {
        "available": False,
        "heartbeat_observed_at": None,
        "replay_start": None,
        "replay_boundary": None,
    }
    query = (
        select(
            store.worker_heartbeats.c.observed_at,
            store.worker_heartbeats.c.payload_json,
        )
        .where(store.worker_heartbeats.c.worker_id == REPLAY_WORKER_ID)
        .order_by(store.worker_heartbeats.c.id.desc())
        .limit(MATERIALIZED_HEARTBEAT_LIMIT)
    )
    with store.engine.connect() as db:
        rows = list(db.execute(query).mappings())

    for row in rows:
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except (TypeError, json.JSONDecodeError):
            continue
        detail = payload.get("detail") if isinstance(payload, dict) else None
        if not isinstance(detail, dict):
            continue
        coverage = detail.get("lane_coverage")
        lanes = coverage.get("lanes") if isinstance(coverage, dict) else None
        if not isinstance(lanes, dict):
            continue
        meta.update(
            {
                "available": True,
                "heartbeat_observed_at": row.get("observed_at"),
                "replay_start": detail.get("replay_start"),
                "replay_boundary": detail.get("replay_boundary")
                or detail.get("live_observatory_started_at"),
            }
        )
        return dict(coverage), meta
    return None, meta


def merge_materialized_lane_coverage(
    history: dict[str, object],
    coverage: dict[str, object] | None,
) -> dict[str, object]:
    """Union certifier evidence into total history without double-counting overlap.

    The certifier aggregate can overlap the first canonical source snapshot and cannot be
    sliced safely. For total diagnostic history, overlap is handled by unioning evidence
    classes/source IDs and taking the maximum source/operating counts rather than adding
    them. This recovers truthful evidence presence while preventing duplicate counts.
    Nothing here creates candidate, forward, qualification, allocation or execution
    authority.
    """

    if not isinstance(coverage, dict):
        return history
    coverage_lanes = coverage.get("lanes")
    history_lanes = history.get("lanes")
    if not isinstance(coverage_lanes, dict) or not isinstance(history_lanes, dict):
        return history

    merged_lane_count = 0
    for lane_id, source_row in coverage_lanes.items():
        lane_id = str(lane_id)
        target = history_lanes.get(lane_id)
        if lane_id not in LANES or not isinstance(source_row, dict) or not isinstance(target, dict):
            continue

        required = {str(value) for value in list(LANES[lane_id].get("required") or [])}
        classes = {
            str(value)
            for value in list(target.get("historical_evidence_classes") or [])
            if str(value)
        }
        classes.update(
            str(value)
            for value in list(source_row.get("historical_evidence_classes") or [])
            if str(value)
        )
        source_ids = {
            str(value) for value in list(target.get("source_ids") or []) if str(value)
        }
        source_ids.update(
            str(value) for value in list(source_row.get("source_ids") or []) if str(value)
        )
        ledgers = {
            str(value) for value in list(target.get("source_ledgers") or []) if str(value)
        }
        ledgers.update(
            str(value)
            for value in list(source_row.get("source_ledgers") or [])
            if str(value)
        )
        if classes or source_ids or int(source_row.get("recovered_source_observations") or 0):
            ledgers.add("candidate_observatory_lane_coverage_heartbeat")

        source_count = max(
            int(target.get("recovered_source_observations") or 0),
            int(source_row.get("recovered_source_observations") or 0),
        )
        operating_count = max(
            int(target.get("recovered_operating_snapshots") or 0),
            int(source_row.get("recovered_operating_snapshots") or 0),
        )
        recovered_count = len(required & classes)
        target.update(
            {
                "history_available": bool(
                    target.get("history_available")
                    or classes
                    or source_ids
                    or source_count
                    or operating_count
                    or source_row.get("earliest_recovered_at")
                    or source_row.get("latest_recovered_at")
                ),
                "evidence_class_history_complete": bool(required and not (required - classes)),
                "required_evidence_class_count": len(required),
                "recovered_evidence_class_count": recovered_count,
                "evidence_class_fill_ratio": (
                    float(recovered_count) / float(len(required)) if required else 1.0
                ),
                "recovered_source_observations": source_count,
                "recovered_operating_snapshots": operating_count,
                "earliest_recovered_at": _iso_min(
                    target.get("earliest_recovered_at"),
                    source_row.get("earliest_recovered_at"),
                ),
                "latest_recovered_at": _iso_max(
                    target.get("latest_recovered_at"),
                    source_row.get("latest_recovered_at"),
                ),
                "historical_evidence_classes": sorted(classes),
                "missing_historical_evidence_classes": sorted(required - classes),
                "source_ids": sorted(source_ids),
                "source_ledgers": sorted(ledgers),
                "candidate_level_history_synthesized": False,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            }
        )
        merged_lane_count += 1

    available = sum(
        bool(row.get("history_available"))
        for row in history_lanes.values()
        if isinstance(row, dict)
    )
    complete = sum(
        bool(row.get("evidence_class_history_complete"))
        for row in history_lanes.values()
        if isinstance(row, dict)
    )
    history["lanes_with_durable_history"] = available
    history["lanes_without_durable_history"] = len(LANES) - available
    history["lanes_with_all_required_evidence_classes"] = complete
    history["materialized_overlap_safe_merge"] = True
    history["materialized_overlap_safe_merged_lane_count"] = merged_lane_count
    history["raw_history_reconstruction_on_http"] = False
    return history


def build_projection(store: Any) -> dict[str, object]:
    history = build_durable_lane_history(store, start=replay_start_from_env())
    coverage: dict[str, object] | None = None
    meta: dict[str, object]
    try:
        coverage, meta = _latest_materialized_lane_coverage(store)
        merge_materialized_lane_coverage(history, coverage)
    except Exception as exc:
        meta = {
            "available": False,
            "error_type": type(exc).__name__,
            "heartbeat_observed_at": None,
            "replay_start": None,
            "replay_boundary": None,
        }
        errors = history.setdefault("read_errors", [])
        if isinstance(errors, list):
            errors.append(
                {"stage": "projection_materialized_lane_coverage", "error_type": type(exc).__name__}
            )
        history["read_degraded"] = True

    history["projection_materialized_coverage"] = meta
    history["projection_observed_at"] = datetime.now(timezone.utc).isoformat()
    history["projection_worker_id"] = DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID
    history["candidate_level_history_synthesized"] = False
    history["historical_counts_as_forward"] = False
    history["qualification_authority"] = False
    history["allocation_authority"] = False
    history["live_execution_authority"] = False
    history["paper_only"] = True
    return history


def _record(store: Any, *, state: str, detail: dict[str, object], error_type: str | None = None) -> None:
    try:
        store.record_worker_heartbeat(
            worker_id=DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={
                **detail,
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "candidate_level_history_synthesized": False,
                "historical_counts_as_forward": False,
                "qualification_thresholds_unchanged": True,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("durable lane history projection requires durable persistence")
    try:
        history = build_projection(store)
    except Exception as exc:
        _record(
            store,
            state="degraded",
            error_type=type(exc).__name__,
            detail={
                "stage": "projection_failed",
                "message": str(exc)[:1000],
                "retrying": True,
            },
        )
        raise

    _record(
        store,
        state="degraded" if bool(history.get("read_degraded")) else "success",
        detail={
            "stage": "projection_ready",
            "history": history,
            "lanes_with_durable_history": history.get("lanes_with_durable_history"),
            "lanes_with_all_required_evidence_classes": history.get(
                "lanes_with_all_required_evidence_classes"
            ),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID",
    "build_projection",
    "merge_materialized_lane_coverage",
]
