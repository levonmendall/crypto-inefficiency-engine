from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import MetaData, Table, inspect, select

from inefficiency_engine.candidate_observatory_historical_replay import (
    REPLAY_WORKER_ID,
    replay_start_from_env,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_catalog import LANES


COVERAGE_COMPLETE_EXIT_CODE = 3
COVERAGE_INCOMPLETE_EXIT_CODE = 4
DEFAULT_EDGE_TOLERANCE = timedelta(hours=12)
REPLAY_TABLE = "candidate_observatory_historical_replay"


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _utc(parsed)


def _coverage_row(lane_id: str) -> dict[str, object]:
    return {
        "lane_id": lane_id,
        "lane_name": str(LANES[lane_id].get("name") or lane_id),
        "required": True,
        "state": "unavailable",
        "earliest_recovered_at": None,
        "latest_recovered_at": None,
        "recovered_funnel_records": 0,
        "source_ledgers": [],
        "reconstruction_quality": "unrecoverable_from_current_replay_sources",
        "candidate_level_rejections_reconstructable": False,
        "omitted_untrusted_records": 0,
        "reason": "no trusted persisted historical funnel has been recovered for this required lane",
    }


def summarize_lane_coverage(
    records: list[dict[str, object]],
    *,
    start: datetime,
    boundary: datetime,
    edge_tolerance: timedelta = DEFAULT_EDGE_TOLERANCE,
) -> dict[str, object]:
    """Build a fail-closed coverage contract over all canonical profit lanes.

    A lane is complete only when trusted persisted funnel evidence exists near both
    edges of the replay interval. Merely draining the implemented replay streams is
    never sufficient. Missing and untrusted legacy evidence stays visible instead of
    being interpreted as zero opportunity activity.
    """

    start = _utc(start)
    boundary = _utc(boundary)
    tolerance = max(timedelta(0), edge_tolerance)
    rows = {lane_id: _coverage_row(lane_id) for lane_id in LANES}
    seen_times: dict[str, list[datetime]] = {lane_id: [] for lane_id in LANES}
    sources: dict[str, set[str]] = {lane_id: set() for lane_id in LANES}
    omitted: dict[str, int] = {lane_id: 0 for lane_id in LANES}

    for record in records:
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else record
        if not isinstance(payload, dict):
            continue
        observed_at = _parse_time(record.get("observed_at") or payload.get("observed_at"))
        if observed_at is None or observed_at < start or observed_at >= boundary:
            continue
        funnels = payload.get("funnels")
        if isinstance(funnels, dict):
            source = str(record.get("source_table") or payload.get("reconstruction_method") or "unknown")
            for lane_id in funnels:
                lane_id = str(lane_id)
                if lane_id not in rows:
                    continue
                seen_times[lane_id].append(observed_at)
                sources[lane_id].add(source)
        omitted_lanes = payload.get("omitted_untrusted_legacy_funnels")
        if isinstance(omitted_lanes, list):
            for lane_id in omitted_lanes:
                lane_id = str(lane_id)
                if lane_id in omitted:
                    omitted[lane_id] += 1

    for lane_id, row in rows.items():
        times = seen_times[lane_id]
        row["omitted_untrusted_records"] = omitted[lane_id]
        if not times:
            if omitted[lane_id]:
                row["state"] = "partial"
                row["reconstruction_quality"] = "untrusted_legacy_only"
                row["reason"] = "legacy funnel records exist but fail the trusted historical reconstruction contract"
            continue

        earliest = min(times)
        latest = max(times)
        row["earliest_recovered_at"] = earliest.isoformat()
        row["latest_recovered_at"] = latest.isoformat()
        row["recovered_funnel_records"] = len(times)
        row["source_ledgers"] = sorted(sources[lane_id])
        row["reconstruction_quality"] = "exact_aggregate"

        covers_start = earliest <= start + tolerance
        covers_boundary = latest >= boundary - tolerance
        trusted = omitted[lane_id] == 0
        if covers_start and covers_boundary and trusted:
            row["state"] = "complete"
            row["reason"] = "trusted persisted aggregate funnel evidence covers both replay edges"
        else:
            row["state"] = "partial"
            reasons: list[str] = []
            if not covers_start:
                reasons.append("recovered history starts after the replay-start tolerance")
            if not covers_boundary:
                reasons.append("recovered history stops before the live-boundary tolerance")
            if not trusted:
                reasons.append("untrusted legacy funnel records were omitted")
            row["reason"] = "; ".join(reasons) or "historical coverage is incomplete"

    state_counts = {state: 0 for state in ("complete", "partial", "unavailable", "not_applicable")}
    for row in rows.values():
        state = str(row["state"])
        state_counts[state] = state_counts.get(state, 0) + 1
    required_complete = all(row["state"] == "complete" for row in rows.values())
    return {
        "required_lane_count": len(rows),
        "complete_lane_count": state_counts.get("complete", 0),
        "partial_lane_count": state_counts.get("partial", 0),
        "unavailable_lane_count": state_counts.get("unavailable", 0),
        "not_applicable_lane_count": state_counts.get("not_applicable", 0),
        "required_lanes_complete": required_complete,
        "coverage_contract": "all_required_lanes_must_have_trusted_edge_to_edge_historical_funnel_evidence",
        "edge_tolerance_seconds": int(tolerance.total_seconds()),
        "lanes": rows,
    }


def _read_replay_records(store) -> list[dict[str, object]]:
    try:
        if REPLAY_TABLE not in set(inspect(store.engine).get_table_names()):
            return []
    except Exception:
        return []
    table = Table(REPLAY_TABLE, MetaData(), autoload_with=store.engine)
    with store.engine.connect() as db:
        raw_rows = list(
            db.execute(
                select(
                    table.c.record_type,
                    table.c.source_table,
                    table.c.observed_at,
                    table.c.payload_json,
                ).where(table.c.record_type.in_(("alpha_funnel", "structural_funnel")))
            ).mappings()
        )
    records: list[dict[str, object]] = []
    for row in raw_rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        records.append(
            {
                "record_type": row["record_type"],
                "source_table": row["source_table"],
                "observed_at": row["observed_at"],
                "payload": payload,
            }
        )
    return records


def certify_lane_coverage(store) -> dict[str, object]:
    """Persist the authoritative post-replay lane certification heartbeat."""

    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(REPLAY_WORKER_ID)
    except Exception:
        heartbeat = None
    previous_detail = getattr(heartbeat, "detail", {}) if heartbeat is not None else {}
    if not isinstance(previous_detail, dict):
        previous_detail = {}

    start = _parse_time(previous_detail.get("replay_start")) or replay_start_from_env()
    boundary = _parse_time(previous_detail.get("replay_boundary"))
    live_start = _parse_time(previous_detail.get("live_observatory_started_at"))
    if boundary is None:
        boundary = live_start or datetime.now(timezone.utc)

    stream_complete = bool(previous_detail.get("complete")) and live_start is not None
    lane_coverage = summarize_lane_coverage(
        _read_replay_records(store),
        start=start,
        boundary=boundary,
    )
    complete = bool(stream_complete and lane_coverage["required_lanes_complete"])
    detail = {
        **previous_detail,
        "stream_replay_complete": stream_complete,
        "lane_coverage": lane_coverage,
        "complete": complete,
        "coverage_certified": complete,
        "completion_contract": "stream replay complete AND all 13 required lanes complete",
        "historical_counts_as_forward": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }
    store.record_worker_heartbeat(
        worker_id=REPLAY_WORKER_ID,
        state="success" if complete else "degraded",
        error_type=None if complete else "HistoricalLaneCoverageIncomplete",
        detail=detail,
    )
    return detail


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("historical lane coverage certification requires durable evidence persistence")
    result = certify_lane_coverage(store)
    return COVERAGE_COMPLETE_EXIT_CODE if bool(result.get("complete")) else COVERAGE_INCOMPLETE_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "COVERAGE_COMPLETE_EXIT_CODE",
    "COVERAGE_INCOMPLETE_EXIT_CODE",
    "DEFAULT_EDGE_TOLERANCE",
    "certify_lane_coverage",
    "summarize_lane_coverage",
]
