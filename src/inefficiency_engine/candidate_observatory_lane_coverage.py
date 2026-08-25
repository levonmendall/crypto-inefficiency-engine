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
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


COVERAGE_COMPLETE_EXIT_CODE = 3
COVERAGE_INCOMPLETE_EXIT_CODE = 4
DEFAULT_EDGE_TOLERANCE = timedelta(hours=12)
REPLAY_TABLE = "candidate_observatory_historical_replay"
SOURCE_COVERAGE_TABLE = "source_coverage_observations"
OPERATING_TABLE = "operating_certification_snapshots"


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


def _source_classes(source_id: str) -> set[str]:
    for spec in SOURCES:
        if str(spec.get("id")) == source_id:
            return {str(value) for value in list(spec.get("classes") or [])}
    return set()


def _coverage_row(lane_id: str) -> dict[str, object]:
    return {
        "lane_id": lane_id,
        "lane_name": str(LANES[lane_id].get("name") or lane_id),
        "required": True,
        "state": "unavailable",
        "earliest_recovered_at": None,
        "latest_recovered_at": None,
        "recovered_funnel_records": 0,
        "recovered_source_observations": 0,
        "recovered_operating_snapshots": 0,
        "historical_evidence_classes": [],
        "missing_historical_evidence_classes": list(LANES[lane_id].get("required") or []),
        "source_ledgers": [],
        "source_ids": [],
        "reconstruction_quality": "unrecoverable_from_current_persisted_sources",
        "candidate_level_rejections_reconstructable": False,
        "omitted_untrusted_records": 0,
        "max_authoritative_observation_count": 0,
        "max_economic_candidate_count": 0,
        "max_forward_signal_count": 0,
        "max_independent_forward_outcome_count": 0,
        "latest_operating_state": None,
        "reason": "no trusted persisted historical lane evidence has been recovered",
    }


def _empty_persisted_history() -> dict[str, dict[str, object]]:
    return {
        lane_id: {
            "source_count": 0,
            "source_earliest": None,
            "source_latest": None,
            "source_ids": set(),
            "evidence_classes": set(),
            "operating_count": 0,
            "operating_earliest": None,
            "operating_latest": None,
            "latest_operating_at": None,
            "latest_operating_state": None,
            "max_authoritative_observation_count": 0,
            "max_economic_candidate_count": 0,
            "max_forward_signal_count": 0,
            "max_independent_forward_outcome_count": 0,
        }
        for lane_id in LANES
    }


def _read_persisted_lane_history(
    store,
    *,
    start: datetime,
    boundary: datetime,
) -> dict[str, dict[str, object]]:
    """Aggregate exact historical lane evidence without manufacturing candidates.

    Source coverage observations prove what evidence classes were actually gathered.
    Operating-certification snapshots preserve point-in-time lane progress, candidate,
    signal, and outcome counts. Neither source is promoted into forward truth here.
    """

    result = _empty_persisted_history()
    try:
        available = set(inspect(store.engine).get_table_names())
    except Exception:
        return result

    start_text = _utc(start).isoformat()
    boundary_text = _utc(boundary).isoformat()

    if SOURCE_COVERAGE_TABLE in available:
        table = Table(SOURCE_COVERAGE_TABLE, MetaData(), autoload_with=store.engine)
        query = (
            select(table.c.lane_id, table.c.source_id, table.c.observed_at, table.c.payload_json)
            .where(table.c.observed_at >= start_text)
            .where(table.c.observed_at < boundary_text)
            .order_by(table.c.id)
        )
        with store.engine.connect() as db:
            for row in db.execute(query).mappings():
                lane_id = str(row.get("lane_id") or "")
                if lane_id not in result:
                    continue
                try:
                    payload = json.loads(str(row.get("payload_json") or "{}"))
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(payload, dict):
                    continue
                # Failed attempts are useful transport telemetry but not historical
                # market evidence. Only admitted point-in-time observations count.
                if not bool(payload.get("healthy")):
                    continue
                if payload.get("authoritative") is False:
                    continue
                if payload.get("commercial_use_permitted") is False:
                    continue
                if payload.get("point_in_time") is False:
                    continue
                observed_at = _parse_time(row.get("observed_at") or payload.get("observed_at"))
                if observed_at is None:
                    continue
                state = result[lane_id]
                state["source_count"] = int(state["source_count"]) + 1
                earliest = state.get("source_earliest")
                latest = state.get("source_latest")
                state["source_earliest"] = observed_at if earliest is None or observed_at < earliest else earliest
                state["source_latest"] = observed_at if latest is None or observed_at > latest else latest
                source_id = str(row.get("source_id") or payload.get("source_id") or "")
                if source_id:
                    state["source_ids"].add(source_id)
                classes = {str(value) for value in list(payload.get("evidence_classes") or [])}
                if not classes and source_id:
                    classes = _source_classes(source_id)
                state["evidence_classes"].update(classes)

    if OPERATING_TABLE in available:
        table = Table(OPERATING_TABLE, MetaData(), autoload_with=store.engine)
        query = (
            select(table.c.observed_at, table.c.payload_json)
            .where(table.c.observed_at >= start_text)
            .where(table.c.observed_at < boundary_text)
            .order_by(table.c.id)
        )
        with store.engine.connect() as db:
            for row in db.execute(query).mappings():
                observed_at = _parse_time(row.get("observed_at"))
                if observed_at is None:
                    continue
                try:
                    payload = json.loads(str(row.get("payload_json") or "{}"))
                except (TypeError, json.JSONDecodeError):
                    continue
                mechanisms = payload.get("mechanisms") if isinstance(payload, dict) else None
                if not isinstance(mechanisms, list):
                    continue
                for mechanism in mechanisms:
                    if not isinstance(mechanism, dict):
                        continue
                    lane_id = str(mechanism.get("mechanism_id") or "")
                    if lane_id not in result:
                        continue
                    state = result[lane_id]
                    state["operating_count"] = int(state["operating_count"]) + 1
                    earliest = state.get("operating_earliest")
                    latest = state.get("operating_latest")
                    state["operating_earliest"] = observed_at if earliest is None or observed_at < earliest else earliest
                    state["operating_latest"] = observed_at if latest is None or observed_at > latest else latest
                    if state.get("latest_operating_at") is None or observed_at >= state["latest_operating_at"]:
                        state["latest_operating_at"] = observed_at
                        state["latest_operating_state"] = mechanism.get("state")
                    for key in (
                        "authoritative_observation_count",
                        "economic_candidate_count",
                        "forward_signal_count",
                        "independent_forward_outcome_count",
                    ):
                        metric_key = f"max_{key}"
                        value = int(mechanism.get(key) or 0)
                        state[metric_key] = max(int(state.get(metric_key) or 0), value)

    return result


def summarize_lane_coverage(
    records: list[dict[str, object]],
    *,
    start: datetime,
    boundary: datetime,
    edge_tolerance: timedelta = DEFAULT_EDGE_TOLERANCE,
    persisted_history: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build a fail-closed historical evidence contract over all canonical lanes.

    Funnel replay remains the strongest reconstruction when it exists. For lanes whose
    legacy candidate identities/funnels were never persisted, exact source-coverage and
    operating-certification ledgers can still prove that useful lane evidence was
    gathered. This exposes real history without inventing rejected candidates.
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

    history = persisted_history or _empty_persisted_history()
    for lane_id, row in rows.items():
        times = seen_times[lane_id]
        lane_history = history.get(lane_id, {})
        source_count = int(lane_history.get("source_count") or 0)
        operating_count = int(lane_history.get("operating_count") or 0)
        source_earliest = lane_history.get("source_earliest")
        source_latest = lane_history.get("source_latest")
        operating_earliest = lane_history.get("operating_earliest")
        operating_latest = lane_history.get("operating_latest")
        evidence_classes = {str(value) for value in lane_history.get("evidence_classes", set())}
        required_classes = {str(value) for value in list(LANES[lane_id].get("required") or [])}
        missing_classes = sorted(required_classes - evidence_classes)

        row["omitted_untrusted_records"] = omitted[lane_id]
        row["recovered_funnel_records"] = len(times)
        row["recovered_source_observations"] = source_count
        row["recovered_operating_snapshots"] = operating_count
        row["historical_evidence_classes"] = sorted(evidence_classes)
        row["missing_historical_evidence_classes"] = missing_classes
        row["source_ids"] = sorted(str(value) for value in lane_history.get("source_ids", set()))
        row["max_authoritative_observation_count"] = int(
            lane_history.get("max_authoritative_observation_count") or 0
        )
        row["max_economic_candidate_count"] = int(lane_history.get("max_economic_candidate_count") or 0)
        row["max_forward_signal_count"] = int(lane_history.get("max_forward_signal_count") or 0)
        row["max_independent_forward_outcome_count"] = int(
            lane_history.get("max_independent_forward_outcome_count") or 0
        )
        row["latest_operating_state"] = lane_history.get("latest_operating_state")

        recovered_times = list(times)
        for value in (source_earliest, source_latest, operating_earliest, operating_latest):
            if isinstance(value, datetime):
                recovered_times.append(value)
        if recovered_times:
            row["earliest_recovered_at"] = min(recovered_times).isoformat()
            row["latest_recovered_at"] = max(recovered_times).isoformat()

        ledger_names = set(sources[lane_id])
        if source_count:
            ledger_names.add(SOURCE_COVERAGE_TABLE)
        if operating_count:
            ledger_names.add(OPERATING_TABLE)
        row["source_ledgers"] = sorted(ledger_names)

        funnel_covers_start = bool(times and min(times) <= start + tolerance)
        funnel_covers_boundary = bool(times and max(times) >= boundary - tolerance)
        trusted_funnel = bool(times and omitted[lane_id] == 0)
        funnel_complete = trusted_funnel and funnel_covers_start and funnel_covers_boundary

        source_covers_start = isinstance(source_earliest, datetime) and source_earliest <= start + tolerance
        source_covers_boundary = isinstance(source_latest, datetime) and source_latest >= boundary - tolerance
        source_classes_complete = not missing_classes
        source_complete = bool(
            source_count
            and source_covers_start
            and source_covers_boundary
            and source_classes_complete
        )

        if funnel_complete:
            row["state"] = "complete"
            row["reconstruction_quality"] = "exact_aggregate_funnel"
            row["reason"] = "trusted persisted aggregate funnel evidence covers both replay edges"
            continue
        if source_complete:
            row["state"] = "complete"
            row["reconstruction_quality"] = "exact_source_evidence_history"
            row["reason"] = (
                "trusted point-in-time source evidence covers every required evidence class and both replay edges; "
                "candidate-level legacy rejections remain unreconstructable"
            )
            continue

        any_history = bool(times or source_count or operating_count or omitted[lane_id])
        if not any_history:
            continue

        row["state"] = "partial"
        if times:
            row["reconstruction_quality"] = "partial_aggregate_funnel"
        elif source_count:
            row["reconstruction_quality"] = "partial_source_evidence_history"
        elif operating_count:
            row["reconstruction_quality"] = "operating_history_only"
        else:
            row["reconstruction_quality"] = "untrusted_legacy_only"

        reasons: list[str] = []
        if times and not funnel_covers_start:
            reasons.append("recovered funnel history starts after the replay-start tolerance")
        if times and not funnel_covers_boundary:
            reasons.append("recovered funnel history stops before the live-boundary tolerance")
        if omitted[lane_id]:
            reasons.append("untrusted legacy funnel records were omitted")
        if source_count and not source_covers_start:
            reasons.append("source evidence starts after the replay-start tolerance")
        if source_count and not source_covers_boundary:
            reasons.append("source evidence stops before the live-boundary tolerance")
        if source_count and missing_classes:
            reasons.append("missing historical evidence classes: " + ", ".join(missing_classes))
        if not source_count and operating_count:
            reasons.append("operating snapshots exist but lane-tagged source history is unavailable")
        row["reason"] = "; ".join(reasons) or "historical lane evidence is incomplete"

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
        "coverage_contract": (
            "all required lanes must have trusted edge-to-edge persisted funnel history OR "
            "trusted edge-to-edge source history covering every required evidence class"
        ),
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
    """Persist authoritative post-replay lane certification and evidence history."""

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

    stream_complete = bool(
        previous_detail.get("stream_replay_complete", previous_detail.get("complete"))
    ) and live_start is not None
    persisted_history = _read_persisted_lane_history(store, start=start, boundary=boundary)
    lane_coverage = summarize_lane_coverage(
        _read_replay_records(store),
        start=start,
        boundary=boundary,
        persisted_history=persisted_history,
    )
    complete = bool(stream_complete and lane_coverage["required_lanes_complete"])
    detail = {
        **previous_detail,
        "stream_replay_complete": stream_complete,
        "lane_coverage": lane_coverage,
        "complete": complete,
        "coverage_certified": complete,
        "completion_contract": (
            "stream replay complete AND all 13 required lanes have trusted historical evidence coverage"
        ),
        "historical_source_reconstruction": True,
        "candidate_level_rejections_synthesized": False,
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
