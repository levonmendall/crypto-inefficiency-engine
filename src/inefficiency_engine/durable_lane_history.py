from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

from sqlalchemy import inspect, text

from inefficiency_engine.historical_raw_lane_evidence import recover_raw_lane_history
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES
from inefficiency_engine.source_coverage_history import SourceCoverageHistoryLedger


DEFAULT_CACHE_SECONDS = 300.0
DEFAULT_SOURCE_ROW_LIMIT = 3000
DEFAULT_OPERATING_ROW_LIMIT = 1000
SOURCE_COVERAGE_TABLE = "source_coverage_observations"
OPERATING_TABLE = "operating_certification_snapshots"
_CACHE_LOCK = threading.Lock()
_CACHE: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}


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
        if str(spec.get("id") or "") == source_id:
            return {str(value) for value in list(spec.get("classes") or [])}
    return set()


def _empty_state() -> dict[str, object]:
    return {
        "source_count": 0,
        "source_earliest": None,
        "source_latest": None,
        "source_ids": set(),
        "evidence_classes": set(),
        "source_ledgers": set(),
        "canonical_snapshot_count": 0,
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


def _empty_history() -> dict[str, dict[str, object]]:
    return {lane_id: _empty_state() for lane_id in LANES}


def _merge_source_state(state: dict[str, object], recovered: dict[str, object]) -> None:
    count = int(recovered.get("source_count") or 0)
    snapshot_count = int(recovered.get("canonical_snapshot_count") or 0)
    state["source_count"] = int(state.get("source_count") or 0) + count
    state["canonical_snapshot_count"] = int(
        state.get("canonical_snapshot_count") or 0
    ) + snapshot_count
    earliest = recovered.get("source_earliest")
    latest = recovered.get("source_latest")
    current_earliest = state.get("source_earliest")
    current_latest = state.get("source_latest")
    if isinstance(earliest, datetime):
        state["source_earliest"] = (
            earliest
            if not isinstance(current_earliest, datetime) or earliest < current_earliest
            else current_earliest
        )
    if isinstance(latest, datetime):
        state["source_latest"] = (
            latest
            if not isinstance(current_latest, datetime) or latest > current_latest
            else current_latest
        )
    state["source_ids"].update(
        str(value) for value in recovered.get("source_ids", set())
    )
    state["evidence_classes"].update(
        str(value) for value in recovered.get("evidence_classes", set())
    )
    state["source_ledgers"].update(
        str(value) for value in recovered.get("source_ledgers", set())
    )


def _merge_operating_state(
    state: dict[str, object],
    *,
    observed_at: datetime,
    mechanism: dict[str, object],
) -> None:
    state["operating_count"] = int(state.get("operating_count") or 0) + 1
    earliest = state.get("operating_earliest")
    latest = state.get("operating_latest")
    state["operating_earliest"] = (
        observed_at if not isinstance(earliest, datetime) or observed_at < earliest else earliest
    )
    state["operating_latest"] = (
        observed_at if not isinstance(latest, datetime) or observed_at > latest else latest
    )
    latest_operating_at = state.get("latest_operating_at")
    if not isinstance(latest_operating_at, datetime) or observed_at >= latest_operating_at:
        state["latest_operating_at"] = observed_at
        state["latest_operating_state"] = mechanism.get("state")
    for key in (
        "authoritative_observation_count",
        "economic_candidate_count",
        "forward_signal_count",
        "independent_forward_outcome_count",
    ):
        metric_key = f"max_{key}"
        try:
            value = max(0, int(mechanism.get(key) or 0))
        except (TypeError, ValueError):
            value = 0
        state[metric_key] = max(int(state.get(metric_key) or 0), value)


def _read_bounded_source_history(
    store,
    *,
    start: datetime,
    end: datetime,
    limit: int,
) -> dict[str, dict[str, object]]:
    """Legacy pre-canonical source observations, bounded and fail-closed."""

    result = _empty_history()
    available = set(inspect(store.engine).get_table_names())
    if SOURCE_COVERAGE_TABLE not in available:
        return result
    bounded = max(1, min(int(limit), 10000))
    query = text(
        "SELECT lane_id,source_id,observed_at,payload_json "
        "FROM source_coverage_observations "
        "WHERE observed_at>=:start AND observed_at<:end "
        "ORDER BY id DESC LIMIT :limit"
    )
    with store.engine.connect() as db:
        rows = list(
            db.execute(
                query,
                {
                    "start": _utc(start).isoformat(),
                    "end": _utc(end).isoformat(),
                    "limit": bounded,
                },
            ).mappings()
        )
    for row in rows:
        lane_id = str(row.get("lane_id") or "")
        if lane_id not in result:
            continue
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if not bool(payload.get("healthy")):
            continue
        if payload.get("authoritative") is False:
            continue
        if payload.get("commercial_use_permitted") is False:
            continue
        if payload.get("point_in_time") is False:
            continue
        observed_at = _parse_time(row.get("observed_at") or payload.get("observed_at"))
        if observed_at is None or observed_at < start or observed_at >= end:
            continue
        source_id = str(row.get("source_id") or payload.get("source_id") or "")
        classes = {
            str(value) for value in list(payload.get("evidence_classes") or [])
        }
        if not classes and source_id:
            classes = _source_classes(source_id)
        state = result[lane_id]
        state["source_count"] = int(state.get("source_count") or 0) + 1
        earliest = state.get("source_earliest")
        latest = state.get("source_latest")
        state["source_earliest"] = (
            observed_at
            if not isinstance(earliest, datetime) or observed_at < earliest
            else earliest
        )
        state["source_latest"] = (
            observed_at
            if not isinstance(latest, datetime) or observed_at > latest
            else latest
        )
        if source_id:
            state["source_ids"].add(source_id)
        state["evidence_classes"].update(classes)
        state["source_ledgers"].add(SOURCE_COVERAGE_TABLE)
    return result


def _read_bounded_operating_history(
    store,
    *,
    start: datetime,
    end: datetime,
    limit: int,
) -> dict[str, dict[str, object]]:
    """Read a bounded tail of operating snapshots for diagnostic context only."""

    result = _empty_history()
    available = set(inspect(store.engine).get_table_names())
    if OPERATING_TABLE not in available:
        return result
    bounded = max(1, min(int(limit), 5000))
    query = text(
        "SELECT observed_at,payload_json FROM operating_certification_snapshots "
        "WHERE observed_at>=:start AND observed_at<:end "
        "ORDER BY id DESC LIMIT :limit"
    )
    with store.engine.connect() as db:
        rows = list(
            db.execute(
                query,
                {
                    "start": _utc(start).isoformat(),
                    "end": _utc(end).isoformat(),
                    "limit": bounded,
                },
            ).mappings()
        )
    for row in rows:
        observed_at = _parse_time(row.get("observed_at"))
        if observed_at is None or observed_at < start or observed_at >= end:
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
            _merge_operating_state(
                result[lane_id],
                observed_at=observed_at,
                mechanism=mechanism,
            )
    return result


def _lane_row(lane_id: str, state: dict[str, object]) -> dict[str, object]:
    definition = LANES[lane_id]
    required = {str(value) for value in list(definition.get("required") or [])}
    recovered = {str(value) for value in state.get("evidence_classes", set())}
    missing = sorted(required - recovered)
    earliest_candidates = [
        value
        for value in (
            state.get("source_earliest"),
            state.get("operating_earliest"),
        )
        if isinstance(value, datetime)
    ]
    latest_candidates = [
        value
        for value in (
            state.get("source_latest"),
            state.get("operating_latest"),
        )
        if isinstance(value, datetime)
    ]
    source_count = int(state.get("source_count") or 0)
    snapshot_count = int(state.get("canonical_snapshot_count") or 0)
    operating_count = int(state.get("operating_count") or 0)
    has_history = bool(
        source_count
        or snapshot_count
        or operating_count
        or earliest_candidates
        or latest_candidates
    )
    recovered_count = len(required & recovered)
    required_count = len(required)
    return {
        "lane_id": lane_id,
        "lane_name": str(definition.get("name") or lane_id),
        "history_available": has_history,
        "evidence_class_history_complete": bool(has_history and not missing),
        "required_evidence_class_count": required_count,
        "recovered_evidence_class_count": recovered_count,
        "evidence_class_fill_ratio": (
            float(recovered_count) / float(required_count) if required_count else 1.0
        ),
        "canonical_source_snapshot_count": snapshot_count,
        "recovered_source_observations": source_count,
        "recovered_operating_snapshots": operating_count,
        "earliest_recovered_at": (
            min(earliest_candidates).isoformat() if earliest_candidates else None
        ),
        "latest_recovered_at": (
            max(latest_candidates).isoformat() if latest_candidates else None
        ),
        "historical_evidence_classes": sorted(recovered),
        "missing_historical_evidence_classes": missing,
        "source_ids": sorted(str(value) for value in state.get("source_ids", set())),
        "source_ledgers": sorted(
            str(value) for value in state.get("source_ledgers", set())
        ),
        "max_authoritative_observation_count": int(
            state.get("max_authoritative_observation_count") or 0
        ),
        "max_economic_candidate_count": int(
            state.get("max_economic_candidate_count") or 0
        ),
        "max_forward_signal_count": int(state.get("max_forward_signal_count") or 0),
        "max_independent_forward_outcome_count": int(
            state.get("max_independent_forward_outcome_count") or 0
        ),
        "latest_operating_state": state.get("latest_operating_state"),
        "candidate_level_history_synthesized": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def build_durable_lane_history(
    store,
    *,
    start: datetime,
    end: datetime | None = None,
    source_row_limit: int = DEFAULT_SOURCE_ROW_LIMIT,
    operating_row_limit: int = DEFAULT_OPERATING_ROW_LIMIT,
) -> dict[str, object]:
    """Read canonical lane history first; reconstruct only a certified prehistory gap."""

    start = _utc(start)
    end = _utc(end or datetime.now(timezone.utc))
    persisted = _empty_history()
    read_errors: list[dict[str, str]] = []
    first_canonical_snapshot: datetime | None = None
    migration_status: dict[str, object] = {
        "checkpoint_heartbeat_id": 0,
        "complete": False,
        "updated_at": None,
    }

    try:
        ledger = SourceCoverageHistoryLedger(store)
        migration_status = ledger.migration_status()
        first_canonical_snapshot = ledger.first_snapshot_at()
        canonical_history = ledger.summary(start=start, end=end)
        for lane_id, recovered in canonical_history.items():
            if lane_id in persisted:
                _merge_source_state(persisted[lane_id], recovered)
    except Exception as exc:
        read_errors.append(
            {"stage": "canonical_source_coverage_history", "error_type": type(exc).__name__}
        )

    migration_complete = bool(migration_status.get("complete"))
    prehistory_end = start
    if migration_complete:
        prehistory_end = end
        if first_canonical_snapshot is not None:
            prehistory_end = min(end, max(start, first_canonical_snapshot))

    if prehistory_end > start:
        try:
            raw_history = recover_raw_lane_history(
                store,
                start=start,
                boundary=prehistory_end,
            )
            for lane_id, recovered in raw_history.items():
                if lane_id in persisted:
                    _merge_source_state(persisted[lane_id], recovered)
        except Exception as exc:
            read_errors.append(
                {"stage": "raw_precanonical_history", "error_type": type(exc).__name__}
            )

        try:
            legacy_sources = _read_bounded_source_history(
                store,
                start=start,
                end=prehistory_end,
                limit=source_row_limit,
            )
            for lane_id, recovered in legacy_sources.items():
                if lane_id in persisted:
                    _merge_source_state(persisted[lane_id], recovered)
        except Exception as exc:
            read_errors.append(
                {"stage": "legacy_source_prehistory", "error_type": type(exc).__name__}
            )

    try:
        bounded_operating = _read_bounded_operating_history(
            store,
            start=start,
            end=end,
            limit=operating_row_limit,
        )
        for lane_id, recovered in bounded_operating.items():
            if lane_id not in persisted:
                continue
            state = persisted[lane_id]
            state["operating_count"] = int(recovered.get("operating_count") or 0)
            state["operating_earliest"] = recovered.get("operating_earliest")
            state["operating_latest"] = recovered.get("operating_latest")
            state["latest_operating_at"] = recovered.get("latest_operating_at")
            state["latest_operating_state"] = recovered.get("latest_operating_state")
            for key in (
                "max_authoritative_observation_count",
                "max_economic_candidate_count",
                "max_forward_signal_count",
                "max_independent_forward_outcome_count",
            ):
                state[key] = int(recovered.get(key) or 0)
    except Exception as exc:
        read_errors.append(
            {"stage": "bounded_operating_history", "error_type": type(exc).__name__}
        )

    lanes = {
        lane_id: _lane_row(lane_id, persisted.get(lane_id, _empty_state()))
        for lane_id in LANES
    }
    available = sum(bool(row["history_available"]) for row in lanes.values())
    full_classes = sum(
        bool(row["evidence_class_history_complete"]) for row in lanes.values()
    )
    return {
        "history_start": start.isoformat(),
        "history_end": end.isoformat(),
        "canonical_history_started_at": (
            first_canonical_snapshot.isoformat()
            if first_canonical_snapshot is not None
            else None
        ),
        "canonical_history_migration_complete": migration_complete,
        "canonical_history_migration_checkpoint": int(
            migration_status.get("checkpoint_heartbeat_id") or 0
        ),
        "canonical_history_migration_updated_at": migration_status.get("updated_at"),
        "lane_count": len(lanes),
        "lanes_with_durable_history": available,
        "lanes_without_durable_history": len(lanes) - available,
        "lanes_with_all_required_evidence_classes": full_classes,
        "lanes": lanes,
        "read_degraded": bool(read_errors),
        "read_errors": read_errors,
        "read_model": "canonical_source_coverage_history_plus_precanonical_recovery",
        "bounded_source_row_limit": max(1, min(int(source_row_limit), 10000)),
        "bounded_operating_row_limit": max(1, min(int(operating_row_limit), 5000)),
        "history_contract": (
            "canonical append-only source-coverage history is authoritative after its "
            "first snapshot; archive migration is checkpointed and raw/provider "
            "reconstruction is disabled until migration completes, then used only "
            "before the first canonical snapshot; post-live evidence does not certify "
            "the strict pre-live backfill"
        ),
        "candidate_level_history_synthesized": False,
        "historical_counts_as_forward": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def read_durable_lane_history(
    store,
    *,
    start: datetime,
    cache_seconds: float = DEFAULT_CACHE_SECONDS,
) -> dict[str, object]:
    """Return a bounded-cache read model for the dashboard read plane."""

    start = _utc(start)
    key = (id(store), start.isoformat())
    now_mono = time.monotonic()
    ttl = max(1.0, float(cache_seconds))
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is not None and now_mono - cached[0] <= ttl:
            return dict(cached[1])
    payload = build_durable_lane_history(store, start=start)
    with _CACHE_LOCK:
        _CACHE[key] = (now_mono, dict(payload))
    return payload


__all__ = [
    "DEFAULT_CACHE_SECONDS",
    "DEFAULT_SOURCE_ROW_LIMIT",
    "DEFAULT_OPERATING_ROW_LIMIT",
    "build_durable_lane_history",
    "read_durable_lane_history",
]
