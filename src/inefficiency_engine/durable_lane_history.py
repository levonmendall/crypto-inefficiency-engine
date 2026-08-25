from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from inefficiency_engine.candidate_observatory_lane_coverage import (
    _read_persisted_lane_history,
)
from inefficiency_engine.source_coverage_catalog import LANES


DEFAULT_CACHE_SECONDS = 300.0
_CACHE_LOCK = threading.Lock()
_CACHE: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


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
    operating_count = int(state.get("operating_count") or 0)
    has_history = bool(source_count or operating_count or earliest_candidates or latest_candidates)
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
) -> dict[str, object]:
    """Summarize all trustworthy durable lane evidence since ``start``.

    This is deliberately distinct from pre-live historical certification. Evidence
    collected after the first live observatory boundary is included here so a lane can
    truthfully show accumulated history without retroactively claiming pre-live
    coverage. No candidate identities, forward samples, qualification, allocation, or
    execution authority are synthesized.
    """

    start = _utc(start)
    end = _utc(end or datetime.now(timezone.utc))
    persisted = _read_persisted_lane_history(store, start=start, boundary=end)
    lanes = {
        lane_id: _lane_row(lane_id, persisted.get(lane_id, {}))
        for lane_id in LANES
    }
    available = sum(bool(row["history_available"]) for row in lanes.values())
    full_classes = sum(
        bool(row["evidence_class_history_complete"]) for row in lanes.values()
    )
    return {
        "history_start": start.isoformat(),
        "history_end": end.isoformat(),
        "lane_count": len(lanes),
        "lanes_with_durable_history": available,
        "lanes_without_durable_history": len(lanes) - available,
        "lanes_with_all_required_evidence_classes": full_classes,
        "lanes": lanes,
        "history_contract": (
            "all trustworthy persisted source/operating evidence since history_start; "
            "post-live evidence does not certify the strict pre-live backfill"
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
    "build_durable_lane_history",
    "read_durable_lane_history",
]
