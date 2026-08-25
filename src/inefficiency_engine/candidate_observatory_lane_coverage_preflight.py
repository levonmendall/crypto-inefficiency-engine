from __future__ import annotations

from inefficiency_engine.candidate_observatory_historical_replay import REPLAY_WORKER_ID
from inefficiency_engine.candidate_observatory_lane_coverage import (
    COVERAGE_COMPLETE_EXIT_CODE,
    COVERAGE_INCOMPLETE_EXIT_CODE,
    certify_lane_coverage,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store


COVERAGE_NOT_READY_EXIT_CODE = 2


def replay_is_ready_for_lane_certification(store) -> bool:
    """Return whether durable stream replay has reached its fixed live boundary.

    This check deliberately does not infer completion from current source health. It
    only accepts the replay worker's durable completion contract plus the captured
    first-live observatory boundary. The process is short-lived so its database stack
    is fully reclaimed before any heavy replay child is considered.
    """

    try:
        heartbeat = store.latest_worker_heartbeat(REPLAY_WORKER_ID)
    except Exception:
        return False
    detail = getattr(heartbeat, "detail", {}) if heartbeat is not None else {}
    if not isinstance(detail, dict):
        return False
    stream_complete = bool(
        detail.get("stream_replay_complete", detail.get("complete"))
    )
    live_boundary_known = bool(detail.get("live_observatory_started_at"))
    return bool(stream_complete and live_boundary_known)


def main() -> int:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError(
            "historical lane coverage preflight requires durable evidence persistence"
        )
    if not replay_is_ready_for_lane_certification(store):
        return COVERAGE_NOT_READY_EXIT_CODE
    result = certify_lane_coverage(store)
    return (
        COVERAGE_COMPLETE_EXIT_CODE
        if bool(result.get("complete"))
        else COVERAGE_INCOMPLETE_EXIT_CODE
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "COVERAGE_NOT_READY_EXIT_CODE",
    "main",
    "replay_is_ready_for_lane_certification",
]
