from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SOURCE_REFRESH_WORKER_ID = "priority-source-refresh-plane"
ALPHA_L2_WORKER_ID = "alpha-l2-research-sampling"
MECHANISM_FORWARD_WORKER_ID = "mechanism-forward-evidence"
# Keep heartbeat recovery aligned with the production worker/dashboard freshness
# contract. Source truth has an additional shorter retry cooldown below so a recent
# degraded heartbeat cannot indefinitely mask old authoritative observations.
DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS = 180.0
DEFAULT_SOURCE_TRUTH_RETRY_COOLDOWN_SECONDS = 60.0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _worker_status(
    store,
    worker_id: str,
    *,
    now: datetime,
    stale_after_seconds: float,
) -> dict[str, Any]:
    """Read one durable heartbeat without turning a diagnostic read error into work."""

    try:
        heartbeat = store.latest_worker_heartbeat(worker_id)
    except Exception as exc:
        return {
            "worker_id": worker_id,
            "available": False,
            "recovery_required": False,
            "reason": "heartbeat_read_unavailable",
            "error_type": type(exc).__name__,
        }

    if heartbeat is None:
        return {
            "worker_id": worker_id,
            "available": False,
            "recovery_required": True,
            "reason": "unobserved",
            "age_seconds": None,
            "state": "unobserved",
        }

    observed_at = _utc(heartbeat.observed_at)
    age_seconds = max(0.0, (now - observed_at).total_seconds())
    recovery_after_seconds = max(60.0, float(stale_after_seconds))
    stale = age_seconds > recovery_after_seconds
    return {
        "worker_id": worker_id,
        "available": True,
        "recovery_required": stale,
        "reason": "grossly_stale" if stale else "current_enough",
        "age_seconds": age_seconds,
        "observed_at": observed_at.isoformat(),
        "state": heartbeat.state,
        "error_type": heartbeat.error_type,
        "recovery_after_seconds": recovery_after_seconds,
    }


def _read_source_truth(store, *, now: datetime) -> dict[str, dict[str, object]]:
    # Lazy import keeps the worker-recovery module lightweight and avoids making
    # dashboard SQL inspection part of import-time process boot.
    from inefficiency_engine.dashboard_source_truth import read_current_source_truth

    return read_current_source_truth(store, now=now)


def _source_truth_status(
    store,
    *,
    now: datetime,
    source_worker: dict[str, Any],
    retry_cooldown_seconds: float,
) -> dict[str, Any]:
    """Return recovery need from the evidence the cards actually consume.

    A source-refresh heartbeat proves that the refresh plane ran; it does not prove
    that a provider returned a new authoritative observation. A degraded refresh can
    therefore have a brand-new heartbeat while the dashboard remains correctly
    stale. Read the same canonical source truth used by the cards and allow another
    bounded refresh once its short retry cooldown expires.
    """

    cooldown = max(30.0, float(retry_cooldown_seconds))
    try:
        truth = _read_source_truth(store, now=now)
    except Exception as exc:
        return {
            "available": False,
            "stale": False,
            "recovery_required": False,
            "reason": "source_truth_read_unavailable",
            "error_type": type(exc).__name__,
            "retry_cooldown_seconds": cooldown,
            "stale_lane_ids": [],
            "stale_source_ids": [],
        }

    stale_lane_ids: list[str] = []
    stale_source_ids: set[str] = set()
    for lane_id, raw in truth.items():
        if not isinstance(raw, dict):
            continue
        raw_ids = raw.get("stale_source_ids")
        ids = [str(value) for value in raw_ids if value not in (None, "")] if isinstance(raw_ids, list) else []
        state = str(raw.get("source_state") or "")
        if state == "stale" or ids:
            stale_lane_ids.append(str(lane_id))
            stale_source_ids.update(ids)

    if not stale_lane_ids:
        return {
            "available": True,
            "stale": False,
            "recovery_required": False,
            "reason": "source_truth_current",
            "retry_cooldown_seconds": cooldown,
            "stale_lane_ids": [],
            "stale_source_ids": [],
        }

    # A heartbeat read failure cannot safely prove that another source refresh is
    # not already running, so keep fail-closed and avoid spawning duplicate work.
    if source_worker.get("reason") == "heartbeat_read_unavailable":
        return {
            "available": True,
            "stale": True,
            "recovery_required": False,
            "reason": "stale_truth_heartbeat_unavailable",
            "retry_cooldown_seconds": cooldown,
            "stale_lane_ids": sorted(stale_lane_ids),
            "stale_source_ids": sorted(stale_source_ids),
        }

    age = source_worker.get("age_seconds")
    recently_attempted = isinstance(age, (int, float)) and float(age) < cooldown
    recovery_required = not recently_attempted
    return {
        "available": True,
        "stale": True,
        "recovery_required": recovery_required,
        "reason": "stale_truth_retry_due" if recovery_required else "stale_truth_retry_cooldown",
        "retry_cooldown_seconds": cooldown,
        "last_refresh_attempt_age_seconds": age,
        "stale_lane_ids": sorted(stale_lane_ids),
        "stale_source_ids": sorted(stale_source_ids),
    }


def critical_evidence_recovery_status(
    store,
    *,
    now: datetime | None = None,
    stale_after_seconds: float = DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS,
    source_truth_retry_cooldown_seconds: float = DEFAULT_SOURCE_TRUTH_RETRY_COOLDOWN_SECONDS,
) -> dict[str, Any]:
    """Return bounded recovery needs for dashboard-critical research evidence.

    Worker heartbeat recovery remains aligned to the 180-second production freshness
    budget. Source recovery additionally reads canonical source truth, because a
    recent degraded refresh heartbeat can otherwise hide evidence that never
    advanced. Stale source truth may trigger one additional early refresh after a
    short cooldown, while fresh provider failures are still rate-limited.

    Qualification, sizing, settlement, allocation and execution authority are
    unchanged.
    """

    current = _utc(now or datetime.now(timezone.utc))
    workers = {
        "source_refresh": _worker_status(
            store,
            SOURCE_REFRESH_WORKER_ID,
            now=current,
            stale_after_seconds=stale_after_seconds,
        ),
        "alpha_l2_sampling": _worker_status(
            store,
            ALPHA_L2_WORKER_ID,
            now=current,
            stale_after_seconds=stale_after_seconds,
        ),
        "mechanism_forward": _worker_status(
            store,
            MECHANISM_FORWARD_WORKER_ID,
            now=current,
            stale_after_seconds=stale_after_seconds,
        ),
    }
    source_truth = _source_truth_status(
        store,
        now=current,
        source_worker=workers["source_refresh"],
        retry_cooldown_seconds=source_truth_retry_cooldown_seconds,
    )
    source_required = bool(
        workers["source_refresh"].get("recovery_required")
        or source_truth.get("recovery_required")
    )
    alpha_required = bool(workers["alpha_l2_sampling"].get("recovery_required"))
    mechanism_required = bool(workers["mechanism_forward"].get("recovery_required"))
    recovery_after_seconds = max(60.0, float(stale_after_seconds))
    return {
        "source_refresh_required": source_required,
        "alpha_forward_required": alpha_required,
        "mechanism_forward_required": mechanism_required,
        "any_required": source_required or alpha_required or mechanism_required,
        "stale_after_seconds": recovery_after_seconds,
        "source_truth_retry_cooldown_seconds": max(30.0, float(source_truth_retry_cooldown_seconds)),
        "dashboard_freshness_aligned": True,
        "source_truth_recovery_active": True,
        "source_truth": source_truth,
        "workers": workers,
        "normal_cadence_unchanged": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "paper_only": True,
    }
