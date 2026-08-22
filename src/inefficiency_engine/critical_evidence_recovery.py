from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SOURCE_REFRESH_WORKER_ID = "priority-source-refresh-plane"
ALPHA_L2_WORKER_ID = "alpha-l2-research-sampling"
MECHANISM_FORWARD_WORKER_ID = "mechanism-forward-evidence"
# Keep recovery aligned with the production worker/dashboard freshness contract.
# The previous 1,800-second guard allowed evidence to remain stale for 27 minutes
# after the runtime had already declared it stale (180 seconds).
DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS = 180.0


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


def critical_evidence_recovery_status(
    store,
    *,
    now: datetime | None = None,
    stale_after_seconds: float = DEFAULT_CRITICAL_EVIDENCE_RECOVERY_STALE_SECONDS,
) -> dict[str, Any]:
    """Return bounded recovery needs for dashboard-critical research workers.

    Recovery uses the same 180-second default freshness budget as the production
    worker/card contract. This closes the old gap where the dashboard could mark
    evidence stale at 180 seconds while recovery waited 1,800 seconds.

    A recent degraded/error heartbeat still suppresses immediate retries until the
    freshness budget elapses, preventing provider hammering. Once that budget is
    exceeded, the next disposable research cycle may force one early source/alpha
    recovery pass. Qualification, sizing, settlement, and execution authority are
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
    source_required = bool(workers["source_refresh"].get("recovery_required"))
    alpha_required = bool(
        workers["alpha_l2_sampling"].get("recovery_required")
        or workers["mechanism_forward"].get("recovery_required")
    )
    recovery_after_seconds = max(60.0, float(stale_after_seconds))
    return {
        "source_refresh_required": source_required,
        "alpha_forward_required": alpha_required,
        "any_required": source_required or alpha_required,
        "stale_after_seconds": recovery_after_seconds,
        "dashboard_freshness_aligned": True,
        "workers": workers,
        "normal_cadence_unchanged": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "paper_only": True,
    }
