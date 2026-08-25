from __future__ import annotations

import os
from typing import Any

from inefficiency_engine.durable_control_cycle_history import _load_checkpoint
from inefficiency_engine.durable_control_cycle_history_target_bridge_runtime import (
    _pin_snapshot_in_place,
    advance_durable_control_cycle_history_cache as _advance_and_pin,
    load_durable_control_cycle_history,
)
from inefficiency_engine.durable_control_cycle_history_target_runtime import (
    _checkpoint_target,
)


def _certified_active_target(
    factory: Any,
    snapshot: Any,
) -> dict[str, object] | None:
    """Pin and verify the last exactly certified frozen cycle-history generation."""

    try:
        checkpoint, valid = _load_checkpoint(factory)
    except Exception:
        return None
    if not valid:
        return None

    active_target = _checkpoint_target(checkpoint, "active_target")
    if active_target is None:
        return None

    serving_scan_id = str(active_target.get("scan_id") or "")
    serving_completed_at = str(active_target.get("completed_at") or "")
    if not serving_scan_id or not serving_completed_at:
        return None

    try:
        target_snapshot = factory.store.load_scan(serving_scan_id)
    except Exception:
        return None
    if target_snapshot.completed_at.isoformat() != serving_completed_at:
        return None

    # A checkpoint active_target is written only after exact promotion, but also verify
    # the compact projection is readable before letting the bridge consume it.
    try:
        history = load_durable_control_cycle_history(factory, target_snapshot)
    except Exception:
        return None
    if history is None:
        return None

    _pin_snapshot_in_place(snapshot, target_snapshot)
    working_target = _checkpoint_target(checkpoint, "working_target")
    return {
        "complete": True,
        "working_complete": working_target is None,
        "rolling_refresh_in_progress": working_target is not None,
        "promoted_working_target": False,
        "serving_scan_id": serving_scan_id,
        "serving_target_completed_at": serving_completed_at,
        "working_target_scan_id": (
            str(working_target.get("scan_id")) if working_target is not None else None
        ),
        "working_target_completed_at": (
            str(working_target.get("completed_at")) if working_target is not None else None
        ),
        "target_frozen_across_executors": True,
        "double_buffered_boundary": True,
        "partial_working_target_authoritative": False,
        "serving_snapshot_pinned_in_place": True,
        "bridge_snapshot_scan_id": str(snapshot.scan_id),
        "bridge_snapshot_completed_at": snapshot.completed_at.isoformat(),
        "durable_checkpoint_persisted": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def _control_executor_read_only_progress(
    factory: Any,
    snapshot: Any,
) -> dict[str, object]:
    """Never reconstruct raw history inside the 25-second canonical-control child."""

    certified = _certified_active_target(factory, snapshot)
    if certified is not None:
        return {
            **certified,
            "control_executor_history_mode": "certified_cache_read_only",
            "background_backfill_owner": "cycle-history-background-backfill",
            "raw_history_queries_in_control": False,
        }

    working_target = None
    try:
        checkpoint, valid = _load_checkpoint(factory)
        if valid:
            working_target = _checkpoint_target(checkpoint, "working_target")
    except Exception:
        valid = False

    return {
        "complete": False,
        "working_complete": False,
        "rolling_refresh_in_progress": working_target is not None,
        "serving_scan_id": None,
        "serving_target_completed_at": None,
        "working_target_scan_id": (
            str(working_target.get("scan_id")) if working_target is not None else None
        ),
        "working_target_completed_at": (
            str(working_target.get("completed_at")) if working_target is not None else None
        ),
        "error_type": "CycleHistoryBackgroundBackfillPending",
        "message": (
            "no certified cycle-history target is available yet; exact raw-ledger "
            "reconstruction is owned by the disposable background backfill worker"
        ),
        "control_executor_history_mode": "certified_cache_read_only",
        "background_backfill_owner": "cycle-history-background-backfill",
        "raw_history_queries_in_control": False,
        "durable_checkpoint_persisted": bool(valid),
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def _certified_active_target_fallback(
    factory: Any,
    snapshot: Any,
    exc: Exception,
) -> dict[str, object] | None:
    """Serve the last certified target when only its rolling refresh fails."""

    certified = _certified_active_target(factory, snapshot)
    if certified is None:
        return None
    return {
        **certified,
        "working_complete": False,
        "rolling_refresh_in_progress": True,
        "refresh_failure_served_prior_exact_target": True,
        "refresh_error_type": type(exc).__name__,
        "refresh_error_message": str(exc)[:500],
    }


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance only outside control; canonical control consumes certified cache only."""

    if os.getenv("CIE_CONTROL_EXECUTOR_CYCLE_ID"):
        return _control_executor_read_only_progress(factory, snapshot)

    try:
        return _advance_and_pin(
            factory,
            snapshot,
            stop_at_monotonic=stop_at_monotonic,
        )
    except Exception as exc:
        fallback = _certified_active_target_fallback(factory, snapshot, exc)
        if fallback is None:
            raise
        return fallback


__all__ = [
    "advance_durable_control_cycle_history_cache",
    "load_durable_control_cycle_history",
]
