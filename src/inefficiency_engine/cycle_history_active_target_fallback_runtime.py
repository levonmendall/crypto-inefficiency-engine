from __future__ import annotations

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


def _certified_active_target_fallback(
    factory: Any,
    snapshot: Any,
    exc: Exception,
) -> dict[str, object] | None:
    """Serve the last certified target when only its rolling refresh fails.

    ``active_target`` is written only after the exact frozen target has completed and
    the durable checkpoint has been persisted with ``complete=True``.  A later working
    target is explicitly non-authoritative.  Therefore a timeout or other database
    error while refreshing that inactive target must not invalidate the already
    certified generation.  If no certified generation exists, return ``None`` so the
    caller preserves the existing fail-closed exception path.
    """

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

    # Pin the one-shot bridge snapshot to the same exact generation that owns the
    # certified compact history. Partial working-target rows remain invisible.
    _pin_snapshot_in_place(snapshot, target_snapshot)
    working_target = _checkpoint_target(checkpoint, "working_target")

    return {
        "complete": True,
        "working_complete": False,
        "rolling_refresh_in_progress": True,
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
        "refresh_failure_served_prior_exact_target": True,
        "refresh_error_type": type(exc).__name__,
        "refresh_error_message": str(exc)[:500],
        "durable_checkpoint_persisted": True,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance the frozen target without letting refresh failure revoke prior truth."""

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
