from __future__ import annotations

from typing import Any

from inefficiency_engine.durable_control_cycle_history_target_runtime import (
    advance_durable_control_cycle_history_cache as _advance_target_cache,
    load_durable_control_cycle_history,
)


def _pin_snapshot_in_place(snapshot: Any, target: Any) -> None:
    """Replace a one-shot source snapshot with the exact certified target snapshot."""

    fields = getattr(target.__class__, "model_fields", {})
    names = tuple(fields) if isinstance(fields, dict) else ()
    if not names:
        names = tuple(getattr(target, "__dict__", {}))
    for name in names:
        setattr(snapshot, name, getattr(target, name))


def advance_durable_control_cycle_history_cache(
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance the frozen target and pin bridge publication to its certified scan.

    ``control_cycle_executor`` deliberately reuses the same ``source_snapshot`` object
    after cache preflight. When a newer working target is still rebuilding, the cache
    serves the prior certified generation. Mutating this one-shot object to that exact
    persisted scan keeps long history, short history, current source evidence, and the
    qualified bridge on one point-in-time boundary without changing the executor API.
    """

    progress = _advance_target_cache(
        factory,
        snapshot,
        stop_at_monotonic=stop_at_monotonic,
    )
    if not bool(progress.get("complete")):
        progress["serving_snapshot_pinned_in_place"] = False
        return progress

    serving_scan_id = str(progress.get("serving_scan_id") or "")
    expected_at = str(progress.get("serving_target_completed_at") or "")
    if not serving_scan_id:
        return {
            **progress,
            "complete": False,
            "error_type": "CycleHistoryServingTargetUnavailable",
            "serving_snapshot_pinned_in_place": False,
            "paper_only": True,
        }

    if serving_scan_id == str(snapshot.scan_id):
        if expected_at and snapshot.completed_at.isoformat() != expected_at:
            return {
                **progress,
                "complete": False,
                "error_type": "CycleHistoryServingTargetMismatch",
                "serving_snapshot_pinned_in_place": False,
                "paper_only": True,
            }
        progress["serving_snapshot_pinned_in_place"] = True
        return progress

    try:
        target_snapshot = factory.store.load_scan(serving_scan_id)
    except Exception as exc:
        return {
            **progress,
            "complete": False,
            "error_type": "CycleHistoryServingTargetScanUnavailable",
            "serving_snapshot_load_error_type": type(exc).__name__,
            "serving_snapshot_pinned_in_place": False,
            "paper_only": True,
        }
    if expected_at and target_snapshot.completed_at.isoformat() != expected_at:
        return {
            **progress,
            "complete": False,
            "error_type": "CycleHistoryServingTargetMismatch",
            "serving_snapshot_pinned_in_place": False,
            "paper_only": True,
        }

    _pin_snapshot_in_place(snapshot, target_snapshot)
    progress["serving_snapshot_pinned_in_place"] = True
    progress["bridge_snapshot_scan_id"] = str(snapshot.scan_id)
    progress["bridge_snapshot_completed_at"] = snapshot.completed_at.isoformat()
    return progress
