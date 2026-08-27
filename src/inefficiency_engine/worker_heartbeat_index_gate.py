from __future__ import annotations

from typing import Any

from inefficiency_engine.runtime_index_maintenance import (
    _index_name,
    _postgres_canonical_index_name,
    _postgres_index_is_usable,
    _postgres_index_state,
    _postgres_replacement_index_states,
    _usable_replacement_index_name,
)


WORKER_HEARTBEAT_INDEX_COLUMNS = ("worker_id", "id")
WORKER_HEARTBEAT_INDEX_TABLE = "worker_heartbeats"


def worker_heartbeat_priority_index_status(store: Any) -> dict[str, object]:
    """Return whether the targeted latest-heartbeat read index is planner-usable.

    This is a read-only gate. It never creates or drops indexes and grants no
    qualification, allocation, or execution authority.
    """

    dialect_name = str(getattr(store.engine.dialect, "name", ""))
    canonical_name = _postgres_canonical_index_name(
        _index_name(WORKER_HEARTBEAT_INDEX_TABLE, WORKER_HEARTBEAT_INDEX_COLUMNS)
    )
    common = {
        "dialect": dialect_name,
        "table": WORKER_HEARTBEAT_INDEX_TABLE,
        "columns": list(WORKER_HEARTBEAT_INDEX_COLUMNS),
        "canonical_index_name": canonical_name,
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }

    if dialect_name != "postgresql":
        return {
            **common,
            "ready": True,
            "effective_index_name": None,
            "planner_usable_verified": False,
            "reason": "postgres_runtime_index_not_required",
        }

    canonical_state = None
    try:
        with store.engine.connect() as db:
            canonical_state = _postgres_index_state(db, index_name=canonical_name)
            if _postgres_index_is_usable(canonical_state):
                return {
                    **common,
                    "ready": True,
                    "effective_index_name": canonical_name,
                    "planner_usable_verified": True,
                    "postgres_index_valid": True,
                    "postgres_index_ready": True,
                    "reason": "canonical_index_ready",
                }

            replacement_states = _postgres_replacement_index_states(
                db,
                index_name=canonical_name,
            )
            replacement_name = _usable_replacement_index_name(
                index_name=canonical_name,
                existing_states=replacement_states,
            )
            if replacement_name is not None:
                return {
                    **common,
                    "ready": True,
                    "effective_index_name": replacement_name,
                    "planner_usable_verified": True,
                    "postgres_index_valid": True,
                    "postgres_index_ready": True,
                    "replacement_index_used": True,
                    "reason": "replacement_index_ready",
                }
    except Exception as exc:
        return {
            **common,
            "ready": False,
            "effective_index_name": None,
            "planner_usable_verified": False,
            "reason": "index_verification_failed",
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
        }

    return {
        **common,
        "ready": False,
        "effective_index_name": None,
        "planner_usable_verified": True,
        "postgres_index_valid": bool(
            canonical_state is not None and canonical_state.get("valid")
        ),
        "postgres_index_ready": bool(
            canonical_state is not None and canonical_state.get("ready")
        ),
        "reason": "planner_usable_index_unavailable",
    }


__all__ = [
    "WORKER_HEARTBEAT_INDEX_COLUMNS",
    "WORKER_HEARTBEAT_INDEX_TABLE",
    "worker_heartbeat_priority_index_status",
]
