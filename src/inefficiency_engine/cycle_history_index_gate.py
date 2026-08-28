from __future__ import annotations

from typing import Any
import os
from datetime import datetime, timedelta, timezone

from inefficiency_engine.runtime_index_maintenance import (
    CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
    _index_name,
    _postgres_canonical_index_name,
    _postgres_index_is_usable,
    _postgres_index_state,
    _postgres_replacement_index_states,
    _usable_replacement_index_name,
)


def _connection_resilience_detail(store: Any) -> dict[str, object]:
    """Expose non-secret exact-index connection settings when the store supplies them."""

    getter = getattr(store, "connection_resilience_detail", None)
    if not callable(getter):
        return {}
    try:
        detail = getter()
    except Exception:
        return {}
    return dict(detail) if isinstance(detail, dict) else {}


def cycle_history_exact_index_status(store: Any) -> dict[str, object]:
    """Return whether the exact cycle-history query index is planner-usable.

    PostgreSQL production backfill is allowed to reconstruct raw 180-day history only
    after the exact ``market_quotes(venue, asset, observed_at, id)`` access path is
    verified as ready/valid. SQLite and other local test stores do not use PostgreSQL
    runtime-index maintenance and therefore pass this production-only gate.

    This function is read-only. It never creates, drops, repairs or grants authority.
    """

    dialect_name = str(getattr(store.engine.dialect, "name", ""))
    table_name, columns = next(iter(CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS.items()))
    canonical_name = _postgres_canonical_index_name(
        _index_name(table_name, columns)
    )
    connection_resilience = _connection_resilience_detail(store)

    if os.getenv("CIE_MARKET_HISTORY_BACKEND", "").strip().lower() == "parquet":
        from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory

        end = datetime.now(timezone.utc)
        status = PartitionedMarketHistory().readiness(
            required_start=end - timedelta(days=180),
            required_end=end - timedelta(minutes=10),
        )
        return {
            **status,
            "dialect": dialect_name,
            "table": "partitioned_market_history",
            "columns": ["venue", "asset", "observed_at", "lineage_hash"],
            "canonical_index_name": None,
            "effective_index_name": None,
            "planner_usable_verified": False,
            "filesystem_history_verified": bool(status["ready"]),
            "allocation_authority": False,
            **connection_resilience,
        }

    if dialect_name != "postgresql":
        return {
            "ready": True,
            "dialect": dialect_name,
            "table": table_name,
            "columns": list(columns),
            "canonical_index_name": canonical_name,
            "effective_index_name": None,
            "reason": "postgres_runtime_index_not_required",
            "planner_usable_verified": False,
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
            **connection_resilience,
        }

    try:
        with store.engine.connect() as db:
            canonical_state = _postgres_index_state(
                db,
                index_name=canonical_name,
            )
            if _postgres_index_is_usable(canonical_state):
                return {
                    "ready": True,
                    "dialect": dialect_name,
                    "table": table_name,
                    "columns": list(columns),
                    "canonical_index_name": canonical_name,
                    "effective_index_name": canonical_name,
                    "reason": "canonical_index_ready",
                    "planner_usable_verified": True,
                    "postgres_index_valid": True,
                    "postgres_index_ready": True,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                    **connection_resilience,
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
                    "ready": True,
                    "dialect": dialect_name,
                    "table": table_name,
                    "columns": list(columns),
                    "canonical_index_name": canonical_name,
                    "effective_index_name": replacement_name,
                    "reason": "replacement_index_ready",
                    "planner_usable_verified": True,
                    "postgres_index_valid": True,
                    "postgres_index_ready": True,
                    "replacement_index_used": True,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                    **connection_resilience,
                }
    except Exception as exc:
        return {
            "ready": False,
            "dialect": dialect_name,
            "table": table_name,
            "columns": list(columns),
            "canonical_index_name": canonical_name,
            "effective_index_name": None,
            "reason": "index_verification_failed",
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
            "planner_usable_verified": False,
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
            **connection_resilience,
        }

    return {
        "ready": False,
        "dialect": dialect_name,
        "table": table_name,
        "columns": list(columns),
        "canonical_index_name": canonical_name,
        "effective_index_name": None,
        "reason": "planner_usable_index_unavailable",
        "planner_usable_verified": True,
        "postgres_index_valid": bool(
            canonical_state is not None and canonical_state.get("valid")
        ),
        "postgres_index_ready": bool(
            canonical_state is not None and canonical_state.get("ready")
        ),
        "qualification_thresholds_unchanged": True,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
        **connection_resilience,
    }


__all__ = ["cycle_history_exact_index_status"]
