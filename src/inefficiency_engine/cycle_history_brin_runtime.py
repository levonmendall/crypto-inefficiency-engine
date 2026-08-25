from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from sqlalchemy import inspect, text

from inefficiency_engine.runtime_index_maintenance import (
    POSTGRES_INDEX_LOCK_TIMEOUT_MS,
    RuntimeIndexVerificationError,
    _configure_postgres_index_deadlines,
    _next_replacement_index_name,
    _postgres_index_is_usable,
    _postgres_index_state,
    _postgres_replacement_index_states,
    _usable_replacement_index_name,
    _verified_index_result,
)


CYCLE_HISTORY_BRIN_INDEX_NAME = "ix_runtime_market_quotes_observed_at_brin"
CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS = 60_000
CYCLE_HISTORY_BRIN_PAGES_PER_RANGE = 32

ProgressCallback = Callable[[dict[str, object]], None]


def _create_brin_index_sql(
    *,
    index_name: str,
    if_not_exists: bool,
) -> str:
    existence = " IF NOT EXISTS" if if_not_exists else ""
    return (
        f"CREATE INDEX CONCURRENTLY{existence} {index_name} "
        "ON market_quotes USING BRIN (observed_at) "
        f"WITH (pages_per_range={CYCLE_HISTORY_BRIN_PAGES_PER_RANGE})"
    )


def _brin_result(
    *,
    canonical_index_name: str,
    effective_index_name: str,
    repaired_invalid_index: bool,
    existing_index_reused: bool,
    ddl_required: bool,
    deferred_invalid_index_name: str | None = None,
) -> dict[str, object]:
    result = _verified_index_result(
        canonical_index_name=canonical_index_name,
        effective_index_name=effective_index_name,
        repaired_invalid_index=repaired_invalid_index,
        existing_index_reused=existing_index_reused,
        ddl_required=ddl_required,
        deferred_invalid_index_name=deferred_invalid_index_name,
    )
    result.update(
        {
            "access_method": "brin",
            "pages_per_range": CYCLE_HISTORY_BRIN_PAGES_PER_RANGE,
            "postgres_statement_timeout_ms": CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS,
            "postgres_lock_timeout_ms": POSTGRES_INDEX_LOCK_TIMEOUT_MS,
        }
    )
    return result


def _ensure_postgres_cycle_history_brin(db: Any) -> dict[str, object]:
    """Create or recover the compact time-range index used by history buckets.

    The exact four-column btree remains a useful optimization, but production has shown
    that building it concurrently can exceed the bounded DDL window on the small Render
    PostgreSQL instance. A BRIN over append-only ISO-8601 ``observed_at`` is tiny and
    cheap to build, yet still gives PostgreSQL an exact range access path before it
    applies venue/asset predicates and the final newest-observation sort.

    Invalid interrupted builds are never dropped on this path. As with the existing
    runtime-index maintainer, a verified dynamic replacement is reused or created.
    """

    canonical = CYCLE_HISTORY_BRIN_INDEX_NAME
    state = _postgres_index_state(db, index_name=canonical)
    if _postgres_index_is_usable(state):
        return _brin_result(
            canonical_index_name=canonical,
            effective_index_name=canonical,
            repaired_invalid_index=False,
            existing_index_reused=True,
            ddl_required=False,
        )

    _configure_postgres_index_deadlines(
        db,
        statement_timeout_ms=CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS,
    )

    if state is not None:
        replacement_states = _postgres_replacement_index_states(
            db,
            index_name=canonical,
        )
        reusable = _usable_replacement_index_name(
            index_name=canonical,
            existing_states=replacement_states,
        )
        if reusable is not None:
            result = _brin_result(
                canonical_index_name=canonical,
                effective_index_name=reusable,
                repaired_invalid_index=True,
                existing_index_reused=True,
                ddl_required=False,
                deferred_invalid_index_name=canonical,
            )
            result["replacement_versions_observed"] = len(replacement_states)
            return result

        replacement = _next_replacement_index_name(
            index_name=canonical,
            existing_states=replacement_states,
        )
        db.execute(text(_create_brin_index_sql(index_name=replacement, if_not_exists=False)))
        replacement_state = _postgres_index_state(db, index_name=replacement)
        if not _postgres_index_is_usable(replacement_state):
            raise RuntimeIndexVerificationError(
                f"PostgreSQL cycle-history BRIN replacement {replacement} "
                "remains invalid or unready after maintenance"
            )
        result = _brin_result(
            canonical_index_name=canonical,
            effective_index_name=replacement,
            repaired_invalid_index=True,
            existing_index_reused=False,
            ddl_required=True,
            deferred_invalid_index_name=canonical,
        )
        result["replacement_versions_observed"] = len(replacement_states)
        return result

    db.execute(text(_create_brin_index_sql(index_name=canonical, if_not_exists=True)))
    state = _postgres_index_state(db, index_name=canonical)
    if not _postgres_index_is_usable(state):
        raise RuntimeIndexVerificationError(
            f"PostgreSQL cycle-history BRIN index {canonical} is missing, invalid, "
            "or unready after maintenance"
        )
    return _brin_result(
        canonical_index_name=canonical,
        effective_index_name=canonical,
        repaired_invalid_index=False,
        existing_index_reused=False,
        ddl_required=True,
    )


def ensure_cycle_history_brin_after_api_bind(
    store: Any,
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Install the lightweight cycle-history range index after the API is live.

    This is performance maintenance only. It never grants evidence, qualification,
    allocation, or execution authority. Canonical control retains its existing bounded,
    exact, fail-closed bucket query if this optimization is unavailable.
    """

    dialect = str(getattr(store.engine.dialect, "name", ""))
    index_name = CYCLE_HISTORY_BRIN_INDEX_NAME
    table_name = "market_quotes"
    started = time.monotonic()

    if dialect != "postgresql":
        skipped = {
            "index": index_name,
            "table": table_name,
            "runtime_seconds": max(0.0, time.monotonic() - started),
            "concurrent": False,
            "ok": True,
            "skipped": True,
            "optional": True,
            "access_method": "brin",
            "reason": "postgresql_only",
        }
        if progress is not None:
            progress({"phase": "skipped", **skipped})
        return {
            "complete": True,
            "dialect": dialect,
            "attempted": [],
            "failures": [],
            "skipped": [skipped],
            "startup_critical_path": False,
            "api_bound_before_maintenance": True,
            "cycle_history_brin_ready": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

    inspector = inspect(store.engine)
    available = set(inspector.get_table_names())
    columns = (
        {
            str(row.get("name"))
            for row in inspector.get_columns(table_name)
            if row.get("name") is not None
        }
        if table_name in available
        else set()
    )
    if table_name not in available or "observed_at" not in columns:
        failure = {
            "index": index_name,
            "table": table_name,
            "runtime_seconds": max(0.0, time.monotonic() - started),
            "concurrent": True,
            "ok": False,
            "error_type": "SchemaColumnMissing",
            "message": "market_quotes.observed_at is required for cycle-history BRIN",
            "access_method": "brin",
            "schema_compatible": False,
        }
        if progress is not None:
            progress({"phase": "failed", **failure})
        return {
            "complete": False,
            "dialect": dialect,
            "attempted": [failure],
            "failures": [failure],
            "skipped": [],
            "startup_critical_path": False,
            "api_bound_before_maintenance": True,
            "cycle_history_brin_ready": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }

    if progress is not None:
        progress(
            {
                "phase": "starting",
                "index": index_name,
                "table": table_name,
                "concurrent": True,
                "access_method": "brin",
                "pages_per_range": CYCLE_HISTORY_BRIN_PAGES_PER_RANGE,
                "schema_compatible": True,
            }
        )

    try:
        with store.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as db:
            state = _ensure_postgres_cycle_history_brin(db)
        row = {
            "index": index_name,
            "table": table_name,
            "runtime_seconds": max(0.0, time.monotonic() - started),
            "concurrent": True,
            "ok": True,
            "schema_compatible": True,
            **state,
        }
        if progress is not None:
            progress({"phase": "complete", **row})
        return {
            "complete": True,
            "dialect": dialect,
            "attempted": [row],
            "failures": [],
            "skipped": [],
            "startup_critical_path": False,
            "api_bound_before_maintenance": True,
            "cycle_history_brin_ready": True,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }
    except Exception as exc:
        failure = {
            "index": index_name,
            "table": table_name,
            "runtime_seconds": max(0.0, time.monotonic() - started),
            "concurrent": True,
            "ok": False,
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
            "access_method": "brin",
            "pages_per_range": CYCLE_HISTORY_BRIN_PAGES_PER_RANGE,
            "schema_compatible": True,
        }
        if progress is not None:
            progress({"phase": "failed", **failure})
        return {
            "complete": False,
            "dialect": dialect,
            "attempted": [failure],
            "failures": [failure],
            "skipped": [],
            "startup_critical_path": False,
            "api_bound_before_maintenance": True,
            "cycle_history_brin_ready": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }
