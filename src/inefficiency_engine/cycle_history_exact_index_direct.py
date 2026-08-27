from __future__ import annotations

import time
from typing import Any, Callable

from sqlalchemy import text

from inefficiency_engine import runtime_index_maintenance as rim


EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS = 8_000
EXACT_INDEX_TABLE = "market_quotes"
EXACT_INDEX_COLUMNS = rim.CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS[EXACT_INDEX_TABLE]
ProgressCallback = Callable[[dict[str, object]], None]


def _emit(progress: ProgressCallback | None, payload: dict[str, object]) -> None:
    if progress is not None:
        progress(payload)


def _postgres_table_columns(db: Any, *, table_name: str) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT a.attname AS name
            FROM pg_attribute AS a
            WHERE a.attrelid = to_regclass(:table_name)
              AND a.attnum > 0
              AND NOT a.attisdropped
            """
        ),
        {"table_name": table_name},
    ).mappings()
    return {str(row["name"]) for row in rows}


def _failure(
    *,
    started: float,
    logical_index_name: str,
    index_name: str,
    exc: Exception,
    pre_ddl_complete: bool,
) -> dict[str, object]:
    return {
        "index": index_name,
        "logical_index": logical_index_name,
        "table": EXACT_INDEX_TABLE,
        "runtime_seconds": max(0.0, time.monotonic() - started),
        "concurrent": True,
        "ok": False,
        "error_type": type(exc).__name__,
        "message": str(exc)[:500],
        "schema_compatible": pre_ddl_complete,
        "pre_ddl_complete": pre_ddl_complete,
        "pre_ddl_statement_timeout_ms": EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
    }


def ensure_exact_cycle_history_index_direct(
    store: Any,
    *,
    statement_timeout_ms: int,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Maintain the known exact index without generic PostgreSQL schema inspection."""

    engine = getattr(store, "engine", None)
    dialect_name = str(getattr(getattr(engine, "dialect", None), "name", ""))
    if dialect_name != "postgresql":
        # Production exact-index ownership is PostgreSQL-only. Preserve the existing
        # generic behavior for SQLite/tests and other non-production dialects.
        return rim.ensure_runtime_indexes_after_api_bind(
            store,
            index_specs=rim.CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
            progress=progress,
        )

    logical_index_name = rim._index_name(EXACT_INDEX_TABLE, EXACT_INDEX_COLUMNS)
    index_name = rim._postgres_canonical_index_name(logical_index_name)
    started = time.monotonic()
    pre_ddl_complete = False
    _emit(
        progress,
        {
            "phase": "preddl_starting",
            "index": index_name,
            "logical_index": logical_index_name,
            "table": EXACT_INDEX_TABLE,
            "concurrent": True,
            "pre_ddl_statement_timeout_ms": EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
            "schema_free_runtime_store": True,
            "generic_schema_inspector_bypassed": True,
        },
    )

    try:
        with store.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as db:
            rim._configure_postgres_index_deadlines(
                db,
                statement_timeout_ms=EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
            )
            actual_columns = _postgres_table_columns(db, table_name=EXACT_INDEX_TABLE)
            missing_columns = [column for column in EXACT_INDEX_COLUMNS if column not in actual_columns]
            if missing_columns:
                raise rim.RuntimeIndexVerificationError(
                    "deployed table market_quotes is missing exact-index columns: "
                    + ",".join(missing_columns)
                )

            canonical_state = rim._postgres_index_state(db, index_name=index_name)
            replacement_states: dict[str, dict[str, bool]] = {}
            reusable_name: str | None = None
            target_name = index_name
            repaired_invalid_index = False
            if canonical_state is not None and not rim._postgres_index_is_usable(canonical_state):
                replacement_states = rim._postgres_replacement_index_states(
                    db,
                    index_name=index_name,
                )
                reusable_name = rim._usable_replacement_index_name(
                    index_name=index_name,
                    existing_states=replacement_states,
                )
                if reusable_name is None:
                    target_name = rim._next_replacement_index_name(
                        index_name=index_name,
                        existing_states=replacement_states,
                    )
                    repaired_invalid_index = True

            pre_ddl_complete = True
            pre_ddl_seconds = max(0.0, time.monotonic() - started)
            _emit(
                progress,
                {
                    "phase": "preddl_complete",
                    "index": target_name,
                    "logical_index": logical_index_name,
                    "table": EXACT_INDEX_TABLE,
                    "concurrent": True,
                    "pre_ddl_runtime_seconds": pre_ddl_seconds,
                    "pre_ddl_statement_timeout_ms": EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
                    "schema_compatible": True,
                    "schema_free_runtime_store": True,
                    "generic_schema_inspector_bypassed": True,
                },
            )

            if rim._postgres_index_is_usable(canonical_state):
                row = rim._verified_index_result(
                    canonical_index_name=index_name,
                    effective_index_name=index_name,
                    repaired_invalid_index=False,
                    existing_index_reused=True,
                    ddl_required=False,
                )
                row.update(
                    {
                        "index": index_name,
                        "logical_index": logical_index_name,
                        "table": EXACT_INDEX_TABLE,
                        "runtime_seconds": max(0.0, time.monotonic() - started),
                        "concurrent": True,
                        "ok": True,
                        "schema_compatible": True,
                        "pre_ddl_complete": True,
                        "pre_ddl_runtime_seconds": pre_ddl_seconds,
                    }
                )
                _emit(progress, {"phase": "complete", **row})
                return {
                    "complete": True,
                    "dialect": dialect_name,
                    "attempted": [row],
                    "failures": [],
                    "requested_tables": [EXACT_INDEX_TABLE],
                    "postgres_index_validity_verified": True,
                    "direct_exact_index_path": True,
                }

            if reusable_name is not None:
                row = rim._verified_index_result(
                    canonical_index_name=index_name,
                    effective_index_name=reusable_name,
                    repaired_invalid_index=True,
                    existing_index_reused=True,
                    ddl_required=False,
                    deferred_invalid_index_name=index_name,
                )
                row.update(
                    {
                        "index": reusable_name,
                        "logical_index": logical_index_name,
                        "table": EXACT_INDEX_TABLE,
                        "runtime_seconds": max(0.0, time.monotonic() - started),
                        "concurrent": True,
                        "ok": True,
                        "schema_compatible": True,
                        "pre_ddl_complete": True,
                        "pre_ddl_runtime_seconds": pre_ddl_seconds,
                        "replacement_versions_observed": len(replacement_states),
                    }
                )
                _emit(progress, {"phase": "complete", **row})
                return {
                    "complete": True,
                    "dialect": dialect_name,
                    "attempted": [row],
                    "failures": [],
                    "requested_tables": [EXACT_INDEX_TABLE],
                    "postgres_index_validity_verified": True,
                    "direct_exact_index_path": True,
                }

            create_statement = rim._create_index_sql(
                dialect_name="postgresql",
                index_name=target_name,
                table_name=EXACT_INDEX_TABLE,
                columns=EXACT_INDEX_COLUMNS,
                if_not_exists=not repaired_invalid_index,
            )
            rim._configure_postgres_index_deadlines(
                db,
                statement_timeout_ms=int(statement_timeout_ms),
            )
            _emit(
                progress,
                {
                    "phase": "ddl_starting",
                    "index": target_name,
                    "logical_index": logical_index_name,
                    "table": EXACT_INDEX_TABLE,
                    "concurrent": True,
                    "statement_timeout_ms": int(statement_timeout_ms),
                    "pre_ddl_complete": True,
                    "pre_ddl_runtime_seconds": pre_ddl_seconds,
                },
            )
            ddl_started = time.monotonic()
            db.execute(text(create_statement))
            ddl_runtime_seconds = max(0.0, time.monotonic() - ddl_started)

            rim._configure_postgres_index_deadlines(
                db,
                statement_timeout_ms=EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS,
            )
            target_state = rim._postgres_index_state(db, index_name=target_name)
            if not rim._postgres_index_is_usable(target_state):
                raise rim.RuntimeIndexVerificationError(
                    f"PostgreSQL exact cycle-history index {target_name} remains invalid or unready after maintenance"
                )

            row = rim._verified_index_result(
                canonical_index_name=index_name,
                effective_index_name=target_name,
                repaired_invalid_index=repaired_invalid_index,
                existing_index_reused=False,
                ddl_required=True,
                deferred_invalid_index_name=(index_name if repaired_invalid_index else None),
            )
            row.update(
                {
                    "index": target_name,
                    "logical_index": logical_index_name,
                    "table": EXACT_INDEX_TABLE,
                    "runtime_seconds": max(0.0, time.monotonic() - started),
                    "ddl_runtime_seconds": ddl_runtime_seconds,
                    "concurrent": True,
                    "ok": True,
                    "schema_compatible": True,
                    "pre_ddl_complete": True,
                    "pre_ddl_runtime_seconds": pre_ddl_seconds,
                    "postgres_statement_timeout_ms": int(statement_timeout_ms),
                    "postgres_lock_timeout_ms": rim.POSTGRES_INDEX_LOCK_TIMEOUT_MS,
                    "replacement_versions_observed": len(replacement_states),
                }
            )
            _emit(progress, {"phase": "complete", **row})
            return {
                "complete": True,
                "dialect": dialect_name,
                "attempted": [row],
                "failures": [],
                "requested_tables": [EXACT_INDEX_TABLE],
                "postgres_index_validity_verified": True,
                "direct_exact_index_path": True,
            }
    except Exception as exc:
        failure = _failure(
            started=started,
            logical_index_name=logical_index_name,
            index_name=index_name,
            exc=exc,
            pre_ddl_complete=pre_ddl_complete,
        )
        _emit(progress, {"phase": "failed", **failure})
        return {
            "complete": False,
            "dialect": dialect_name,
            "attempted": [failure],
            "failures": [failure],
            "requested_tables": [EXACT_INDEX_TABLE],
            "postgres_index_validity_verified": True,
            "direct_exact_index_path": True,
        }


__all__ = [
    "EXACT_INDEX_COLUMNS",
    "EXACT_INDEX_PRE_DDL_STATEMENT_TIMEOUT_MS",
    "EXACT_INDEX_TABLE",
    "ensure_exact_cycle_history_index_direct",
]
