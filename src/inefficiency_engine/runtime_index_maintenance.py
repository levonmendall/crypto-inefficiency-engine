from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import inspect, text


CONTROL_GATE_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "market_quotes": ("venue", "observed_at"),
    "funding_quotes": ("venue", "observed_at"),
    "order_books": ("venue", "observed_at"),
    "opportunities": ("observed_at",),
    "provider_statuses": ("provider", "id"),
    "source_coverage_observations": ("source_id", "lane_id", "id"),
    "provider_gap_admissions": ("mechanism_id", "provider", "id"),
}

# Canonical cycle-history bootstrap reads one venue/asset/day from the append-only
# market quote ledger, then retains the newest source ids. Keep the existing
# venue/observed_at source-read index above and add this second, purpose-built index as
# a separate control-gate scope because one dict cannot represent two indexes for the
# same table. Canonical control must not start until both access paths are available.
CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "market_quotes": ("venue", "asset", "observed_at", "id"),
}

# These indexes improve bounded read paths but are not prerequisites for starting
# canonical control. In particular, the maker/transfer ledgers can exist in legacy
# production databases with an older schema. Their absence or schema drift must stay
# fail-closed for those individual evidence sources without freezing the whole control
# plane behind optional index DDL.
BACKGROUND_INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "maker_shadow_outcomes": ("observed_at",),
    "capital_transfer_outcomes": ("observed_at",),
    "alpha_forward_events": ("event_type", "strategy_id", "family"),
    "allocation_forward_trials": ("strategy", "settlement_supported", "id"),
    "allocation_forward_outcomes": ("strategy", "id"),
}

INDEX_SPECS: dict[str, tuple[str, ...]] = {
    **CONTROL_GATE_INDEX_SPECS,
    **BACKGROUND_INDEX_SPECS,
}

ProgressCallback = Callable[[dict[str, object]], None]

# Runtime index maintenance is deliberately outside API startup, but required indexes
# still gate canonical control. Bound PostgreSQL DDL at the database session so a
# blocked concurrent build cannot freeze that gate indefinitely. A timed-out required
# index stays fail-closed and is retried by the post-bind supervisor.
POSTGRES_INDEX_STATEMENT_TIMEOUT_MS = 30_000
POSTGRES_INDEX_LOCK_TIMEOUT_MS = 5_000


class RuntimeIndexVerificationError(RuntimeError):
    """Raised when PostgreSQL cannot certify a required runtime index as usable."""


def _index_name(table_name: str, columns: tuple[str, ...]) -> str:
    return f"ix_runtime_{table_name}_{'_'.join(columns)}"


def _create_index_sql(
    *,
    dialect_name: str,
    index_name: str,
    table_name: str,
    columns: tuple[str, ...],
    if_not_exists: bool = True,
) -> str:
    concurrent = " CONCURRENTLY" if dialect_name == "postgresql" else ""
    existence = " IF NOT EXISTS" if if_not_exists else ""
    return (
        f"CREATE INDEX{concurrent}{existence} {index_name} "
        f"ON {table_name} ({','.join(columns)})"
    )


def _postgres_index_state(db: Any, *, index_name: str) -> dict[str, bool] | None:
    """Return PostgreSQL planner-usable state for one index in the active search path."""

    row = (
        db.execute(
            text(
                """
                SELECT i.indisvalid AS valid, i.indisready AS ready
                FROM pg_index AS i
                WHERE i.indexrelid = to_regclass(:index_name)
                """
            ),
            {"index_name": index_name},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return {
        "valid": bool(row.get("valid")),
        "ready": bool(row.get("ready")),
    }


def _configure_postgres_index_deadlines(db: Any) -> None:
    """Apply finite session deadlines before any runtime-index DDL is issued."""

    db.execute(text(f"SET statement_timeout TO '{POSTGRES_INDEX_STATEMENT_TIMEOUT_MS}ms'"))
    db.execute(text(f"SET lock_timeout TO '{POSTGRES_INDEX_LOCK_TIMEOUT_MS}ms'"))


def _ensure_postgres_index(
    db: Any,
    *,
    index_name: str,
    table_name: str,
    columns: tuple[str, ...],
) -> dict[str, object]:
    """Verify first, then create or self-heal one PostgreSQL runtime index.

    A planner-usable existing index is returned immediately without issuing DDL. This
    matters on small production databases because even ``CREATE INDEX CONCURRENTLY IF
    NOT EXISTS`` can wait behind another catalog/DDL operation before PostgreSQL reaches
    its no-op decision.

    Missing or invalid indexes are repaired under finite ``statement_timeout`` and
    ``lock_timeout`` session settings. If PostgreSQL cancels a build, the exception is
    surfaced to the outer maintenance loop, which records the failure and retries while
    canonical control remains fail-closed. Interrupted concurrent builds are detected
    through ``pg_index`` and dropped/rebuilt on the next bounded attempt.
    """

    state = _postgres_index_state(db, index_name=index_name)
    if state is not None and bool(state["valid"] and state["ready"]):
        return {
            "postgres_index_valid": True,
            "postgres_index_ready": True,
            "repaired_invalid_index": False,
            "existing_index_reused": True,
            "ddl_required": False,
        }

    _configure_postgres_index_deadlines(db)
    repaired_invalid_index = state is not None

    if repaired_invalid_index:
        db.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}"))
        create_statement = _create_index_sql(
            dialect_name="postgresql",
            index_name=index_name,
            table_name=table_name,
            columns=columns,
            if_not_exists=False,
        )
    else:
        create_statement = _create_index_sql(
            dialect_name="postgresql",
            index_name=index_name,
            table_name=table_name,
            columns=columns,
            if_not_exists=True,
        )

    db.execute(text(create_statement))
    state = _postgres_index_state(db, index_name=index_name)

    if state is None:
        raise RuntimeIndexVerificationError(
            f"PostgreSQL runtime index {index_name} is missing after maintenance"
        )
    if not bool(state["valid"] and state["ready"]):
        raise RuntimeIndexVerificationError(
            f"PostgreSQL runtime index {index_name} remains invalid or unready after repair"
        )

    return {
        "postgres_index_valid": True,
        "postgres_index_ready": True,
        "repaired_invalid_index": repaired_invalid_index,
        "existing_index_reused": False,
        "ddl_required": True,
        "postgres_statement_timeout_ms": POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
        "postgres_lock_timeout_ms": POSTGRES_INDEX_LOCK_TIMEOUT_MS,
    }


def ensure_runtime_indexes_after_api_bind(
    store: Any,
    *,
    index_specs: Mapping[str, tuple[str, ...]] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Create bounded-read indexes outside the web-service startup critical path.

    Production PostgreSQL uses ``CREATE INDEX CONCURRENTLY`` in autocommit mode so
    multimillion-row index builds do not hold the normal table-write lock or prevent
    Render from binding the API port. PostgreSQL index existence alone is insufficient:
    every maintained index is verified as planner-usable through ``pg_index`` and any
    invalid leftover from an interrupted concurrent build is rebuilt before success is
    reported. Existing valid indexes are reused without DDL, and all required DDL is
    database-time-bounded so maintenance cannot block the control gate indefinitely.
    SQLite and test stores keep ordinary idempotent index creation.

    The helper validates each table's actual deployed columns before issuing DDL.
    Missing columns remain a hard failure for control-gate indexes, but background
    optimization indexes are terminally skipped. This makes legacy auxiliary schema
    drift observable without turning it into system-wide control unavailability.
    """

    requested = dict(index_specs or INDEX_SPECS)
    inspector = inspect(store.engine)
    available = set(inspector.get_table_names())
    dialect_name = str(getattr(store.engine.dialect, "name", ""))
    attempted: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    for table_name, columns in requested.items():
        if table_name not in available:
            continue
        index_name = _index_name(table_name, columns)
        started = time.monotonic()
        actual_columns = {
            str(row.get("name"))
            for row in inspector.get_columns(table_name)
            if row.get("name") is not None
        }
        missing_columns = [column for column in columns if column not in actual_columns]
        if missing_columns:
            row = {
                "index": index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": False,
                "error_type": "SchemaColumnMissing",
                "message": (
                    f"deployed table {table_name} is missing runtime-index columns: "
                    + ",".join(missing_columns)
                ),
                "missing_columns": missing_columns,
                "schema_compatible": False,
            }
            attempted.append(row)
            if table_name in BACKGROUND_INDEX_SPECS:
                skipped_row = {**row, "skipped": True, "optional": True}
                skipped.append(skipped_row)
                if progress is not None:
                    progress({"phase": "skipped", **skipped_row})
                continue
            failures.append(row)
            if progress is not None:
                progress({"phase": "failed", **row})
            continue

        statement = _create_index_sql(
            dialect_name=dialect_name,
            index_name=index_name,
            table_name=table_name,
            columns=columns,
        )
        if progress is not None:
            progress(
                {
                    "phase": "starting",
                    "index": index_name,
                    "table": table_name,
                    "concurrent": dialect_name == "postgresql",
                    "schema_compatible": True,
                }
            )
        try:
            postgres_state: dict[str, object] = {}
            if dialect_name == "postgresql":
                with store.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as db:
                    postgres_state = _ensure_postgres_index(
                        db,
                        index_name=index_name,
                        table_name=table_name,
                        columns=columns,
                    )
            else:
                with store.engine.begin() as db:
                    db.execute(text(statement))
            row = {
                "index": index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": True,
                "schema_compatible": True,
                **postgres_state,
            }
            attempted.append(row)
            if progress is not None:
                progress({"phase": "complete", **row})
        except Exception as exc:
            failure = {
                "index": index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": False,
                "error_type": type(exc).__name__,
                "message": str(exc)[:500],
                "schema_compatible": True,
            }
            attempted.append(failure)
            failures.append(failure)
            if progress is not None:
                progress({"phase": "failed", **failure})

    return {
        "complete": not failures,
        "dialect": dialect_name,
        "attempted": attempted,
        "failures": failures,
        "skipped": skipped,
        "requested_tables": list(requested),
        "startup_critical_path": False,
        "api_bound_before_maintenance": True,
        "postgres_index_validity_verified": dialect_name == "postgresql",
    }
