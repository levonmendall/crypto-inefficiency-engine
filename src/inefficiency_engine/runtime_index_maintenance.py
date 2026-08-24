from __future__ import annotations

import time
from typing import Any

from sqlalchemy import inspect, text


INDEX_SPECS: dict[str, tuple[str, ...]] = {
    "market_quotes": ("venue", "observed_at"),
    "funding_quotes": ("venue", "observed_at"),
    "order_books": ("venue", "observed_at"),
    "opportunities": ("observed_at",),
    "provider_statuses": ("provider", "id"),
    "source_coverage_observations": ("source_id", "lane_id", "id"),
    "provider_gap_admissions": ("mechanism_id", "provider", "id"),
    "maker_shadow_outcomes": ("observed_at",),
    "capital_transfer_outcomes": ("observed_at",),
    # Canonical operating reconciliation consumes append-only strategy evidence.
    # These indexes keep the initial aggregate/filter pass bounded while subsequent
    # cycles read only rows newer than the durable primary-key tails.
    "alpha_forward_events": ("event_type", "strategy_id", "family"),
    "allocation_forward_trials": ("strategy", "settlement_supported", "id"),
    "allocation_forward_outcomes": ("strategy", "id"),
}


def _index_name(table_name: str, columns: tuple[str, ...]) -> str:
    return f"ix_runtime_{table_name}_{'_'.join(columns)}"


def _create_index_sql(
    *,
    dialect_name: str,
    index_name: str,
    table_name: str,
    columns: tuple[str, ...],
) -> str:
    concurrent = " CONCURRENTLY" if dialect_name == "postgresql" else ""
    return (
        f"CREATE INDEX{concurrent} IF NOT EXISTS {index_name} "
        f"ON {table_name} ({','.join(columns)})"
    )


def ensure_runtime_indexes_after_api_bind(store: Any) -> dict[str, object]:
    """Create bounded-read indexes outside the web-service startup critical path.

    Production PostgreSQL uses ``CREATE INDEX CONCURRENTLY`` in autocommit mode so
    multimillion-row index builds do not hold the normal table-write lock or prevent
    Render from binding the API port. SQLite and test stores keep the ordinary
    idempotent ``CREATE INDEX IF NOT EXISTS`` form.

    The caller is expected to run this from a background maintenance thread after
    the API is already healthy. Failures are returned per index so the supervisor can
    retry without taking the service offline.
    """

    available = set(inspect(store.engine).get_table_names())
    dialect_name = str(getattr(store.engine.dialect, "name", ""))
    attempted: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []

    for table_name, columns in INDEX_SPECS.items():
        if table_name not in available:
            continue
        index_name = _index_name(table_name, columns)
        statement = _create_index_sql(
            dialect_name=dialect_name,
            index_name=index_name,
            table_name=table_name,
            columns=columns,
        )
        started = time.monotonic()
        try:
            if dialect_name == "postgresql":
                with store.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as db:
                    db.execute(text(statement))
            else:
                with store.engine.begin() as db:
                    db.execute(text(statement))
            attempted.append(
                {
                    "index": index_name,
                    "table": table_name,
                    "runtime_seconds": max(0.0, time.monotonic() - started),
                    "concurrent": dialect_name == "postgresql",
                    "ok": True,
                }
            )
        except Exception as exc:
            failure = {
                "index": index_name,
                "table": table_name,
                "runtime_seconds": max(0.0, time.monotonic() - started),
                "concurrent": dialect_name == "postgresql",
                "ok": False,
                "error_type": type(exc).__name__,
                "message": str(exc)[:500],
            }
            attempted.append(failure)
            failures.append(failure)

    return {
        "complete": not failures,
        "dialect": dialect_name,
        "attempted": attempted,
        "failures": failures,
        "startup_critical_path": False,
        "api_bound_before_maintenance": True,
    }
