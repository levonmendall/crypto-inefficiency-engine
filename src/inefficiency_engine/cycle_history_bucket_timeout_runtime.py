from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import event


_DEFAULT_STATEMENT_TIMEOUT_SECONDS = 4.0
_DEFAULT_LOCK_TIMEOUT_SECONDS = 1.0


def _bounded_seconds(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def cycle_history_bucket_statement_timeout_seconds() -> float:
    """Maximum duration of one PostgreSQL statement inside a history bucket."""

    return _bounded_seconds(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_STATEMENT_TIMEOUT_SECONDS",
        _DEFAULT_STATEMENT_TIMEOUT_SECONDS,
        minimum=0.5,
        maximum=6.0,
    )


def cycle_history_bucket_lock_timeout_seconds() -> float:
    """Maximum lock wait while replacing one durable compact-history bucket."""

    return _bounded_seconds(
        "CIE_CONTROL_CYCLE_HISTORY_BUCKET_LOCK_TIMEOUT_SECONDS",
        _DEFAULT_LOCK_TIMEOUT_SECONDS,
        minimum=0.25,
        maximum=2.0,
    )


def _postgres_timeout_statements() -> tuple[str, str]:
    statement_ms = max(1, int(cycle_history_bucket_statement_timeout_seconds() * 1000.0))
    lock_ms = max(1, int(cycle_history_bucket_lock_timeout_seconds() * 1000.0))
    return (
        f"SET LOCAL statement_timeout = {statement_ms}",
        f"SET LOCAL lock_timeout = {lock_ms}",
    )


@contextmanager
def cycle_history_bucket_database_timeout(store: Any) -> Iterator[None]:
    """Apply a short transaction-local timeout only while cycle history advances.

    Canonical control already has a broader database timeout. Production telemetry
    proved that one market-history bucket can consume that entire allowance. This
    scoped hook overrides it only for transactions opened by the durable cycle-history
    cache, so one pathological indexed seek or cache replacement fails closed instead
    of consuming the 25-second executor deadline. The listener is removed immediately
    after the cache slice returns or raises.
    """

    engine = store.engine
    dialect_name = str(getattr(engine.dialect, "name", ""))
    if dialect_name != "postgresql":
        yield
        return

    statements = _postgres_timeout_statements()

    def apply_transaction_timeout(connection: Any) -> None:
        for statement in statements:
            connection.exec_driver_sql(statement)

    event.listen(engine, "begin", apply_transaction_timeout)
    try:
        yield
    finally:
        event.remove(engine, "begin", apply_transaction_timeout)
