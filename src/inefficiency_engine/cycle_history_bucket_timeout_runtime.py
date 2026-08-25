from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import delete, event, insert, select


_DEFAULT_STATEMENT_TIMEOUT_SECONDS = 4.0
_DEFAULT_LOCK_TIMEOUT_SECONDS = 1.0
_DEFAULT_CONTROL_EXECUTOR_SLICE_SECONDS = 3.0
_CONTROL_EXECUTOR_BUCKET_QUERY_CAP = 1
_BUCKET_QUERY_BUDGET_ENV = "CIE_CONTROL_CYCLE_HISTORY_BUCKET_QUERY_BUDGET"


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


def control_executor_cycle_history_slice_seconds() -> float:
    """Soft work slice used only by the disposable canonical-control child.

    Production control reaches cycle-history bootstrap late in its unchanged 25-second
    lifetime. The durable history cache is resumable, so one child should make a small
    checkpointed advance and return normally rather than spending the cache's ordinary
    eight-second bootstrap allowance and being killed by the external supervisor.
    """

    return _bounded_seconds(
        "CIE_CONTROL_CYCLE_HISTORY_EXECUTOR_SLICE_SECONDS",
        _DEFAULT_CONTROL_EXECUTOR_SLICE_SECONDS,
        minimum=1.0,
        maximum=4.0,
    )


def _postgres_timeout_statements() -> tuple[str, str, str]:
    statement_ms = max(1, int(cycle_history_bucket_statement_timeout_seconds() * 1000.0))
    lock_ms = max(1, int(cycle_history_bucket_lock_timeout_seconds() * 1000.0))
    return (
        f"SET LOCAL statement_timeout = {statement_ms}",
        f"SET LOCAL lock_timeout = {lock_ms}",
        "SET LOCAL enable_seqscan = off",
    )


@contextmanager
def _single_bucket_query_budget() -> Iterator[None]:
    """Temporarily cap one disposable control child to one durable history bucket."""

    previous = os.environ.get(_BUCKET_QUERY_BUDGET_ENV)
    os.environ[_BUCKET_QUERY_BUDGET_ENV] = str(_CONTROL_EXECUTOR_BUCKET_QUERY_CAP)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(_BUCKET_QUERY_BUDGET_ENV, None)
        else:
            os.environ[_BUCKET_QUERY_BUDGET_ENV] = previous


def advance_control_executor_cycle_history_cache(
    advance: Any,
    factory: Any,
    snapshot: Any,
    *,
    stop_at_monotonic: float | None = None,
) -> dict[str, object]:
    """Advance at most one checkpointed bucket inside a short control-child slice.

    The exact cache remains fail-closed and resumes from its durable checkpoint on the
    next fresh control interpreter. This wrapper changes only how much maintenance one
    disposable child may attempt; evidence requirements, the external 25-second
    deadline, PostgreSQL statement/lock limits, and allocation authority are untouched.
    """

    slice_seconds = control_executor_cycle_history_slice_seconds()
    local_stop = time.monotonic() + slice_seconds
    if stop_at_monotonic is not None:
        local_stop = min(local_stop, float(stop_at_monotonic))

    with _single_bucket_query_budget():
        progress = dict(
            advance(
                factory,
                snapshot,
                stop_at_monotonic=local_stop,
            )
        )

    progress.update(
        {
            "control_executor_slice_seconds": slice_seconds,
            "control_executor_bucket_query_cap": _CONTROL_EXECUTOR_BUCKET_QUERY_CAP,
            "control_executor_supervisor_safe_slice": True,
            "external_process_deadline_unchanged": True,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        }
    )
    return progress


def _index_aligned_replace_bucket(
    *,
    factory: Any,
    namespace: str,
    venue: str,
    asset: str,
    day: Any,
    start: Any,
    end: Any,
    limit: int,
) -> int:
    """Replace one compact bucket with an index-aligned chronological top-N seek.

    Production may have the optional ``market_quotes(venue, asset, observed_at, id)``
    runtime index. A lightweight BRIN on ``observed_at`` is also maintained after API
    bind so the same exact range predicate can avoid a full ledger scan while the larger
    btree is unavailable. The query keeps exact filter-before-rank semantics in either
    case. Rank by ``observed_at DESC, id DESC`` so the newest observations are exact and
    ``id`` is only a deterministic tie-breaker.
    """

    from inefficiency_engine import durable_control_cycle_history as legacy
    from inefficiency_engine.models import MarketQuote

    table = factory.store.market_quotes
    selected: list[dict[str, object]] = []
    if end > start and limit > 0:
        id_query = (
            select(table.c.id)
            .where(table.c.venue == venue)
            .where(table.c.asset == asset)
            .where(table.c.observed_at >= start.isoformat())
            .where(table.c.observed_at < end.isoformat())
            .order_by(table.c.observed_at.desc(), table.c.id.desc())
            .limit(limit)
        )
        with factory.store.engine.connect() as db:
            source_ids = [int(source_id) for source_id in db.scalars(id_query)]
            rows = (
                list(
                    db.execute(
                        select(
                            table.c.id,
                            table.c.venue,
                            table.c.asset,
                            table.c.observed_at,
                            table.c.payload_json,
                        ).where(table.c.id.in_(source_ids))
                    )
                )
                if source_ids
                else []
            )
        rows.sort(key=lambda row: (str(row[3]), int(row[0])), reverse=True)
        for source_id, row_venue, row_asset, observed_at, payload_json in rows:
            quote = MarketQuote.model_validate_json(payload_json)
            selected.append(
                {
                    "namespace": namespace,
                    "source_id": int(source_id),
                    "venue": str(row_venue),
                    "asset": str(row_asset).upper(),
                    "market_kind": quote.market_kind.value,
                    "day": day.isoformat(),
                    "observed_at": str(observed_at),
                    "payload_json": str(payload_json),
                }
            )

    with factory.store.engine.begin() as db:
        db.execute(
            delete(legacy.CONTROL_CYCLE_HISTORY_ROWS)
            .where(legacy.CONTROL_CYCLE_HISTORY_ROWS.c.namespace == namespace)
            .where(legacy.CONTROL_CYCLE_HISTORY_ROWS.c.venue == venue)
            .where(legacy.CONTROL_CYCLE_HISTORY_ROWS.c.asset == asset)
            .where(legacy.CONTROL_CYCLE_HISTORY_ROWS.c.day == day.isoformat())
        )
        if selected:
            db.execute(insert(legacy.CONTROL_CYCLE_HISTORY_ROWS), selected)
    return len(selected)


def install_index_aligned_cycle_history_bucket_runtime() -> None:
    """Install the exact bucket reader in both legacy and frozen-target runtimes."""

    from inefficiency_engine import durable_control_cycle_history as legacy
    from inefficiency_engine import durable_control_cycle_history_target_runtime as target_runtime

    legacy._replace_bucket = _index_aligned_replace_bucket
    target_runtime._replace_bucket = _index_aligned_replace_bucket


@contextmanager
def cycle_history_bucket_database_timeout(store: Any) -> Iterator[None]:
    """Apply bounded transaction-local controls only while cycle history advances.

    Canonical control already has a broader database timeout. Production telemetry
    proved that one market-history bucket can consume that entire allowance. This scoped
    hook retains the short statement/lock bounds and makes PostgreSQL prefer available
    range indexes over a sequential scan. ``enable_seqscan=off`` is planner guidance,
    not a correctness shortcut: PostgreSQL can still use a sequential scan when no other
    path exists, and the setting disappears with the transaction. The listener is
    removed immediately after the cache slice returns or raises.
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


# The external supervisor supplies this id only to the disposable canonical-control
# child. Install the runtime repair there, after aggregate-memory admission but before
# run_one_control_cycle imports the durable cache symbols. Ordinary test/import paths
# and every other worker keep the original module untouched.
if os.getenv("CIE_CONTROL_EXECUTOR_CYCLE_ID"):
    from inefficiency_engine import durable_control_cycle_history as _legacy_cycle_history
    from inefficiency_engine.cycle_history_active_target_fallback_runtime import (
        advance_durable_control_cycle_history_cache as _advance_frozen_cycle_history,
        load_durable_control_cycle_history as _load_frozen_cycle_history,
    )

    def _advance_supervisor_safe_cycle_history(
        factory: Any,
        snapshot: Any,
        *,
        stop_at_monotonic: float | None = None,
    ) -> dict[str, object]:
        return advance_control_executor_cycle_history_cache(
            _advance_frozen_cycle_history,
            factory,
            snapshot,
            stop_at_monotonic=stop_at_monotonic,
        )

    install_index_aligned_cycle_history_bucket_runtime()
    _legacy_cycle_history.advance_durable_control_cycle_history_cache = (
        _advance_supervisor_safe_cycle_history
    )
    _legacy_cycle_history.load_durable_control_cycle_history = _load_frozen_cycle_history
