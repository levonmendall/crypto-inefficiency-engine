from __future__ import annotations

import time

from sqlalchemy.exc import OperationalError

from inefficiency_engine import _install_stage_one_runtime_memory_guard
from inefficiency_engine import partitioned_market_history
from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine import stage_one_local_persistence_migration as stage_one
from inefficiency_engine.stage_one_bounded_coarse_market_history import (
    BoundedStageOneCoarsePartitionedMarketHistory as CoarsePartitionedMarketHistory,
)
from inefficiency_engine.stage_one_market_memory_guard import (
    allocator_trim_loop,
    compaction_batch_rows,
    install_market_copy_guard,
)


def _durable_migration_is_terminal() -> bool:
    report = migration._load_progress(migration._progress_path())
    return str(report.get("state") or "").lower() in {"failed", "interrupted", "verified"}


def _record_startup_source_retry(*, delay: float) -> None:
    progress_path = migration._progress_path()
    report = migration._load_progress(progress_path)
    current_table = report.get("current_table")
    tables = report.get("tables")
    if not isinstance(current_table, str) or not isinstance(tables, dict):
        return
    table_report = tables.get(current_table)
    if not isinstance(table_report, dict):
        return
    table_report.update(
        source_transport_retries=int(table_report.get("source_transport_retries") or 0) + 1,
        last_source_retry_phase="stage_one_source_metadata_reflection",
        last_source_retry_delay_seconds=delay,
        last_source_retry_recovered=False,
    )
    migration._publish(report, progress_path)


def _run_with_bounded_startup_source_retry() -> int:
    """Retry only transient PostgreSQL startup/reflection failures before terminal truth.

    The canonical migration already retries bounded restart-safe market/source reads.
    A child restart can fail earlier, while SQLAlchemy reflects PostgreSQL metadata,
    before that retry boundary or the migration try/except is entered. Reuse the same
    transient classifier and delay ceiling here, but never retry after durable terminal
    migration truth has been published.
    """

    retries = 0
    delays = migration.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS
    while True:
        try:
            result = migration.main()
            if retries:
                progress_path = migration._progress_path()
                report = migration._load_progress(progress_path)
                current_table = report.get("current_table")
                tables = report.get("tables")
                if isinstance(current_table, str) and isinstance(tables, dict):
                    table_report = tables.get(current_table)
                    if isinstance(table_report, dict):
                        table_report["last_source_retry_recovered"] = True
                        migration._publish(report, progress_path)
            return result
        except OperationalError as exc:
            if (
                _durable_migration_is_terminal()
                or not migration._is_transient_source_read_error(exc)
                or retries >= len(delays)
            ):
                raise
            delay = delays[retries]
            retries += 1
            _record_startup_source_retry(delay=delay)
            time.sleep(delay)


def main() -> int:
    """Run canonical Stage 1 with bounded copy, verification, and startup source recovery."""

    stage_one.install_stage_one_repair()
    _install_stage_one_runtime_memory_guard()
    migration.PartitionedMarketHistory = CoarsePartitionedMarketHistory
    partitioned_market_history.COMPACTION_BATCH_ROWS = compaction_batch_rows()
    install_market_copy_guard(migration)
    with allocator_trim_loop():
        return _run_with_bounded_startup_source_retry()


if __name__ == "__main__":
    raise SystemExit(main())
