"""Crypto Opportunity Engine."""

from __future__ import annotations

import gc
import sys
from typing import Any

__version__ = "3.8.3"

_STAGE_ONE_MODULE = "inefficiency_engine.stage_one_local_persistence_migration"
_STAGE_ONE_MAX_BATCH_SIZE = 256
_STAGE_ONE_CAPTURED_APPEND_ONLY_TABLES = {
    "cycle_historical_quotes",
    "dashboard_projection_snapshots",
    "funding_quotes",
    "source_coverage_history",
    "source_event_observations",
    "worker_heartbeats",
}
_STAGE_ONE_MONOTONIC_HIGH_WATER_TABLES = {
    "funding_quotes",
    "source_event_observations",
    "worker_heartbeats",
}


def _running_stage_one_migration() -> bool:
    """Detect the exact ``python -m`` migration child before its module is imported."""

    return _STAGE_ONE_MODULE in tuple(getattr(sys, "orig_argv", ()))


def _install_stage_one_runtime_memory_guard() -> None:
    """Keep Stage 1 retries inside the 2 GiB Render cgroup memory budget.

    The migration-specific entrypoint already owns the retry semantics. This early hook
    bounds each relational batch, routes proven append-only Stage 1 ledgers through
    restart-safe finite capture paths, and makes a *same-process* retry skip integrity
    rescans of tables that the immediately preceding attempt already verified. A fresh
    migration process still performs every full integrity check, so restart behavior
    remains fail-closed.
    """

    from inefficiency_engine import postgres_local_migration as migration
    from inefficiency_engine.stage_one_monotonic_append_only import (
        migrate_monotonic_integer_append_only_table,
    )

    # These tables have immutable INSERT-only writers. Stage 1 therefore does not need
    # the generic mutable-table repeatable-read payload transaction. Hash/non-monotonic
    # ledgers retain the exact captured-membership implementation installed by the
    # Stage 1 entrypoint; proven monotonic integer ledgers can use a durable high-water
    # plus bounded keyset reads instead.
    migration.RESUMABLE_APPEND_ONLY_TABLES.update(_STAGE_ONE_CAPTURED_APPEND_ONLY_TABLES)

    current = migration.migrate_engines
    if getattr(current, "_cie_stage_one_memory_guard", False):
        return

    calls = 0

    def guarded_migrate_engines(
        source: Any,
        target: Any,
        history: Any,
        *,
        progress_path: Any,
        batch_size: int = migration.BATCH_SIZE,
        interrupt_after_batches: int | None = None,
    ) -> dict[str, object]:
        nonlocal calls

        retry_resume = calls > 0
        calls += 1
        bounded_batch_size = max(1, min(int(batch_size), _STAGE_ONE_MAX_BATCH_SIZE))

        verified_target = migration._verified_target_is_intact
        resumable_append_only = migration._migrate_resumable_append_only_table

        if retry_resume:
            target_engine = getattr(target, "engine", None)
            if target_engine is not None and hasattr(target_engine, "dispose"):
                target_engine.dispose()
            gc.collect()

            def verified_target_without_rescan(
                target_engine: Any,
                table: Any,
                shared: list[str],
                table_report: dict[str, Any],
            ) -> bool:
                if table_report.get("verified") is True:
                    return True
                return verified_target(target_engine, table, shared, table_report)

            migration._verified_target_is_intact = verified_target_without_rescan

        def routed_append_only(
            source_engine: Any,
            target_engine: Any,
            source_table: Any,
            target_table: Any,
            shared: list[str],
            table_report: dict[str, Any],
            report: dict[str, Any],
            progress: Any,
            *,
            batch_size: int,
            completed_batches: int,
            interrupt_after_batches: int | None,
        ) -> int:
            if retry_resume and table_report.get("verified") is True:
                return completed_batches
            if source_table.name in _STAGE_ONE_MONOTONIC_HIGH_WATER_TABLES:
                return migrate_monotonic_integer_append_only_table(
                    source_engine,
                    target_engine,
                    source_table,
                    target_table,
                    shared,
                    table_report,
                    report,
                    progress,
                    batch_size=batch_size,
                    completed_batches=completed_batches,
                    interrupt_after_batches=interrupt_after_batches,
                )
            return resumable_append_only(
                source_engine,
                target_engine,
                source_table,
                target_table,
                shared,
                table_report,
                report,
                progress,
                batch_size=batch_size,
                completed_batches=completed_batches,
                interrupt_after_batches=interrupt_after_batches,
            )

        migration._migrate_resumable_append_only_table = routed_append_only
        try:
            return current(
                source,
                target,
                history,
                progress_path=progress_path,
                batch_size=bounded_batch_size,
                interrupt_after_batches=interrupt_after_batches,
            )
        finally:
            migration._migrate_resumable_append_only_table = resumable_append_only
            if retry_resume:
                migration._verified_target_is_intact = verified_target

    guarded_migrate_engines._cie_stage_one_memory_guard = True  # type: ignore[attr-defined]
    migration.migrate_engines = guarded_migrate_engines


if _running_stage_one_migration():
    _install_stage_one_runtime_memory_guard()
