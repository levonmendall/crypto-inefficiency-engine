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
_STAGE_ONE_MONOTONIC_MIGRATION_MODE = "captured_monotonic_integer_high_water"
_STAGE_ONE_RESUMABLE_MONOTONIC_PHASES = {
    "high_water_captured",
    "copying_snapshot",
    "verifying_snapshot",
}
_STAGE_ONE_PRIORITY_RESUME_FAILURE_PHASE = "priority_monotonic_resume"


def _running_stage_one_migration() -> bool:
    """Detect the exact ``python -m`` migration child before its module is imported."""

    return _STAGE_ONE_MODULE in tuple(getattr(sys, "orig_argv", ()))


def _durable_monotonic_resume_candidates(progress: dict[str, Any]) -> list[str]:
    """Return unfinished monotonic snapshots that already own a durable boundary.

    A fresh Stage 1 child must honor persisted restart-safe work before walking the
    schema from the beginning. This does not skip later integrity checks; it only makes
    the durable unfinished checkpoint the first unit of work after a process restart.
    """

    tables = progress.get("tables")
    if not isinstance(tables, dict):
        return []
    candidates: list[str] = []
    for name in sorted(_STAGE_ONE_MONOTONIC_HIGH_WATER_TABLES):
        table_report = tables.get(name)
        if not isinstance(table_report, dict):
            continue
        if table_report.get("verified") is True:
            continue
        if table_report.get("migration_mode") != _STAGE_ONE_MONOTONIC_MIGRATION_MODE:
            continue
        if table_report.get("snapshot_high_water_captured") is not True:
            continue
        if table_report.get("snapshot_phase") not in _STAGE_ONE_RESUMABLE_MONOTONIC_PHASES:
            continue
        candidates.append(name)
    return candidates


def _persist_priority_resume_failure(
    migration: Any,
    progress_path: Any,
    exc: BaseException,
) -> None:
    """Publish priority-resume terminal truth without discarding its durable checkpoint."""

    progress = migration._load_progress(progress_path)
    current_table = str(progress.get("current_table") or "").strip() or None
    progress.update(
        state="failed",
        error_type=type(exc).__name__,
        error=str(exc),
        failure_phase=_STAGE_ONE_PRIORITY_RESUME_FAILURE_PHASE,
        failure_table=current_table,
        paper_only=True,
        live_execution_authority=False,
        forward_evidence_granted=False,
        postgresql_authoritative=True,
        cutover_ready=False,
    )
    migration._publish(progress, progress_path)


def _resume_durable_monotonic_checkpoints_first(
    migration: Any,
    migrate_monotonic_integer_append_only_table: Any,
    source: Any,
    target: Any,
    *,
    progress_path: Any,
    batch_size: int,
    interrupt_after_batches: int | None,
) -> None:
    """Finish durable monotonic checkpoints before normal schema traversal.

    Normal ``migrate_engines`` still runs afterward and therefore retains its complete
    fresh-process integrity scan. The priority pass only prevents a 54/55 checkpoint
    from being stranded behind rescans of dozens of already completed tables.
    """

    progress = migration._load_progress(progress_path)
    candidates = _durable_monotonic_resume_candidates(progress)
    if not candidates:
        return

    source_metadata = migration.MetaData()
    source_metadata.reflect(source)
    migration.bootstrap_local_schema_from_source(source_metadata, target.engine)
    target_metadata = migration.MetaData()
    target_metadata.reflect(target.engine)

    tables = progress.get("tables")
    if not isinstance(tables, dict):
        return

    progress.pop("error", None)
    progress.pop("error_type", None)
    progress.pop("failure_phase", None)
    progress.pop("failure_table", None)
    progress.update(
        state="running",
        paper_only=True,
        live_execution_authority=False,
        forward_evidence_granted=False,
        postgresql_authoritative=True,
        cutover_ready=False,
        verification_scope="captured_primary_key_high_water",
    )
    completed_batches = 0
    for name in candidates:
        if name not in source_metadata.tables or name not in target_metadata.tables:
            raise RuntimeError(f"durable resume table missing from reflected schema: {name}")
        source_table = source_metadata.tables[name]
        target_table = target_metadata.tables[name]
        primary_key = migration._primary_key(source_table)
        shared = [column.name for column in source_table.columns if column.name in target_table.c]
        if any(column.name not in shared for column in primary_key):
            raise RuntimeError(f"target schema missing primary key columns for {name}")
        table_report = tables[name]
        progress["current_table"] = name
        migration._publish(progress, progress_path)
        completed_batches = migrate_monotonic_integer_append_only_table(
            source,
            target.engine,
            source_table,
            target_table,
            shared,
            table_report,
            progress,
            progress_path,
            batch_size=batch_size,
            completed_batches=completed_batches,
            interrupt_after_batches=interrupt_after_batches,
        )


def _install_stage_one_runtime_memory_guard() -> None:
    """Keep Stage 1 retries inside the 2 GiB Render cgroup memory budget.

    The migration-specific entrypoint already owns the retry semantics. This early hook
    bounds each relational batch, routes proven append-only Stage 1 ledgers through
    restart-safe finite capture paths, prioritizes durable unfinished monotonic
    checkpoints after a process restart, and makes a *same-process* retry skip integrity
    rescans of tables that the immediately preceding attempt already verified. A fresh
    migration process still performs every full integrity check before Stage 1 can finish.
    """

    from inefficiency_engine import postgres_local_migration as migration
    from inefficiency_engine.stage_one_monotonic_append_only import (
        migrate_monotonic_integer_append_only_table,
    )

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
            try:
                _resume_durable_monotonic_checkpoints_first(
                    migration,
                    migrate_monotonic_integer_append_only_table,
                    source,
                    target,
                    progress_path=progress_path,
                    batch_size=bounded_batch_size,
                    interrupt_after_batches=interrupt_after_batches,
                )
            except Exception as exc:
                _persist_priority_resume_failure(migration, progress_path, exc)
                raise
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
