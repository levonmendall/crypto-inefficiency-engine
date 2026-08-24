from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _cache_status() -> dict[str, object]:
    from inefficiency_engine.bounded_control_evidence_runtime import (
        bounded_control_outcome_cache_diagnostics,
    )
    from inefficiency_engine.bounded_strategy_evidence_runtime import (
        bounded_strategy_evidence_cache_diagnostics,
    )

    strategy = bounded_strategy_evidence_cache_diagnostics()
    outcomes = bounded_control_outcome_cache_diagnostics()
    complete = bool(
        strategy.get("all_caches_complete")
        and outcomes.get("all_caches_complete")
    )
    return {
        "complete": complete,
        "strategy": strategy,
        "outcomes": outcomes,
    }


def _cache_rebuilding_control_payload(cache: dict[str, object]) -> dict[str, object]:
    """Return a normal fail-closed control result while exact outcome cache advances."""

    return {
        "canonical_control_plane_refresh": True,
        "operating_reconciliation_complete": False,
        "qualified_bridge_publication_complete": False,
        "research_projection_publication_complete": False,
        "historical_cache_complete": False,
        "historical_cache_progress": cache,
        "control_plane_errors": {
            "historical_evidence_cache": "HistoricalEvidenceCacheRebuilding"
        },
        "control_stage_timings_seconds": {},
        "control_plane_healthy": False,
    }


def _cycle_history_rebuilding_control_payload(
    cache: dict[str, object],
    cycle_history: dict[str, object],
    *,
    error_type: str = "CycleHistoryCacheRebuilding",
) -> dict[str, object]:
    """Fail closed while the exact compact long-history projection catches up."""

    return {
        "canonical_control_plane_refresh": True,
        "operating_reconciliation_complete": False,
        "qualified_bridge_publication_complete": False,
        "research_projection_publication_complete": False,
        "historical_cache_complete": False,
        "historical_cache_progress": cache,
        "cycle_history_cache_complete": False,
        "cycle_history_cache_progress": cycle_history,
        "control_plane_errors": {"cycle_history_cache": error_type},
        "control_stage_timings_seconds": {},
        "control_plane_healthy": False,
    }


def run_one_control_cycle() -> dict[str, object]:
    """Run exactly one durable control cycle in this disposable process."""

    from inefficiency_engine.bounded_control_evidence_runtime import (
        advance_bounded_control_outcome_caches,
        install_bounded_control_outcome_ledgers,
    )
    from inefficiency_engine.bounded_strategy_evidence_runtime import (
        install_control_database_timeouts,
    )
    from inefficiency_engine.canonical_control_plane_runtime import (
        refresh_canonical_control_plane,
    )
    from inefficiency_engine.config import Settings
    from inefficiency_engine.control_cycle_runtime import (
        install_control_pool_checkout_timeout,
    )
    from inefficiency_engine.durable_control_cache import (
        ensure_durable_control_cache_schema,
    )
    from inefficiency_engine.durable_control_cycle_history import (
        advance_durable_control_cycle_history_cache,
        ensure_durable_control_cycle_history_schema,
    )
    from inefficiency_engine.durable_source_coverage_runtime import (
        install_control_source_coverage_snapshot_reader_runtime,
    )
    from inefficiency_engine.evidence import build_evidence_store
    from inefficiency_engine.permanent_control_worker import _build_control_services
    from inefficiency_engine.source_runtime_safety import (
        install_source_coverage_reconciliation_runtime,
    )

    status_path = Path(os.environ["CIE_CONTROL_EXECUTOR_STATUS_PATH"])
    cycle_id = os.environ["CIE_CONTROL_EXECUTOR_CYCLE_ID"]
    sequence = int(os.environ["CIE_CONTROL_EXECUTOR_SEQUENCE"])
    deadline = max(
        0.01,
        float(os.getenv("CIE_CONTROL_CYCLE_DEADLINE_SECONDS", "25.0")),
    )
    started = time.monotonic()
    last_progress: dict[str, object] = {}
    cycle_history_progress: dict[str, object] = {}

    def write_stage(stage: str, progress: dict[str, object]) -> None:
        _atomic_json(
            status_path,
            {
                "cycle_id": cycle_id,
                "sequence": sequence,
                "executor_pid": os.getpid(),
                "stage": stage,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "executor_age_seconds": max(0.0, time.monotonic() - started),
                "historical_cache_progress": progress,
                "historical_cache_complete": bool(progress.get("complete")),
                "cycle_history_cache_progress": dict(cycle_history_progress),
                "cycle_history_cache_complete": (
                    bool(cycle_history_progress.get("complete"))
                    if cycle_history_progress
                    else None
                ),
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "paper_only": True,
            },
        )

    def stage_reporter(stage: str) -> None:
        nonlocal last_progress
        last_progress = _cache_status()
        write_stage(stage, last_progress)

    def bridge_stage_reporter(stage: str) -> None:
        # Exact bridge telemetry must not itself consume the remaining control budget.
        # Reuse the most recently computed cache status instead of querying durable
        # cache tables at every diagnostic substage.
        write_stage(stage, last_progress)

    stage_reporter("control_executor_starting")
    install_source_coverage_reconciliation_runtime()
    # Canonical control must consume the complete source snapshot already computed
    # by the priority-source owner. Never rebuild the multi-table source view inside
    # this 25-second executor; missing/stale persisted truth fails closed explicitly.
    install_control_source_coverage_snapshot_reader_runtime()
    install_bounded_control_outcome_ledgers()
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("control executor requires durable evidence persistence")
    ensure_durable_control_cache_schema(store)
    ensure_durable_control_cycle_history_schema(store)
    statement_timeout_seconds = max(5.0, deadline - 5.0)
    lock_timeout_seconds = min(3.0, max(1.0, statement_timeout_seconds / 4.0))
    pool_checkout_timeout_seconds = min(5.0, max(1.0, deadline / 5.0))
    install_control_pool_checkout_timeout(
        store,
        timeout_seconds=pool_checkout_timeout_seconds,
    )
    install_control_database_timeouts(
        store,
        statement_timeout_seconds=statement_timeout_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
    )
    operating_certification, qualified_bridge, research_projection = (
        _build_control_services(settings, store)
    )

    # A disposable executor must never discover a cold historical-outcome bootstrap
    # for the first time inside mechanism readiness. Advance exactly one bounded batch
    # for each exact append-only outcome ledger at a named preflight boundary. If the
    # durable cache is not yet exact, finish this cycle normally but fail closed before
    # operating reconciliation. The next fresh executor resumes from the checkpoint.
    stage_reporter("historical_outcome_cache_bootstrap")
    outcome_cache = advance_bounded_control_outcome_caches(
        mechanism_execution=operating_certification.mechanism_execution,
        allocation_certification=operating_certification.allocation_certification,
    )
    last_progress = _cache_status()
    if not bool(outcome_cache.get("all_caches_complete")):
        write_stage("historical_outcome_cache_rebuilding", last_progress)
        return {
            "ok": True,
            "stage": "control_executor_complete",
            "control": _cache_rebuilding_control_payload(last_progress),
            "alpha_durable_promotion": {
                "provider_requests_used": 0,
                "deferred_for_historical_outcome_cache": True,
            },
            "historical_cache_progress": last_progress,
            "historical_cache_complete": False,
            "database_identity": str(
                getattr(store, "safe_database_url", "durable")
            ),
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        }
    write_stage("historical_outcome_cache_ready", last_progress)

    # The cycle-aware alpha path used to reconstruct its 180-day live compact history
    # with one PostgreSQL row_number window query inside discovery. Production proved
    # that first discovery can consume the whole 25-second control budget. Prime the
    # algebraically equivalent latest-N-per-day projection in bounded primary-key
    # slices before entering canonical reconciliation. A partial cache is durable but
    # invisible to discovery, so no partial history can certify a candidate.
    alpha_factory = getattr(qualified_bridge.allocator, "alpha_factory", None)
    if alpha_factory is not None:
        stage_reporter("cycle_history_cache_source_snapshot")
        source_snapshot = qualified_bridge._latest_scan()
        if source_snapshot is not None:
            stage_reporter("cycle_history_cache_bootstrap")
            try:
                cycle_history_progress = advance_durable_control_cycle_history_cache(
                    alpha_factory,
                    source_snapshot,
                )
            except Exception as exc:
                cycle_history_progress = {
                    "complete": False,
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:500],
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                }
                write_stage("cycle_history_cache_failed", last_progress)
                control = _cycle_history_rebuilding_control_payload(
                    last_progress,
                    cycle_history_progress,
                    error_type="CycleHistoryCacheError",
                )
                return {
                    "ok": True,
                    "stage": "control_executor_complete",
                    "control": control,
                    "alpha_durable_promotion": {
                        "provider_requests_used": 0,
                        "deferred_for_cycle_history_cache": True,
                    },
                    "historical_cache_progress": last_progress,
                    "historical_cache_complete": False,
                    "cycle_history_cache_progress": cycle_history_progress,
                    "cycle_history_cache_complete": False,
                    "database_identity": str(
                        getattr(store, "safe_database_url", "durable")
                    ),
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "paper_only": True,
                }
            if not bool(cycle_history_progress.get("complete")):
                write_stage("cycle_history_cache_rebuilding", last_progress)
                control = _cycle_history_rebuilding_control_payload(
                    last_progress,
                    cycle_history_progress,
                )
                return {
                    "ok": True,
                    "stage": "control_executor_complete",
                    "control": control,
                    "alpha_durable_promotion": {
                        "provider_requests_used": 0,
                        "deferred_for_cycle_history_cache": True,
                    },
                    "historical_cache_progress": last_progress,
                    "historical_cache_complete": False,
                    "cycle_history_cache_progress": cycle_history_progress,
                    "cycle_history_cache_complete": False,
                    "database_identity": str(
                        getattr(store, "safe_database_url", "durable")
                    ),
                    "provider_requests_allowed": False,
                    "provider_requests_used": 0,
                    "paper_only": True,
                }
            write_stage("cycle_history_cache_ready", last_progress)

    set_bridge_reporter = getattr(
        qualified_bridge,
        "set_control_stage_reporter",
        None,
    )
    if callable(set_bridge_reporter):
        set_bridge_reporter(bridge_stage_reporter)
    control = asyncio.run(
        refresh_canonical_control_plane(
            store=store,
            operating_certification=operating_certification,
            qualified_bridge=qualified_bridge,
            research_projection=research_projection,
            settings=settings,
            bridge_snapshot=None,
            stage_reporter=stage_reporter,
            historical_cache_status=_cache_status,
        )
    )
    if cycle_history_progress:
        control = {
            **control,
            "cycle_history_cache_complete": bool(
                cycle_history_progress.get("complete")
            ),
            "cycle_history_cache_progress": cycle_history_progress,
        }
    alpha_factory = getattr(qualified_bridge.allocator, "alpha_factory", None)
    alpha_diagnostics = (
        alpha_factory.durable_promotion_diagnostics()
        if alpha_factory is not None
        and callable(getattr(alpha_factory, "durable_promotion_diagnostics", None))
        else {"provider_requests_used": 0}
    )
    cache = _cache_status()
    all_historical_complete = bool(cache["complete"]) and (
        not cycle_history_progress
        or bool(cycle_history_progress.get("complete"))
    )
    return {
        "ok": True,
        "stage": "control_executor_complete",
        "control": control,
        "alpha_durable_promotion": alpha_diagnostics,
        "historical_cache_progress": cache,
        "historical_cache_complete": all_historical_complete,
        "cycle_history_cache_progress": cycle_history_progress,
        "cycle_history_cache_complete": (
            bool(cycle_history_progress.get("complete"))
            if cycle_history_progress
            else None
        ),
        "database_identity": str(getattr(store, "safe_database_url", "durable")),
        "provider_requests_allowed": False,
        "provider_requests_used": 0,
        "paper_only": True,
    }


def main() -> int:
    from inefficiency_engine.disposable_executor_memory_guard import (
        MEMORY_PRESSURE_EXIT_CODE,
        DisposableMemoryAdmissionDeferred,
        disposable_executor_memory_guard,
    )

    result_path = Path(os.environ["CIE_CONTROL_EXECUTOR_RESULT_PATH"])
    try:
        with disposable_executor_memory_guard("canonical-control-executor"):
            payload = run_one_control_cycle()
    except DisposableMemoryAdmissionDeferred as exc:
        payload = {
            "ok": False,
            "stage": "control_executor_memory_admission",
            "error_type": "ControlExecutorMemoryAdmissionDeferred",
            "message": str(exc)[:500],
            "memory": exc.snapshot.as_dict(),
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        }
        _atomic_json(result_path, payload)
        return MEMORY_PRESSURE_EXIT_CODE
    except BaseException as exc:
        status_path = Path(os.environ["CIE_CONTROL_EXECUTOR_STATUS_PATH"])
        status = {}
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, json.JSONDecodeError):
            pass
        payload: dict[str, Any] = {
            "ok": False,
            "stage": status.get("stage") or "control_executor_starting",
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        }
        _atomic_json(result_path, payload)
        traceback.print_exc()
        return 1
    _atomic_json(result_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
