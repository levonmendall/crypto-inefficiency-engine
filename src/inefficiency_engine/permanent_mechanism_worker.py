from __future__ import annotations

import asyncio
import gc
import os
import time
from datetime import datetime, timezone

from inefficiency_engine.canonical_control_plane_runtime import refresh_canonical_control_plane
from inefficiency_engine.config import Settings
from inefficiency_engine.critical_evidence_recovery import MECHANISM_FORWARD_WORKER_ID
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.execution import qualify_opportunity
from inefficiency_engine.service import OpportunityService, _books_for_opportunity
from inefficiency_engine.source_runtime_safety import (
    install_bulk_provider_catalog_runtime,
    install_research_source_delegation,
    install_source_coverage_reconciliation_runtime,
)


DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS = 30.0
DEFAULT_MECHANISM_SOURCE_MAX_AGE_SECONDS = 120.0


def _interval_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_MECHANISM_FORWARD_INTERVAL_SECONDS",
                str(DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS
    return max(5.0, value)


def _source_max_age_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_MECHANISM_SOURCE_MAX_AGE_SECONDS",
                str(DEFAULT_MECHANISM_SOURCE_MAX_AGE_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_MECHANISM_SOURCE_MAX_AGE_SECONDS
    return max(30.0, value)


def mechanism_forward_funnel(execution, cycle) -> dict[str, object]:
    readiness = execution.readiness_summary()
    rows = [row for row in readiness.values() if isinstance(row, dict)]
    return {
        "mechanism_count": len(rows),
        "current_spec_count": int(cycle.current_specs),
        "trials_recorded": int(cycle.trials_recorded),
        "outcomes_matured": int(cycle.outcomes_matured),
        "forward_outcome_count": sum(int(row.get("forward_outcome_count") or 0) for row in rows),
        "incremental_qualified_cohort_count": sum(
            int(row.get("incremental_qualified_cohort_count") or 0) for row in rows
        ),
        "full_qualified_cohort_count": sum(
            int(row.get("full_qualified_cohort_count") or 0) for row in rows
        ),
        "currently_qualified_mechanism_count": sum(
            1 for row in rows if bool(row.get("currently_qualified"))
        ),
        "current_promoted_candidate_count": sum(
            int(row.get("current_promoted_candidate_count") or 0) for row in rows
        ),
        "cycle_promoted_candidate_count": int(cycle.promoted_candidates),
        "by_mechanism": readiness,
    }


def bridge_snapshot_from_source(service: OpportunityService, snapshot):
    """Compatibility helper for deterministic executability reconstruction.

    The permanent mechanism loop no longer publishes the canonical bridge. This
    helper remains available for tests and disposable callers that need to attach
    fail-closed executability to an already-collected snapshot without another
    provider request.
    """

    latency_resolver = service.empirical_latency_resolver()
    executability = [
        qualify_opportunity(
            opportunity,
            _books_for_opportunity(opportunity, list(snapshot.order_books)),
            service.settings,
            notionals_usd=service.settings.capital_tiers_usd,
            now=snapshot.completed_at,
            latency_model_resolver=latency_resolver.resolve,
        )
        for opportunity in snapshot.opportunities
    ]
    return snapshot.model_copy(update={"executability": executability})


def _current_durable_source_snapshot(factory: DisposableExpandedAlphaFactoryService):
    """Return current permanent-source evidence or fail closed without network fallback."""

    snapshot = factory._latest_permanent_source_snapshot()  # noqa: SLF001 - same runtime package
    if snapshot is None:
        raise RuntimeError("PermanentSourceSnapshotUnavailable")
    observed_at = snapshot.completed_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    age_seconds = max(
        0.0,
        (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
    )
    max_age = _source_max_age_seconds()
    if age_seconds > max_age:
        raise RuntimeError(
            f"PermanentSourceSnapshotStale:{age_seconds:.1f}s>{max_age:.1f}s"
        )
    return snapshot


async def _run() -> None:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("permanent mechanism-forward worker requires durable evidence persistence")

    install_bulk_provider_catalog_runtime()
    install_source_coverage_reconciliation_runtime()
    install_research_source_delegation()
    service = OpportunityService(settings=settings, evidence_store=store)
    factory = DisposableExpandedAlphaFactoryService(service, store)
    execution = factory.mechanism_execution

    interval = _interval_seconds()
    sequence = 0

    while True:
        sequence += 1
        started = time.monotonic()
        try:
            store.record_worker_heartbeat(
                worker_id=MECHANISM_FORWARD_WORKER_ID,
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "forward_evidence",
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward",
                    "canonical_control_owned_elsewhere": True,
                    "provider_acquisition_owned_elsewhere": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )

            snapshot = await asyncio.to_thread(_current_durable_source_snapshot, factory)
            original_evidence = service.collect_live_evidence
            original_executability = getattr(service, "collect_live_executability", None)

            async def cached_snapshot():
                return snapshot

            service.collect_live_evidence = cached_snapshot
            if original_executability is not None:
                service.collect_live_executability = cached_snapshot
            try:
                cycle = await execution.run_evidence_cycle()
            finally:
                service.collect_live_evidence = original_evidence
                if original_executability is not None:
                    service.collect_live_executability = original_executability

            funnel = mechanism_forward_funnel(execution, cycle)
            store.record_worker_heartbeat(
                worker_id=MECHANISM_FORWARD_WORKER_ID,
                state="success",
                detail={
                    "sequence": sequence,
                    **funnel,
                    "stage": "forward_evidence_complete",
                    "funnel_telemetry": True,
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward",
                    "canonical_control_owned_elsewhere": True,
                    "provider_acquisition_owned_elsewhere": True,
                    "disposable_research_dependency": False,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
            sleep_seconds = max(0.25, interval - (time.monotonic() - started))
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=MECHANISM_FORWARD_WORKER_ID,
                    state="error",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": sequence,
                        "stage": "forward_evidence_failed",
                        "message": str(exc)[:500],
                        "permanent_process": True,
                        "runtime_plane": "mechanism-forward",
                        "canonical_control_owned_elsewhere": True,
                        "provider_acquisition_owned_elsewhere": True,
                        "disposable_research_dependency": False,
                        "qualification_thresholds_unchanged": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass
            sleep_seconds = max(1.0, float(settings.worker_error_backoff_seconds))
        gc.collect()
        await asyncio.sleep(sleep_seconds)


def main() -> int:
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
