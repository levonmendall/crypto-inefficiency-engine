from __future__ import annotations

import asyncio
import gc
import os
import time

from inefficiency_engine.config import Settings
from inefficiency_engine.critical_evidence_recovery import MECHANISM_FORWARD_WORKER_ID
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.source_runtime_safety import (
    install_bulk_provider_catalog_runtime,
    install_research_source_delegation,
    install_source_coverage_reconciliation_runtime,
)


DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS = 30.0


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
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward",
                    "allocation_authority": False,
                    "paper_only": True,
                },
            )

            original_evidence = service.collect_live_evidence
            original_executability = getattr(service, "collect_live_executability", None)
            snapshot = await factory.refresh_l2_source_snapshot(original_evidence)

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
                    "funnel_telemetry": True,
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward",
                    "disposable_research_dependency": False,
                    "qualification_thresholds_unchanged": True,
                    "allocation_authority": False,
                    "paper_only": True,
                    "live_execution_authority": False,
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
                        "message": str(exc)[:500],
                        "permanent_process": True,
                        "runtime_plane": "mechanism-forward",
                        "disposable_research_dependency": False,
                        "qualification_thresholds_unchanged": True,
                        "allocation_authority": False,
                        "paper_only": True,
                        "live_execution_authority": False,
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
