from __future__ import annotations

import asyncio
import gc
import os
import time

from inefficiency_engine import __version__
from inefficiency_engine.canonical_control_plane_runtime import refresh_canonical_control_plane
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_projection import ResearchDashboardProjectionLedger
from inefficiency_engine.durable_control_alpha import DurableControlAlphaFactoryService
from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService as OperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService as AllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService as UnifiedPaperAllocatorService,
)
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.universal_service import UniversalOpportunityService


CONTROL_WORKER_ID = "canonical-control-operating-loop"
DEFAULT_CONTROL_INTERVAL_SECONDS = 30.0
DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS = 25.0


def _interval_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_INTERVAL_SECONDS",
                str(DEFAULT_CONTROL_INTERVAL_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_INTERVAL_SECONDS
    return max(5.0, value)


def _deadline_seconds() -> float:
    """Bound one database-only control cycle below its normal publication cadence."""

    try:
        value = float(
            os.getenv(
                "CIE_CONTROL_CYCLE_DEADLINE_SECONDS",
                str(DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CONTROL_CYCLE_DEADLINE_SECONDS
    return max(5.0, value)


def _build_control_services(settings, store):
    """Build the durable control graph without granting acquisition authority."""

    service = OpportunityService(settings=settings, evidence_store=store)
    # This control-specific alpha factory rejects any promotion path that would need
    # a provider request. Current executable cost must come from the persisted source
    # snapshot or the candidate remains fail-closed.
    factory = DurableControlAlphaFactoryService(service, store)
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, factory)
    qualified_bridge = DurableControlQualifiedOpportunityBridgePublisher(
        service,
        store,
        unified,
    )
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        factory,
        allocation_certification,
        version=__version__,
    )
    research_projection = ResearchDashboardProjectionLedger(store)
    return operating_certification, qualified_bridge, research_projection


async def _run() -> None:
    settings = Settings.from_env()
    store = build_evidence_store(settings.evidence_db_path)
    if store is None:
        raise RuntimeError("permanent control worker requires durable evidence persistence")

    operating_certification, qualified_bridge, research_projection = _build_control_services(
        settings,
        store,
    )
    interval = _interval_seconds()
    deadline = _deadline_seconds()
    sequence = 0

    while True:
        sequence += 1
        started = time.monotonic()
        try:
            store.record_worker_heartbeat(
                worker_id=CONTROL_WORKER_ID,
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "durable_reconciliation",
                    "runtime_plane": "canonical-control",
                    "permanent_process": True,
                    "provider_requests_allowed": False,
                    "current_execution_cost_source": "persisted_order_books_only",
                    "missing_current_executable_depth_policy": "fail_closed",
                    "cycle_deadline_seconds": deadline,
                    "mechanism_forward_dependency": False,
                    "disposable_research_dependency": False,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )

            control = await asyncio.wait_for(
                refresh_canonical_control_plane(
                    store=store,
                    operating_certification=operating_certification,
                    qualified_bridge=qualified_bridge,
                    research_projection=research_projection,
                    settings=settings,
                    bridge_snapshot=None,
                ),
                timeout=deadline,
            )
            errors = control.get("control_plane_errors")
            error_map = errors if isinstance(errors, dict) else {}
            state = "degraded" if error_map else "success"
            error_type = next(iter(error_map.values()), None)
            alpha_factory = getattr(qualified_bridge.allocator, "alpha_factory", None)
            alpha_diagnostics = (
                alpha_factory.durable_promotion_diagnostics()
                if alpha_factory is not None
                and callable(getattr(alpha_factory, "durable_promotion_diagnostics", None))
                else {}
            )
            store.record_worker_heartbeat(
                worker_id=CONTROL_WORKER_ID,
                state=state,
                error_type=str(error_type) if error_type else None,
                detail={
                    "sequence": sequence,
                    **control,
                    "stage": "durable_control_complete",
                    "runtime_plane": "canonical-control",
                    "permanent_process": True,
                    "provider_requests_allowed": False,
                    "current_execution_cost_source": "persisted_order_books_only",
                    "missing_current_executable_depth_policy": "fail_closed",
                    "cycle_deadline_seconds": deadline,
                    "cycle_runtime_seconds": max(0.0, time.monotonic() - started),
                    "alpha_durable_promotion": alpha_diagnostics,
                    "mechanism_forward_dependency": False,
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
                    worker_id=CONTROL_WORKER_ID,
                    state="error",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": sequence,
                        "stage": "durable_control_failed",
                        "message": str(exc)[:500],
                        "runtime_plane": "canonical-control",
                        "permanent_process": True,
                        "provider_requests_allowed": False,
                        "current_execution_cost_source": "persisted_order_books_only",
                        "missing_current_executable_depth_policy": "fail_closed",
                        "cycle_deadline_seconds": deadline,
                        "cycle_runtime_seconds": max(0.0, time.monotonic() - started),
                        "mechanism_forward_dependency": False,
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
