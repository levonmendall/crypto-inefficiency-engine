from __future__ import annotations

import asyncio
import gc
import os
import time

from inefficiency_engine import __version__
from inefficiency_engine.canonical_paper_portfolio import CANONICAL_INITIAL_CAPITAL_USD
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher as QualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.config import Settings
from inefficiency_engine.critical_evidence_recovery import MECHANISM_FORWARD_WORKER_ID
from inefficiency_engine.dashboard_projection import (
    DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
    ResearchDashboardProjectionLedger,
)
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService as OperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService as AllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService as UnifiedPaperAllocatorService,
)
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.source_runtime_safety import (
    install_bulk_provider_catalog_runtime,
    install_research_source_delegation,
    install_source_coverage_reconciliation_runtime,
)
from inefficiency_engine.universal_service import UniversalOpportunityService


DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS = 30.0
PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"


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


def _portfolio_nav(store) -> float:
    """Read the latest canonical NAV without giving this worker portfolio authority."""

    try:
        heartbeat = store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
    except Exception:
        heartbeat = None
    detail = getattr(heartbeat, "detail", {}) or {}
    value = detail.get("portfolio_nav_usd") if isinstance(detail, dict) else None
    if isinstance(value, (int, float)) and float(value) > 0:
        return float(value)
    return float(CANONICAL_INITIAL_CAPITAL_USD)


async def refresh_canonical_control_plane(
    *,
    store,
    operating_certification,
    qualified_bridge,
    research_projection,
    settings,
) -> dict[str, object]:
    """Advance the durable operating -> bridge -> projection handoff.

    This is deliberately independent from disposable research. It creates no new
    market evidence and grants no execution authority: operating reconciliation reads
    already-persisted source/forward/settlement truth, the bridge republishes only
    currently qualified canonical-settleable candidates, and the projection exposes
    that same durable state to the dashboard.
    """

    result: dict[str, object] = {
        "canonical_control_plane_refresh": True,
        "operating_reconciliation_complete": False,
        "qualified_bridge_publication_complete": False,
        "research_projection_publication_complete": False,
        "control_plane_errors": {},
    }
    errors: dict[str, str] = {}

    try:
        reconciled = await asyncio.to_thread(
            operating_certification.reconcile_latest_runtime_truth
        )
        if reconciled is None:
            errors["operating_reconciliation"] = "OperatingSnapshotUnavailable"
        else:
            result["operating_reconciliation_complete"] = True
            result["operating_snapshot_id"] = reconciled.snapshot_id
            result["operating_observed_at"] = reconciled.observed_at.isoformat()
    except Exception as exc:
        errors["operating_reconciliation"] = type(exc).__name__

    if result["operating_reconciliation_complete"]:
        try:
            bridge = await qualified_bridge.publish_latest(
                total_capital_usd=_portfolio_nav(store)
            )
            result["qualified_bridge_publication_complete"] = True
            result["qualified_bridge_published"] = bridge is not None
            result["qualified_bridge_candidate_count"] = (
                len(bridge.candidates) if bridge is not None else 0
            )
            result["qualified_bridge_observed_at"] = (
                bridge.observed_at.isoformat() if bridge is not None else None
            )
        except Exception as exc:
            errors["qualified_bridge_publication"] = type(exc).__name__

        try:
            payload = await asyncio.to_thread(
                research_projection.publish,
                forward_target=max(1, int(settings.alpha_min_forward_samples)),
                settled_target=max(
                    5,
                    int(
                        getattr(
                            settings,
                            "operating_certification_min_settled_trials",
                            20,
                        )
                    ),
                ),
                shadow_horizons_seconds=tuple(
                    getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,)
                ),
                shadow_cycle_interval_seconds=float(settings.shadow_cycle_interval_seconds),
                alpha_evidence_every_cycles=max(1, int(settings.alpha_evidence_every_cycles)),
                heartbeat_stale_seconds=float(settings.worker_heartbeat_stale_seconds),
            )
            result["research_projection_publication_complete"] = True
            result["research_projection_observed_at"] = payload.get("observed_at")
            store.record_worker_heartbeat(
                worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                state="success",
                detail={
                    "projection_observed_at": payload.get("observed_at"),
                    "publication_stage": "permanent_control_plane_refresh",
                    "operating_reconciled_first": True,
                    "disposable_research_dependency": False,
                    "presentation_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            errors["research_projection_publication"] = type(exc).__name__
            try:
                store.record_worker_heartbeat(
                    worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "publication_stage": "permanent_control_plane_refresh",
                        "operating_reconciled_first": True,
                        "retrying": True,
                        "disposable_research_dependency": False,
                        "presentation_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass

    result["control_plane_errors"] = errors
    result["control_plane_healthy"] = not errors
    return result


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

    # Keep the qualification/portfolio handoff in the same independently supervised
    # permanent plane as forward evidence. These services only consume persisted
    # truth here; network-heavy source acquisition remains in the source process.
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, factory)
    qualified_bridge = QualifiedOpportunityBridgePublisher(service, store, unified)
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        factory,
        allocation_certification,
        version=__version__,
    )
    research_projection = ResearchDashboardProjectionLedger(store)

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
                    "runtime_plane": "mechanism-forward-and-control",
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
                state="running",
                detail={
                    "sequence": sequence,
                    "stage": "canonical_control_plane_refresh",
                    **funnel,
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward-and-control",
                    "disposable_research_dependency": False,
                    "allocation_authority": False,
                    "paper_only": True,
                },
            )
            control = await refresh_canonical_control_plane(
                store=store,
                operating_certification=operating_certification,
                qualified_bridge=qualified_bridge,
                research_projection=research_projection,
                settings=settings,
            )
            errors = control.get("control_plane_errors")
            error_map = errors if isinstance(errors, dict) else {}
            state = "degraded" if error_map else "success"
            error_type = next(iter(error_map.values()), None)
            store.record_worker_heartbeat(
                worker_id=MECHANISM_FORWARD_WORKER_ID,
                state=state,
                error_type=str(error_type) if error_type else None,
                detail={
                    "sequence": sequence,
                    **funnel,
                    **control,
                    "funnel_telemetry": True,
                    "permanent_process": True,
                    "runtime_plane": "mechanism-forward-and-control",
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
                        "runtime_plane": "mechanism-forward-and-control",
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
