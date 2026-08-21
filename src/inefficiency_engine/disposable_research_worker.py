from __future__ import annotations

import gc
from types import SimpleNamespace

from inefficiency_engine import __version__
from inefficiency_engine.bounded_shadow_service import MemoryBoundedShadowService
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher as QualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.certification_worker import run_certification_loop
from inefficiency_engine.dashboard_projection import (
    DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
    ResearchDashboardProjectionLedger,
)
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService as OperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService as AllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService as UnifiedPaperAllocatorService,
)
from inefficiency_engine.research_closure_worker import run_research_closure_cycle
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats


RESEARCH_WORKER_ID = "shadow-research-auxiliary"


def _due(sequence: int, every: int, offset_fraction: float = 0.0) -> bool:
    every = max(1, int(every))
    offset = int(every * offset_fraction)
    offset = max(1, min(every, offset if offset > 0 else every))
    return (sequence - offset) % every == 0


def _error_keys(detail: dict[str, object]) -> list[str]:
    return sorted(key for key in detail if key.endswith("_error_type"))


async def _run_release(runner):
    try:
        return await runner()
    finally:
        gc.collect()


async def run_disposable_research_cycle(
    service: OpportunityService,
    store: EvidenceStore,
    *,
    sequence: int,
) -> WorkerRunStats:
    """Run exactly one integrated research cycle, persist results, then exit.

    The disposable process is the production Render research path. It therefore
    installs the all-lane evidence factory, evidence-velocity source boundary, and
    Release D subtractive lane-success allocator/certification together. Final
    statistical, source, execution, risk, settlement, and profitability thresholds
    remain unchanged.
    """

    sequence = max(1, int(sequence))
    bounded_shadow = MemoryBoundedShadowService(service, store)
    universal = UniversalOpportunityService(service)
    tier_shadow = DexTierShadowService(service, evidence_store=store)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    composite_shadow = CexDexCompositeEdgeShadowService(
        composite_service,
        evidence_store=store,
    )
    stablecoin_shadow = StablecoinDepthShadowService(
        StablecoinConversionDepthService(service.settings),
        evidence_store=store,
    )
    alpha_factory = DisposableExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
    qualified_bridge = QualifiedOpportunityBridgePublisher(service, store, unified)
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        alpha_factory,
        allocation_certification,
        version=__version__,
    )
    research_projection = ResearchDashboardProjectionLedger(store)

    tier_every = max(1, int(service.settings.dex_route_tier_shadow_every_cycles))
    composite_every = 10
    stablecoin_every = 10
    alpha_every = max(1, int(service.settings.alpha_evidence_every_cycles))
    certification_every = alpha_every
    frontier_every = max(1, int(service.settings.dex_route_frontier_every_cycles))

    detail: dict[str, object] = {
        "sequence": sequence,
        "disposable_process": True,
        "history_backfill_inline": False,
        "sequential_research_surfaces": True,
        "all_lane_evidence_velocity_runtime": True,
        "release_d_lane_success_runtime": True,
        "paper_only": True,
    }
    store.record_worker_heartbeat(
        worker_id=RESEARCH_WORKER_ID,
        state="running",
        detail=detail,
    )

    # Bootstrap source truth periodically. The alpha/mechanism cycle performs an
    # additional source refresh immediately before evidence generation, so short-
    # lived trade-flow/liquidation evidence cannot expire merely because of the
    # sequential disposable scheduler.
    if sequence == 1 or sequence % 10 == 1:
        try:
            bootstrap = await operating_certification.provider_gap_collection.run_cycle()
            source_coverage = bootstrap.get("source_coverage", {}) if isinstance(bootstrap, dict) else {}
            detail["provider_gap_bootstrap_complete"] = True
            if isinstance(source_coverage, dict):
                detail["source_coverage_sufficient_lane_count"] = int(
                    source_coverage.get("sufficient_lane_count") or 0
                )
                detail["source_coverage_research_eligible_lane_count"] = int(
                    source_coverage.get("research_eligible_lane_count") or 0
                )
                detail["source_coverage_forward_test_eligible_lane_count"] = int(
                    source_coverage.get("forward_test_eligible_lane_count") or 0
                )
        except Exception as exc:
            detail["provider_gap_bootstrap_complete"] = False
            detail["provider_gap_bootstrap_error_type"] = type(exc).__name__
        gc.collect()

    try:
        cycle = await bounded_shadow.run_shadow_cycle()
        detail["cycle_id"] = cycle.cycle_id
        detail["verification_scan_id"] = cycle.verification_scan_id
        detail["observation_count"] = len(cycle.observations)
        detail["survived_count"] = sum(bool(item.survived) for item in cycle.observations)
        scan_id = cycle.verification_scan_id
        cycle_id = cycle.cycle_id
        del cycle
        gc.collect()
    except Exception as exc:
        detail["core_error_type"] = type(exc).__name__
        detail["message"] = str(exc)[:500]
        store.record_worker_heartbeat(
            worker_id=RESEARCH_WORKER_ID,
            state="error",
            error_type=type(exc).__name__,
            detail=detail,
        )
        return WorkerRunStats(RESEARCH_WORKER_ID, 1, 0, 1)

    try:
        nav_heartbeat = store.latest_worker_heartbeat("canonical-portfolio-operating-loop")
        nav_value = nav_heartbeat.detail.get("portfolio_nav_usd") if nav_heartbeat else None
        nav = float(nav_value) if isinstance(nav_value, (int, float)) and nav_value > 0 else 250_000.0
        bridge = await qualified_bridge.publish_latest(total_capital_usd=nav)
        detail["qualified_bridge_candidate_count"] = len(bridge.candidates) if bridge else 0
        del bridge
    except Exception as exc:
        detail["qualified_bridge_error_type"] = type(exc).__name__
    gc.collect()

    try:
        route = await universal.run_dex_route_shadow_cycle()
        detail["dex_route_observation_count"] = len(getattr(route, "observations", ()))
        del route
    except Exception as exc:
        detail["dex_route_shadow_error_type"] = type(exc).__name__
    gc.collect()

    if sequence % tier_every == 0:
        try:
            value = await tier_shadow.run_cycle()
            detail["dex_tier_shadow_observation_count"] = len(getattr(value, "observations", ()))
            del value
        except Exception as exc:
            detail["dex_tier_shadow_error_type"] = type(exc).__name__
        gc.collect()

    if _due(sequence, composite_every, 0.5):
        try:
            value = await composite_shadow.run_cycle()
            detail["cex_dex_composite_observation_count"] = len(getattr(value, "observations", ()))
            del value
        except Exception as exc:
            detail["cex_dex_composite_shadow_error_type"] = type(exc).__name__
        gc.collect()

    if _due(sequence, stablecoin_every, 0.25):
        try:
            value = await stablecoin_shadow.run_cycle()
            detail["stablecoin_depth_observation_count"] = len(getattr(value, "observations", ()))
            del value
        except Exception as exc:
            detail["stablecoin_depth_shadow_error_type"] = type(exc).__name__
        gc.collect()

    if _due(sequence, certification_every, 1.0 / 3.0):
        try:
            import asyncio

            stop = asyncio.Event()
            await run_certification_loop(
                service,
                store,
                allocation_certification=allocation_certification,
                operating_certification=operating_certification,
                stop_event=stop,
                max_cycles=1,
            )
            closure = await run_research_closure_cycle(
                service=service,
                store=store,
                alpha_factory=alpha_factory,
                operating_certification=operating_certification,
                total_capital_usd=float(service.settings.alpha_research_capital_usd),
            )
            detail["research_closure_observed_at"] = (
                closure.observed_at.isoformat() if closure is not None else None
            )
            del closure
        except Exception as exc:
            detail["allocation_certification_error_type"] = type(exc).__name__
        gc.collect()

    if _due(sequence, alpha_every, 0.75):
        # Refresh short-lived provider evidence at the exact decision point where
        # alpha and native mechanism forward trials consume it. Source-specific
        # collector TTLs still prevent unnecessary network requests.
        try:
            source_refresh = await operating_certification.provider_gap_collection.run_cycle()
            refresh_state = source_refresh.get("source_refresh", {}) if isinstance(source_refresh, dict) else {}
            detail["pre_alpha_source_refresh_complete"] = True
            if isinstance(refresh_state, dict):
                detail["pre_alpha_source_refresh_state"] = refresh_state.get("state")
                detail["pre_alpha_source_refresh_failed_sources"] = list(
                    refresh_state.get("failed_sources") or []
                )
                detail["pre_alpha_source_refresh_deferred_sources"] = list(
                    refresh_state.get("memory_deferred_sources") or []
                )
        except Exception as exc:
            detail["pre_alpha_source_refresh_complete"] = False
            detail["pre_alpha_source_refresh_error_type"] = type(exc).__name__
        gc.collect()

        try:
            value = await alpha_factory.run_evidence_cycle()
            detail["alpha_candidate_count"] = value.candidate_count
            detail["alpha_signals_recorded"] = value.signals_recorded
            detail["alpha_outcomes_matured"] = value.outcomes_matured
            mechanism = alpha_factory.mechanism_execution.readiness_summary()
            detail["mechanism_forward_outcomes"] = {
                lane: int(row.get("forward_outcome_count") or 0)
                for lane, row in mechanism.items()
            }
            del value
        except Exception as exc:
            detail["alpha_forward_evidence_error_type"] = type(exc).__name__
        gc.collect()

    if sequence % frontier_every == 0:
        try:
            value = await universal.probe_dex_route_size_frontiers()
            detail["dex_route_frontier_count"] = len(value)
            del value
        except Exception as exc:
            detail["dex_route_frontier_error_type"] = type(exc).__name__
        gc.collect()

    try:
        payload = research_projection.publish(
            forward_target=max(1, int(service.settings.alpha_min_forward_samples)),
            settled_target=max(
                5,
                int(getattr(service.settings, "operating_certification_min_settled_trials", 20)),
            ),
            shadow_horizons_seconds=tuple(
                getattr(service.settings, "shadow_horizons_seconds", (60.0,)) or (60.0,)
            ),
            shadow_cycle_interval_seconds=float(service.settings.shadow_cycle_interval_seconds),
            alpha_evidence_every_cycles=alpha_every,
            heartbeat_stale_seconds=float(service.settings.worker_heartbeat_stale_seconds),
        )
        store.record_worker_heartbeat(
            worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
            state="success",
            detail={
                "projection_observed_at": payload.get("observed_at"),
                "disposable_process": True,
                "presentation_only": True,
                "paper_only": True,
            },
        )
    except Exception as exc:
        detail["research_projection_error_type"] = type(exc).__name__

    errors = _error_keys(detail)
    detail["subsystem_error_keys"] = errors
    detail["subsystem_error_count"] = len(errors)
    final_state = "degraded" if errors else "success"
    store.record_worker_heartbeat(
        worker_id=RESEARCH_WORKER_ID,
        state=final_state,
        cycle_id=cycle_id,
        scan_id=scan_id,
        error_type="ResearchSubsystemDegraded" if errors else None,
        detail=detail,
    )
    # Core collection is the process-fatal boundary. Optional/subsystem failures are
    # represented truthfully as a degraded heartbeat while allowing the disposable
    # supervisor to continue future independent cycles and recover automatically.
    return WorkerRunStats(RESEARCH_WORKER_ID, 1, 1, 0)
