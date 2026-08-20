from __future__ import annotations

import asyncio
import signal
from types import SimpleNamespace

from inefficiency_engine import __version__
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.bounded_shadow_service import MemoryBoundedShadowService
from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService as CanonicalPortfolioAllocatorService,
    CexDexFreshnessSeparatedQualifiedOpportunityBridgePublisher as QualifiedOpportunityBridgePublisher,
    CexDexUniversalOperationallyResilientPaperPortfolioService as OperationallyResilientPaperPortfolioService,
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
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.memory_bounded_alpha_factory import (
    MemoryBoundedExpandedAlphaFactoryService as ExpandedAlphaFactoryService,
)
from inefficiency_engine.memory_bounded_research_worker import run_memory_bounded_research_worker
from inefficiency_engine.provider_gap_collection import (
    ProviderGapAwareOperatingCertificationService as OperatingCertificationService,
)
from inefficiency_engine.research_closure_worker import run_research_closure_cycle
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats


RESEARCH_WORKER_ID = "shadow-research-auxiliary"


def _stop_event() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    return stop


class _CompactCoreResearchService:
    """Keep only the tiny heartbeat summary from a completed full shadow cycle."""

    def __init__(self, service: OpportunityService):
        self._service = service
        self.settings = service.settings

    async def run_shadow_cycle(self):
        cycle = await self._service.run_shadow_cycle()
        return SimpleNamespace(
            cycle_id=cycle.cycle_id,
            verification_scan_id=cycle.verification_scan_id,
            observations=tuple(
                SimpleNamespace(survived=bool(observation.survived))
                for observation in cycle.observations
            ),
        )


async def run_research_child(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Run full research/certification under a bounded working set.

    Research surfaces execute sequentially and release their results between phases.
    The core multi-horizon shadow surface additionally uses a rotating bounded L2
    working set, so one scan cannot materialize books/tier state for the entire
    discovered universe at every horizon. Full public-market discovery is still
    persisted on every scan and the exploration half of the L2 budget rotates across
    the tail of the universe. Every successful research cycle also publishes a tiny
    read-only research-card projection after the research heartbeat is durable.
    """

    stop = _stop_event()
    bounded_shadow = MemoryBoundedShadowService(service, store)
    compact_core = _CompactCoreResearchService(bounded_shadow)
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
    alpha_factory = ExpandedAlphaFactoryService(service, store)

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

    # The canonical portfolio has already completed its supervisor bootstrap before
    # this research thread starts. Probe provider-dependent research surfaces here,
    # once per research-thread start, so a deploy/restart does not leave the dashboard
    # showing an obsolete provider gap until the staggered certification cadence fires.
    # Each individual provider remains isolated/fail-closed inside run_cycle().
    try:
        store.record_worker_heartbeat(
            worker_id=RESEARCH_WORKER_ID,
            state="starting",
            detail={
                "provider_gap_bootstrap": True,
                "provider_gap_bootstrap_complete": False,
                "paper_only": True,
            },
        )
        bootstrap = await operating_certification.provider_gap_collection.run_cycle()
        mechanisms = bootstrap.get("mechanisms", {}) if isinstance(bootstrap, dict) else {}
        healthy_count = sum(
            bool(row.get("healthy"))
            for row in mechanisms.values()
            if isinstance(row, dict)
        ) if isinstance(mechanisms, dict) else 0
        store.record_worker_heartbeat(
            worker_id=RESEARCH_WORKER_ID,
            state="running",
            detail={
                "provider_gap_bootstrap": True,
                "provider_gap_bootstrap_complete": True,
                "provider_gap_bootstrap_healthy_count": healthy_count,
                "provider_gap_bootstrap_mechanism_count": len(mechanisms) if isinstance(mechanisms, dict) else 0,
                "paper_only": True,
            },
        )
    except Exception as exc:
        # Provider bootstrap is additive research telemetry. Never suppress the
        # canonical portfolio or the established research loop if this probe fails.
        try:
            store.record_worker_heartbeat(
                worker_id=RESEARCH_WORKER_ID,
                state="degraded",
                error_type=type(exc).__name__,
                detail={
                    "provider_gap_bootstrap": True,
                    "provider_gap_bootstrap_complete": False,
                    "message": str(exc)[:500],
                    "paper_only": True,
                },
            )
        except Exception:
            pass

    research_projection = ResearchDashboardProjectionLedger(store)

    async def qualified_opportunity_cycle() -> object:
        nav_heartbeat = store.latest_worker_heartbeat("canonical-portfolio-operating-loop")
        nav = None
        if nav_heartbeat is not None:
            value = nav_heartbeat.detail.get("portfolio_nav_usd")
            if isinstance(value, (float, int)) and value > 0:
                nav = float(value)
        return await qualified_bridge.publish_latest(
            total_capital_usd=nav or 250_000.0
        )

    async def route_shadow_with_bridge() -> object:
        try:
            snapshot = await qualified_opportunity_cycle()
            if snapshot is None:
                store.record_worker_heartbeat(
                    worker_id="qualified-opportunity-bridge",
                    state="degraded",
                    error_type="QualifiedOpportunitySourceScanUnavailableOrStale",
                    detail={
                        "candidate_count": 0,
                        "memory_bounded_projection": True,
                        "candidate_freshness_separated": True,
                        "paper_only": True,
                    },
                )
            else:
                store.record_worker_heartbeat(
                    worker_id="qualified-opportunity-bridge",
                    state="success",
                    scan_id=snapshot.source_scan_id,
                    detail={
                        "candidate_count": len(snapshot.candidates),
                        "expires_at": snapshot.expires_at.isoformat(),
                        "memory_bounded_projection": True,
                        "candidate_freshness_separated": True,
                        "paper_only": True,
                    },
                )
        except Exception as exc:
            store.record_worker_heartbeat(
                worker_id="qualified-opportunity-bridge",
                state="error",
                error_type=type(exc).__name__,
                detail={
                    "message": str(exc)[:500],
                    "memory_bounded_projection": True,
                    "candidate_freshness_separated": True,
                    "paper_only": True,
                },
            )
        return await universal.run_dex_route_shadow_cycle()

    async def certification_cycle() -> object:
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
        return {
            "allocation": allocation_certification.ledger.summary(),
            "research_closure": closure.model_dump(mode="json") if closure is not None else None,
        }

    def publish_research_dashboard() -> None:
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
                shadow_cycle_interval_seconds=float(
                    getattr(service.settings, "shadow_cycle_interval_seconds", 30.0)
                ),
                alpha_evidence_every_cycles=max(
                    1,
                    int(getattr(service.settings, "alpha_evidence_every_cycles", 10)),
                ),
                heartbeat_stale_seconds=float(
                    getattr(service.settings, "worker_heartbeat_stale_seconds", 180.0)
                ),
            )
            store.record_worker_heartbeat(
                worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                state="success",
                detail={
                    "projection_observed_at": payload.get("observed_at"),
                    "source_research_heartbeat_at": payload.get("source_research_heartbeat_at"),
                    "source_operating_observed_at": payload.get("source_operating_observed_at"),
                    "presentation_only": True,
                    "portfolio_authority_unchanged": True,
                    "paper_only": True,
                },
            )
        except Exception as exc:
            try:
                store.record_worker_heartbeat(
                    worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
                    state="error",
                    error_type=type(exc).__name__,
                    detail={
                        "message": str(exc)[:500],
                        "presentation_only": True,
                        "portfolio_authority_unchanged": True,
                        "research_authority_unchanged": True,
                        "paper_only": True,
                    },
                )
            except Exception:
                pass

    certification_every = max(
        1,
        int(getattr(service.settings, "alpha_evidence_every_cycles", 10)),
    )
    return await run_memory_bounded_research_worker(
        compact_core,  # type: ignore[arg-type]
        store,
        worker_id=RESEARCH_WORKER_ID,
        stop_event=stop,
        route_shadow_runner=route_shadow_with_bridge,
        tier_shadow_runner=tier_shadow.run_cycle,
        tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
        composite_shadow_runner=composite_shadow.run_cycle,
        composite_shadow_every_cycles=10,
        stablecoin_shadow_runner=stablecoin_shadow.run_cycle,
        stablecoin_shadow_every_cycles=10,
        alpha_runner=alpha_factory.run_evidence_cycle,
        alpha_every_cycles=service.settings.alpha_evidence_every_cycles,
        allocation_certification_runner=certification_cycle,
        allocation_certification_every_cycles=certification_every,
        frontier_runner=universal.probe_dex_route_size_frontiers,
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
        post_success_publisher=publish_research_dashboard,
    )


async def run_portfolio_child(service: OpportunityService, store: EvidenceStore) -> int:
    """Run only canonical accounting on the isolated portfolio event loop."""

    stop = _stop_event()
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    canonical_allocator = CanonicalPortfolioAllocatorService(
        service,
        None,
        alpha_factory,
    )
    portfolio = OperationallyResilientPaperPortfolioService(
        service,
        canonical_allocator,
        store,
    )
    return await run_canonical_portfolio_loop(
        service,
        store,
        portfolio=portfolio,
        stop_event=stop,
    )


async def run_certification_child(service: OpportunityService, store: EvidenceStore) -> int:
    """Retained for explicit/manual certification runs; not used by `cie worker`."""

    stop = _stop_event()
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        alpha_factory,
        allocation_certification,
        version=__version__,
    )
    return await run_certification_loop(
        service,
        store,
        allocation_certification=allocation_certification,
        operating_certification=operating_certification,
        stop_event=stop,
    )
