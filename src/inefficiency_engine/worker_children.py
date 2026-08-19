from __future__ import annotations

import asyncio
import signal

from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_worker import run_portfolio_operating_loop
from inefficiency_engine.portfolio_stage_isolation import (
    IsolatedAllocationCertificationProxy,
    IsolatedAllocatorProxy,
    IsolatedOperatingCertificationProxy,
    IsolatedOpportunityCoreProxy,
)
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats, run_shadow_worker


def _stop_event() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    return stop


async def run_research_child(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Run broad shadow research on its own event loop/process."""

    stop = _stop_event()
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
    return await run_shadow_worker(
        service,
        store,
        stop_event=stop,
        route_shadow_runner=universal.run_dex_route_shadow_cycle,
        tier_shadow_runner=tier_shadow.run_cycle,
        tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
        composite_shadow_runner=composite_shadow.run_cycle,
        composite_shadow_every_cycles=10,
        stablecoin_shadow_runner=stablecoin_shadow.run_cycle,
        stablecoin_shadow_every_cycles=10,
        alpha_runner=alpha_factory.run_evidence_cycle,
        alpha_every_cycles=service.settings.alpha_evidence_every_cycles,
        frontier_runner=universal.probe_dex_route_size_frontiers,
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
    )


async def run_portfolio_child(service: OpportunityService, store: EvidenceStore) -> int:
    """Run canonical accounting with provider-heavy stages in disposable processes.

    The portfolio child itself is long-lived and responsive. Live market scans,
    unified allocation, and both certification passes execute in shorter-lived
    subprocesses with hard OS-level deadlines, so synchronous provider/library
    stalls cannot freeze canonical accounting or defeat asyncio cancellation.
    """

    stop = _stop_event()
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)

    isolated_core = IsolatedOpportunityCoreProxy(service, store)
    isolated_allocator = IsolatedAllocatorProxy(unified)
    portfolio = OperationallyResilientPaperPortfolioService(
        isolated_core,
        isolated_allocator,
        store,
    )
    allocation_certification = IsolatedAllocationCertificationProxy()
    operating_certification = IsolatedOperatingCertificationProxy()

    return await run_portfolio_operating_loop(
        service,
        store,
        portfolio=portfolio,
        allocation_certification=allocation_certification,  # type: ignore[arg-type]
        operating_certification=operating_certification,  # type: ignore[arg-type]
        stop_event=stop,
    )
