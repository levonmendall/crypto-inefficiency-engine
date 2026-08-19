from __future__ import annotations

import asyncio
import signal

from inefficiency_engine import __version__
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.bounded_alpha_factory import (
    BoundedExpandedAlphaFactoryService as ExpandedAlphaFactoryService,
)
from inefficiency_engine.canonical_allocator import CanonicalPortfolioAllocatorService
from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.certification_worker import run_certification_loop
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats, run_shadow_worker


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


async def run_research_child(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Run all non-canonical research and certification on one auxiliary event loop.

    This deliberately caps the production worker at two heavy runtime domains:
    canonical accounting and an auxiliary research/certification domain. Mechanism
    certification remains complete, but it no longer needs a third simultaneous
    OpportunityService/provider graph. A synchronous stall here can degrade research
    without taking down canonical accounting.
    """

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

    # Full forward/mechanism certification shares the non-authoritative auxiliary
    # service graph instead of constructing a third provider/database graph.
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

    async def certification_cycle() -> object:
        await run_certification_loop(
            service,
            store,
            allocation_certification=allocation_certification,
            operating_certification=operating_certification,
            stop_event=stop,
            max_cycles=1,
        )
        return allocation_certification.ledger.summary()

    certification_every = max(
        1,
        int(getattr(service.settings, "alpha_evidence_every_cycles", 10)),
    )
    return await run_shadow_worker(
        service,
        store,
        worker_id=RESEARCH_WORKER_ID,
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
        allocation_certification_runner=certification_cycle,
        allocation_certification_every_cycles=certification_every,
        frontier_runner=universal.probe_dex_route_size_frontiers,
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
    )


async def run_portfolio_child(service: OpportunityService, store: EvidenceStore) -> int:
    """Run only canonical accounting on the isolated portfolio event loop."""

    stop = _stop_event()
    universal = UniversalOpportunityService(service)
    composite_service = CexDexCompositeEvidenceService(service, universal=universal)
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    promotion = CexDexPaperPromotionService(service, composite_service, store)
    canonical_allocator = CanonicalPortfolioAllocatorService(
        service,
        promotion,
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
