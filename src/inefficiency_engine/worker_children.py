from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

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


class _MemorySerializedService:
    """Duck-typed OpportunityService wrapper that serializes the heavy core shadow scan.

    `run_shadow_worker` intentionally schedules its research surfaces with
    `asyncio.gather`. On a 512 MB Render worker, concurrent full-market scans can
    exceed the instance memory ceiling even though each individual research surface
    fits. A shared gate preserves all research work while allowing only one
    provider/data-heavy auxiliary surface to own peak memory at a time.
    """

    def __init__(self, service: OpportunityService, gate: asyncio.Lock):
        self._service = service
        self._gate = gate
        self.settings = service.settings

    async def run_shadow_cycle(self):
        async with self._gate:
            return await self._service.run_shadow_cycle()


def _serialized_runner(
    gate: asyncio.Lock,
    runner: Callable[[], Awaitable[object]],
) -> Callable[[], Awaitable[object]]:
    async def run() -> object:
        async with gate:
            return await runner()

    return run


async def run_research_child(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Run complete research/certification with a single auxiliary peak-memory owner.

    Canonical accounting stays isolated on its own thread. Inside the auxiliary
    research thread, the worker's normal staggered schedule is preserved, but the
    full-market shadow scan, route probes, mechanism shadows, alpha evidence and
    certification are serialized behind one lock. This prevents `asyncio.gather`
    from multiplying large point-in-time market snapshots in memory while retaining
    the same evidence surfaces, thresholds, paper-only authority and durable output.
    """

    stop = _stop_event()
    memory_gate = asyncio.Lock()
    serialized_service = _MemorySerializedService(service, memory_gate)

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

    async def _certification_cycle() -> object:
        await run_certification_loop(
            service,
            store,
            allocation_certification=allocation_certification,
            operating_certification=operating_certification,
            stop_event=stop,
            max_cycles=1,
        )
        return allocation_certification.ledger.summary()

    certification_cycle = _serialized_runner(memory_gate, _certification_cycle)
    certification_every = max(
        1,
        int(getattr(service.settings, "alpha_evidence_every_cycles", 10)),
    )
    return await run_shadow_worker(
        serialized_service,  # type: ignore[arg-type]
        store,
        worker_id=RESEARCH_WORKER_ID,
        stop_event=stop,
        route_shadow_runner=_serialized_runner(
            memory_gate,
            universal.run_dex_route_shadow_cycle,
        ),
        tier_shadow_runner=_serialized_runner(memory_gate, tier_shadow.run_cycle),
        tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
        composite_shadow_runner=_serialized_runner(memory_gate, composite_shadow.run_cycle),
        composite_shadow_every_cycles=10,
        stablecoin_shadow_runner=_serialized_runner(memory_gate, stablecoin_shadow.run_cycle),
        stablecoin_shadow_every_cycles=10,
        alpha_runner=_serialized_runner(memory_gate, alpha_factory.run_evidence_cycle),
        alpha_every_cycles=service.settings.alpha_evidence_every_cycles,
        allocation_certification_runner=certification_cycle,
        allocation_certification_every_cycles=certification_every,
        frontier_runner=_serialized_runner(
            memory_gate,
            universal.probe_dex_route_size_frontiers,
        ),  # type: ignore[arg-type]
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
    )


async def run_portfolio_child(service: OpportunityService, store: EvidenceStore) -> int:
    """Run only canonical accounting on the isolated portfolio event loop.

    The canonical allocator overrides unified family discovery and only consumes the
    settlement-compatible alpha family. Do not construct the unused universal,
    composite CEX↔DEX, or promotion graphs in this thread: they add memory pressure
    but have no authority or code path in canonical allocation.
    """

    stop = _stop_event()
    alpha_factory = ExpandedAlphaFactoryService(service, store)
    canonical_allocator = CanonicalPortfolioAllocatorService(
        service,
        None,  # inherited CEX↔DEX constructor seam is unused by canonical allocator
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
