from __future__ import annotations

import asyncio
import signal

from inefficiency_engine import __version__
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CanonicalPaperPortfolioService,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeShadowService
from inefficiency_engine.dex_tier_shadow import DexTierShadowService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import StablecoinDepthShadowService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import WorkerRunStats, run_shadow_worker


async def run_forever(service: OpportunityService, store: EvidenceStore) -> WorkerRunStats:
    """Production paper worker with compounding portfolio and certification.

    The canonical account begins exactly once with $250,000. Portfolio state is
    durable and append-only; deploys/restarts recover the existing account rather
    than recreating capital. Unsupported settlement paths remain fail-closed.
    """

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

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
    allocation_certification = AllocationForwardCertificationService(service, unified, store)
    portfolio = CanonicalPaperPortfolioService(service, unified, store)
    operating_certification = OperatingCertificationService(
        service,
        store,
        alpha_factory,
        allocation_certification,
        version=__version__,
    )

    portfolio.ledger.ensure_genesis()
    if portfolio.ledger.latest_snapshot() is None:
        portfolio.ledger.record_snapshot(portfolio.ledger.current_state())

    async def allocation_portfolio_and_operating_cycle():
        current = portfolio.ledger.current_state()
        certification_capital = max(1.0, current.nav_usd)
        allocation_cycle = await allocation_certification.run_cycle(
            total_capital_usd=certification_capital
        )
        portfolio_cycle = await portfolio.run_cycle()
        await operating_certification.run_cycle(
            total_capital_usd=max(1.0, portfolio_cycle.nav_usd)
        )
        return allocation_cycle

    return await run_shadow_worker(
        service,
        store,
        stop_event=stop_event,
        route_shadow_runner=universal.run_dex_route_shadow_cycle,
        tier_shadow_runner=tier_shadow.run_cycle,
        tier_shadow_every_cycles=service.settings.dex_route_tier_shadow_every_cycles,
        composite_shadow_runner=composite_shadow.run_cycle,
        composite_shadow_every_cycles=10,
        stablecoin_shadow_runner=stablecoin_shadow.run_cycle,
        stablecoin_shadow_every_cycles=10,
        allocation_certification_runner=allocation_portfolio_and_operating_cycle,
        allocation_certification_every_cycles=max(
            1, int(getattr(service.settings, "allocation_certification_every_cycles", 10))
        ),
        alpha_runner=alpha_factory.run_evidence_cycle,
        alpha_every_cycles=service.settings.alpha_evidence_every_cycles,
        frontier_runner=universal.probe_dex_route_size_frontiers,
        frontier_every_cycles=service.settings.dex_route_frontier_every_cycles,
    )
