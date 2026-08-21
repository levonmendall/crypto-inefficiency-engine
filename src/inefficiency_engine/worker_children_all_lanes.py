from __future__ import annotations

from inefficiency_engine import worker_children as _base
from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.executable_lane_runtime import (
    AllLaneAllocationForwardCertificationService,
    AllLaneOperationallyResilientPaperPortfolioService,
    AllLaneQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.executable_operating_certification import (
    AllLaneOperatingCertificationService,
)


RESEARCH_WORKER_ID = _base.RESEARCH_WORKER_ID


def _install_all_lane_runtime() -> None:
    # worker_children resolves these names at call time. Replacing them once keeps
    # its mature memory-bounded scheduling and failure isolation while changing only
    # the concrete evidence/allocation/settlement implementations.
    _base.ExpandedAlphaFactoryService = AllLaneEvidenceFactoryService
    _base.OperatingCertificationService = AllLaneOperatingCertificationService
    _base.UnifiedPaperAllocatorService = AllLaneQualifiedOpportunityAllocatorService
    _base.AllocationForwardCertificationService = AllLaneAllocationForwardCertificationService
    _base.CanonicalPortfolioAllocatorService = AllLaneQualifiedOpportunityAllocatorService
    _base.OperationallyResilientPaperPortfolioService = (
        AllLaneOperationallyResilientPaperPortfolioService
    )


async def run_research_child(service, store):
    _install_all_lane_runtime()
    return await _base.run_research_child(service, store)


async def run_portfolio_child(service, store):
    _install_all_lane_runtime()
    return await _base.run_portfolio_child(service, store)


async def run_certification_child(service, store):
    _install_all_lane_runtime()
    return await _base.run_certification_child(service, store)
