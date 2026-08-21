from __future__ import annotations

from inefficiency_engine import disposable_research_worker as _base
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService,
    EvidenceVelocityLaneSuccessAllocationForwardCertificationService,
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService,
)


def _install_runtime() -> None:
    """Install integrated classes into the existing disposable-cycle scheduler.

    The scheduler resolves these module globals when the cycle function runs. This
    retains its proven disposable-memory behavior while ensuring the actual Render
    research path uses all-lane evidence, source-aware learning gates, and Release D
    subtractive allocation/calibration.
    """

    _base.DisposableExpandedAlphaFactoryService = DisposableExpandedAlphaFactoryService
    _base.OperatingCertificationService = EvidenceVelocityAllLaneOperatingCertificationService
    _base.UnifiedPaperAllocatorService = EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService
    _base.AllocationForwardCertificationService = (
        EvidenceVelocityLaneSuccessAllocationForwardCertificationService
    )


async def run_disposable_research_cycle(service, store, *, sequence: int):
    _install_runtime()
    return await _base.run_disposable_research_cycle(
        service,
        store,
        sequence=sequence,
    )
