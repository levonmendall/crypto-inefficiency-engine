from __future__ import annotations

from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.lane_success import LaneSuccessController
from inefficiency_engine.lane_success_runtime import (
    LaneSuccessAllocationForwardCertificationService,
    LaneSuccessOperationallyResilientPaperPortfolioService,
    LaneSuccessQualifiedOpportunityAllocatorService,
)


class EvidenceVelocityLaneSuccessMechanismExecutionService(
    GovernedMechanismExecutionService
):
    """Release D mechanism feedback on top of the evidence-velocity source boundary."""

    def __init__(self, core, store):
        super().__init__(core, store)
        self.lane_success = LaneSuccessController(store)

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        regime = self.lane_success.market_regime(now=snapshot.completed_at)
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        result = []
        for spec in rows:
            payload = dict(spec.settlement_payload)
            payload["lane_success_regime"] = regime
            payload["lane_success_regime_conditioning"] = True
            result.append(spec.model_copy(update={"settlement_payload": payload}))
        return result

    def _outcome(self, trial, settlement):
        outcome = super()._outcome(trial, settlement)
        self.lane_success.record_mechanism_outcome(
            trial,
            outcome,
            settlement_detail=dict(settlement.detail or {}),
        )
        return outcome


class EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService(
    LaneSuccessQualifiedOpportunityAllocatorService
):
    """Release D allocator with source-governed mechanism promotion."""

    def __init__(self, core, cex_dex, alpha_factory):
        super().__init__(core, cex_dex, alpha_factory)
        store = getattr(alpha_factory, "store", None)
        if store is None:
            raise RuntimeError("evidence-velocity lane-success allocator requires durable evidence")
        self.mechanisms = EvidenceVelocityLaneSuccessMechanismExecutionService(core, store)


class EvidenceVelocityLaneSuccessAllocationForwardCertificationService(
    LaneSuccessAllocationForwardCertificationService
):
    """Release D allocator settlement with governed mechanism source qualification."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.mechanisms = EvidenceVelocityLaneSuccessMechanismExecutionService(core, store)


class EvidenceVelocityLaneSuccessOperationallyResilientPaperPortfolioService(
    LaneSuccessOperationallyResilientPaperPortfolioService
):
    """Canonical paper portfolio using the integrated evidence-velocity settlement path."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.settlement = EvidenceVelocityLaneSuccessAllocationForwardCertificationService(
            core,
            allocator,
            store,
        )
