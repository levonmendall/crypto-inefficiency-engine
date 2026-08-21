from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine.executable_operating_certification import (
    AllLaneOperatingCertificationService,
)
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.lane_success import LaneSuccessController
from inefficiency_engine.lane_success_runtime import (
    LaneSuccessAllocationForwardCertificationService,
    LaneSuccessOperationallyResilientPaperPortfolioService,
    LaneSuccessQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions


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
            raise RuntimeError(
                "evidence-velocity lane-success allocator requires durable evidence"
            )
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


class EvidenceVelocityAllLaneOperatingCertificationService(
    AllLaneOperatingCertificationService
):
    """Operating read plane that distinguishes learning eligibility from allocation.

    The inherited final certification thresholds are unchanged. This override only
    stops a redundancy-only source gap from being described as if it prevented
    forward learning; redundancy remains mandatory for promotion/allocation.
    """

    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(
            core,
            store,
            alpha_factory,
            allocation_certification,
            version=version,
        )
        self.mechanism_execution = EvidenceVelocityLaneSuccessMechanismExecutionService(
            core,
            store,
        )

    def _forward_evidence_heartbeat(self, family: str, *, now: datetime):
        """Always evaluate worker health against wall-clock UTC, never a stale scan time."""

        del now
        return super()._forward_evidence_heartbeat(
            family,
            now=datetime.now(timezone.utc),
        )

    def _mechanism_status(self, existing):
        status = super()._mechanism_status(existing)
        lane = self.source_coverage.lane(existing.mechanism_id)
        source = classify_lane_source_dimensions(lane)
        if not source.forward_test_eligible or source.allocation_source_qualified:
            return status

        forward = int(status.independent_forward_outcome_count)
        statistically_qualified = int(status.current_statistically_qualified_count)
        if forward >= 30 and statistically_qualified <= 0:
            # This is a genuine strategy result, not an engineering/source problem.
            return status.model_copy(
                update={
                    "state": "statistical_failure",
                    "stage": "profitability_certifiable",
                    "primary_reason": (
                        "forward learning completed under complete authoritative evidence, "
                        "but the cohort does not clear the unchanged statistical gate; "
                        "allocation-source redundancy is also still pending"
                    ),
                    "next_action": (
                        "continue independent forward observation and restore source redundancy; "
                        "do not weaken statistical or source thresholds"
                    ),
                    "blockers": [
                        "authoritative source redundancy target is not satisfied",
                        *lane.downstream_evidence_gaps,
                    ],
                }
            )

        if statistically_qualified > 0:
            stage = "provisional_forward_positive"
            reason = (
                "forward evidence clears an incremental/full statistical cohort gate, "
                "but independent authoritative source redundancy is still required "
                "before any portfolio promotion"
            )
        elif forward >= 3:
            stage = "forward_learning_active_redundancy_pending"
            reason = (
                "complete authoritative evidence is producing forward outcomes; "
                "qualification is still accumulating and source redundancy remains "
                "an allocation-only blocker"
            )
        else:
            stage = "forward_learning_active_redundancy_pending"
            reason = (
                f"complete authoritative evidence is admitted and native forward settlement "
                f"is accumulating ({forward}/3 before incremental evidence can be interpreted); "
                "source redundancy remains an allocation-only blocker"
            )

        return status.model_copy(
            update={
                "state": "collecting",
                "stage": stage,
                "provider_ready": source.provider_ready,
                "primary_reason": reason,
                "next_action": (
                    "continue forward learning and independently restore source redundancy; "
                    "do not lower economic, statistical, execution, risk, settlement, or source gates"
                ),
                "blockers": [
                    "authoritative source redundancy target is not satisfied",
                    *lane.downstream_evidence_gaps,
                ],
            }
        )
