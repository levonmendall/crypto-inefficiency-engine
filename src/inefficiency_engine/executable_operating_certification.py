from __future__ import annotations

import statistics
import uuid

from inefficiency_engine.alpha_factory import _mean_lower, _wilson_lower
from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
from inefficiency_engine.executable_source_collection import (
    ExecutableSourceCoverageAwareOperatingCertificationService,
)
from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.operating_certification import (
    MechanismOperatingStatus,
    OperatingCertificationCycle,
)
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions


class AllLaneOperatingCertificationService(
    ExecutableSourceCoverageAwareOperatingCertificationService
):
    """Operating status whose five former research lanes now have real forward paths."""

    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(
            core,
            store,
            alpha_factory,
            allocation_certification,
            version=version,
        )
        self.mechanism_execution = ExecutableMechanismExecutionService(core, store)

    def _allocator_outcomes(self, mechanism_id: str):
        rows = self.allocation_certification.ledger.outcomes()
        return [
            row for row in rows
            if row.family == "mechanism"
            and row.strategy.startswith(f"mechanism:{mechanism_id}:")
        ]

    def _mechanism_status(self, existing: MechanismOperatingStatus) -> MechanismOperatingStatus:
        mechanism_id = existing.mechanism_id
        readiness = self.mechanism_execution.readiness_summary()[mechanism_id]
        lane = self.source_coverage.lane(mechanism_id)
        source = classify_lane_source_dimensions(lane)
        outcomes = self.mechanism_execution.ledger.outcomes(mechanism_id=mechanism_id)
        values = [row.realized_net_return for row in outcomes]
        positive = sum(value > 0 for value in values)
        mean = statistics.fmean(values) if values else None
        lower = _mean_lower(values)
        hit = positive / len(values) if values else None
        current_promoted = int(readiness["current_promoted_candidate_count"])
        full_qualified = int(readiness["full_qualified_cohort_count"])
        incremental_qualified = int(readiness["incremental_qualified_cohort_count"])

        allocator = self._allocator_outcomes(mechanism_id)
        allocator_values = [row.realized_net_return for row in allocator]
        allocator_positive = sum(value > 0 for value in allocator_values)
        allocator_lower = _mean_lower(allocator_values)
        allocator_hit_lower = _wilson_lower(allocator_positive, len(allocator_values))
        allocator_profit = sum(row.realized_profit_usd for row in allocator)
        min_allocator = self.min_allocator_settled_trials
        certified = bool(
            len(allocator) >= min_allocator
            and allocator_lower is not None
            and allocator_lower > 0
            and allocator_hit_lower is not None
            and allocator_hit_lower >= self.min_allocator_profitable_rate_lower
            and allocator_profit > 0
        )

        # Provider connectivity and complete source sufficiency are deliberately
        # separate. ``provider_gap`` now means exactly that no fresh admitted
        # authoritative provider is usable. Missing evidence classes, insufficient
        # independent-source redundancy, or stale evidence remain fail-closed but do
        # not pretend an already-connected provider disappeared.
        if not source.source_layer_sufficient:
            stage = f"waiting_for_source:{source.source_sufficiency_state}"
            if source.source_sufficiency_state == "provider_gap":
                state = "provider_gap"
                reason = (
                    "no fresh admitted authoritative provider is currently usable; "
                    f"provider connectivity is {source.provider_connectivity_state}"
                )
                next_action = (
                    "restore or connect an authoritative provider; forward eligibility remains fail-closed"
                )
            elif source.source_sufficiency_state == "stale":
                state = "collecting"
                reason = (
                    "provider integration exists, but its authoritative evidence is stale; "
                    "this is a freshness gap, not a missing-provider gap"
                )
                next_action = (
                    "refresh the admitted source evidence; do not lower freshness or qualification thresholds"
                )
            elif source.source_sufficiency_state == "redundancy_gap":
                state = "collecting"
                reason = (
                    "authoritative provider evidence is connected, but the independent-source redundancy "
                    "target is not yet satisfied"
                )
                next_action = (
                    "continue/restore independent authoritative source collection; forward eligibility remains fail-closed"
                )
            else:
                state = "collecting"
                reason = (
                    "authoritative provider evidence is connected, but one or more required evidence classes "
                    "are incomplete"
                )
                next_action = (
                    "collect the missing evidence classes; do not add duplicate providers or lower source requirements"
                )
        elif len(values) < 3:
            stage = "profitability_certifiable"
            state = "collecting"
            reason = f"native forward settlement is operating ({len(values)}/3 outcomes before incremental paper eligibility)"
            next_action = "continue native forward settlement without lowering economic or statistical gates"
        elif full_qualified <= 0 and incremental_qualified <= 0:
            stage = "profitability_certifiable"
            state = "statistical_failure" if len(values) >= 30 else "collecting"
            reason = (
                "forward outcomes exist, but no cohort currently clears the incremental/full statistical gate"
            )
            next_action = "continue independent forward outcomes; 3→30 sizing remains unchanged"
        elif current_promoted <= 0:
            stage = "profitability_certifiable"
            state = "collecting"
            reason = "forward evidence is qualified, but no fresh current mechanism candidate is present"
            next_action = "wait for a fresh after-cost opportunity under the unchanged qualification policy"
        elif certified:
            stage = "profitability_certifiable"
            state = "certified"
            reason = "native mechanism allocations have statistically conservative positive settled paper profitability"
            next_action = "maintain monitoring and automatically revoke if future realized evidence degrades"
        else:
            stage = "profitability_certifiable"
            state = "certifying"
            reason = (
                f"native mechanism path is paper-allocatable; allocator settlement is accumulating "
                f"({len(allocator)}/{min_allocator})"
            )
            next_action = "continue canonical paper allocation and native settlement toward profitability certification"

        blockers = [
            *lane.missing_evidence_classes,
            *lane.downstream_evidence_gaps,
        ]
        if source.source_sufficiency_state == "redundancy_gap":
            blockers.insert(0, "authoritative source redundancy target is not satisfied")
        elif source.source_sufficiency_state == "stale":
            blockers.insert(0, "authoritative source evidence is stale")
        elif source.source_sufficiency_state == "provider_gap":
            blockers.insert(0, "no fresh admitted authoritative provider is currently usable")

        return existing.model_copy(
            update={
                "state": state,
                "stage": stage,
                # provider_ready now means provider connectivity only. The separate
                # source_layer_sufficient gate still controls forward-trial admission.
                "provider_ready": source.provider_ready,
                "authoritative_observation_count": max(
                    existing.authoritative_observation_count,
                    lane.healthy_source_count,
                ),
                "forward_signal_count": max(existing.forward_signal_count, len(values)),
                "independent_forward_outcome_count": len(values),
                "current_candidate_count": current_promoted,
                "current_statistically_qualified_count": incremental_qualified + full_qualified,
                "current_promoted_count": current_promoted,
                "settled_allocator_outcome_count": len(allocator),
                "mean_forward_net_return": mean,
                "mean_forward_net_return_ci_lower": lower,
                "forward_hit_rate": hit,
                "allocator_realized_profit_usd": allocator_profit,
                "allocator_mean_net_return_ci_lower": allocator_lower,
                "allocator_profitable_rate_ci_lower": allocator_hit_lower,
                "profitability_certified": certified,
                "primary_reason": reason,
                "next_action": next_action,
                "blockers": [] if state in {"certifying", "certified"} else blockers,
            }
        )

    async def run_cycle(
        self,
        *,
        total_capital_usd: float = 100000.0,
    ) -> OperatingCertificationCycle:
        # First run the established source/profitability interpretation, then write a
        # second immutable snapshot that advances only the five lanes now backed by
        # native forward/settlement contracts.
        await super().run_cycle(total_capital_usd=total_capital_usd)
        latest = self.ledger.latest()
        if latest is None:
            raise RuntimeError("operating certification did not persist a snapshot")
        statuses = [
            self._mechanism_status(row) if row.mechanism_id in MECHANISM_IDS else row
            for row in latest.mechanisms
        ]
        blocked_states = {"statistical_failure", "execution_blocked", "settlement_blocked"}
        corrected = latest.model_copy(
            update={
                "snapshot_id": uuid.uuid4().hex,
                "mechanisms": statuses,
                # This count is now literal: evidence-class/redundancy/freshness gaps
                # no longer inflate the missing-provider number.
                "provider_gap_count": sum(row.state == "provider_gap" for row in statuses),
                "collecting_count": sum(row.state == "collecting" for row in statuses),
                "poor_economics_count": sum(row.state == "poor_economics" for row in statuses),
                "blocked_count": sum(row.state in blocked_states for row in statuses),
                "certifying_count": sum(row.state == "certifying" for row in statuses),
                "certified_count": sum(row.state == "certified" for row in statuses),
            }
        )
        self.ledger.record(corrected)
        return OperatingCertificationCycle(
            observed_at=corrected.observed_at,
            snapshot_id=corrected.snapshot_id,
            mechanism_count=corrected.mechanism_count,
            certified_count=corrected.certified_count,
            provider_gap_count=corrected.provider_gap_count,
            poor_economics_count=corrected.poor_economics_count,
        )
