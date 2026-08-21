from __future__ import annotations

import uuid
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
from inefficiency_engine.operating_certification import OperatingCertificationCycle
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions
from inefficiency_engine.strategy_evidence_read import _load_evidence


ALPHA_LANE_IDS = {
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
}


def _mixed_alpha_state(rows: list[dict[str, object]]) -> str:
    """Return the best live strategy state without letting one failed cohort condemn a lane."""

    states = {str(row.get("state") or "collecting") for row in rows}
    for state in (
        "certified",
        "certifying",
        "collecting",
        "execution_blocked",
        "settlement_blocked",
        "statistical_failure",
        "poor_economics",
        "provider_gap",
    ):
        if state in states:
            return state
    return "collecting"


def _best_strategy_row(rows: list[dict[str, object]], state: str) -> dict[str, object] | None:
    matching = [row for row in rows if str(row.get("state") or "collecting") == state]
    if not matching:
        matching = rows
    if not matching:
        return None
    return max(
        matching,
        key=lambda row: (
            int(row.get("candidate_local_forward_outcome_count") or 0),
            int(row.get("independent_forward_outcome_count") or 0),
            int(row.get("settled_allocator_outcome_count") or 0),
        ),
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
    """Operating truth using the same evidence semantics as production qualification.

    Research eligibility, forward-test eligibility, and allocation source qualification
    remain separate. Alpha lane conclusions are derived from candidate-local plus
    correlation-discounted cross-asset evidence, matching the executable alpha
    qualification path instead of pooling every outcome in a family. A failed strategy
    therefore cannot incorrectly condemn a lane while another strategy remains viable.
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
            # This is a genuine mechanism-cohort result, not an engineering/source problem.
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

    def _alpha_lane_status(self, existing, strategy_rows: list[dict[str, object]]):
        lane = self.source_coverage.lane(existing.mechanism_id)
        source = classify_lane_source_dimensions(lane)
        missing = list(lane.missing_evidence_classes)
        downstream = list(lane.downstream_evidence_gaps)

        if not source.provider_ready:
            return existing.model_copy(
                update={
                    "state": "provider_gap",
                    "stage": "waiting_for_source:provider_gap",
                    "provider_ready": False,
                    "primary_reason": (
                        "no fresh admitted authoritative provider is currently usable for this lane"
                    ),
                    "next_action": (
                        "restore an authoritative source producer; do not interpret missing data as strategy failure"
                    ),
                    "blockers": [
                        "no fresh admitted authoritative provider is currently usable",
                        *missing,
                        *downstream,
                    ],
                    "profitability_certified": False,
                }
            )

        if not source.forward_test_eligible:
            return existing.model_copy(
                update={
                    "state": "collecting",
                    "stage": f"waiting_for_source:{source.source_sufficiency_state}",
                    "provider_ready": True,
                    "primary_reason": (
                        "authoritative provider connectivity exists, but the evidence classes required "
                        "for a forward-testable candidate are incomplete or stale"
                    ),
                    "next_action": (
                        "restore the missing/fresh evidence classes; do not label this as poor economics or statistical failure"
                    ),
                    "blockers": [*missing, *downstream],
                    "profitability_certified": False,
                }
            )

        strategy_state = _mixed_alpha_state(strategy_rows)
        selected = _best_strategy_row(strategy_rows, strategy_state)
        strategy_counts: dict[str, int] = {}
        for row in strategy_rows:
            state = str(row.get("state") or "collecting")
            strategy_counts[state] = strategy_counts.get(state, 0) + 1
        count_text = ", ".join(
            f"{count} {state.replace('_', ' ')}"
            for state, count in sorted(strategy_counts.items())
        ) or "no strategy evidence yet"

        state = strategy_state
        stage = "runtime_equivalent_strategy_evidence"
        reason = (
            f"candidate-level strategy evidence: {count_text}; lane state follows the best still-viable "
            "strategy rather than pooled family outcomes"
        )
        next_action = (
            "continue strategy + asset + direction forward evidence under unchanged statistical, regime, and cost gates"
        )

        if strategy_state in {"certifying", "certified"}:
            if not source.allocation_source_qualified:
                state = "collecting"
                stage = "provisional_forward_positive_redundancy_pending"
                reason = (
                    "runtime-equivalent strategy evidence is mature, but independent authoritative source "
                    "redundancy remains an allocation-only blocker"
                )
                next_action = (
                    "restore independent source redundancy while continuing forward evidence; do not lower source thresholds"
                )
            elif existing.current_promoted_count <= 0:
                if existing.current_candidate_count <= 0:
                    state = "collecting"
                    stage = "qualified_history_waiting_for_current_candidate"
                    reason = (
                        "runtime-equivalent evidence is mature, but there is no fresh current candidate to promote"
                    )
                elif existing.current_statistically_qualified_count <= 0:
                    state = "collecting"
                    stage = "current_candidate_not_qualified"
                    reason = (
                        "historical candidate-level evidence is mature, but current candidates do not presently "
                        "clear the complete statistical/regime gate"
                    )
                else:
                    state = "execution_blocked"
                    stage = "current_execution_gate"
                    reason = (
                        "a current statistically qualified candidate exists but does not clear current L2/cost/capacity/health promotion"
                    )
                    next_action = (
                        "continue fresh execution evidence; require positive capturable economics after current costs"
                    )

        updates: dict[str, object] = {
            "state": state,
            "stage": stage,
            "provider_ready": True,
            "primary_reason": reason,
            "next_action": next_action,
            "blockers": [] if state in {"certifying", "certified"} else [*missing, *downstream],
            "profitability_certified": state == "certified",
        }
        if selected is not None:
            updates.update(
                {
                    "forward_signal_count": int(selected.get("forward_signal_count") or 0),
                    "independent_forward_outcome_count": int(
                        selected.get("independent_forward_outcome_count") or 0
                    ),
                    "mean_forward_net_return": selected.get("mean_forward_net_return"),
                    "mean_forward_net_return_ci_lower": selected.get(
                        "mean_forward_net_return_ci_lower"
                    ),
                    "forward_hit_rate": selected.get("forward_hit_rate"),
                    "forward_hit_rate_ci_lower": selected.get("forward_hit_rate_ci_lower"),
                    "settled_allocator_outcome_count": int(
                        selected.get("settled_allocator_outcome_count") or 0
                    ),
                }
            )
        return existing.model_copy(update=updates)

    def _capital_location_truth(self, existing):
        lane = self.source_coverage.lane("capital_location_settlement")
        missing = set(lane.missing_evidence_classes)
        if not ({"transfer_costs", "transfer_latency"} & missing):
            return existing
        return existing.model_copy(
            update={
                "state": "settlement_blocked",
                "stage": "upstream_evidence_producer_missing",
                "primary_reason": (
                    "capital-location downstream code exists, but production has no authoritative empirical "
                    "transfer-cost/transfer-latency producer; historical location scores alone cannot authorize a trial"
                ),
                "next_action": (
                    "connect truthful venue transfer/withdrawal telemetry before interpreting this lane as collecting forward evidence"
                ),
                "blockers": [
                    "production empirical transfer telemetry producer is missing",
                    *list(lane.missing_evidence_classes),
                    *list(lane.downstream_evidence_gaps),
                ],
                "profitability_certified": False,
            }
        )

    async def run_cycle(
        self,
        *,
        total_capital_usd: float = 100000.0,
    ) -> OperatingCertificationCycle:
        """Persist a final lane-truth snapshot after the normal certification cycle."""

        await super().run_cycle(total_capital_usd=total_capital_usd)
        latest = self.ledger.latest()
        if latest is None:
            raise RuntimeError("operating certification did not persist a snapshot")

        strategy_evidence = _load_evidence(self.store, self.core.settings)
        statuses = []
        for row in latest.mechanisms:
            if row.mechanism_id in ALPHA_LANE_IDS:
                row = self._alpha_lane_status(
                    row,
                    [dict(item) for item in strategy_evidence.get(row.mechanism_id, [])],
                )
            if row.mechanism_id == "capital_location_settlement":
                row = self._capital_location_truth(row)
            statuses.append(row)

        blocked_states = {"statistical_failure", "execution_blocked", "settlement_blocked"}
        corrected = latest.model_copy(
            update={
                "snapshot_id": uuid.uuid4().hex,
                "mechanisms": statuses,
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
