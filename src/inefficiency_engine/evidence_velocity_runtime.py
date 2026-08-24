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
from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions
from inefficiency_engine.strategy_evidence_read import (
    _load_evidence as _load_strategy_evidence,
    _mixed_lane_state,
    _state_counts,
)


_ALPHA_LANES = {
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
}
_BLOCKED_STATES = {"statistical_failure", "execution_blocked", "settlement_blocked"}


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

    The inherited final certification thresholds are unchanged. This layer also owns
    a durable-only reconciliation pass used immediately before dashboard publication.
    That pass performs no provider requests and creates no new authority: it simply
    makes the latest source, forward-outcome, strategy and settlement ledgers agree
    on one current operating snapshot.
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

    @staticmethod
    def _authoritative_source_observation_count(lane) -> int:
        """Return current admitted authoritative source observations for one lane.

        Source rows do not all use the same counter semantics (some durable table
        probes are intentionally represented as one current observation), so callers
        retain any larger historical operating count rather than pretending this is a
        cumulative research-sample count.
        """

        return sum(
            max(0, int(source.get("item_count") or 0))
            for source in list(lane.sources or [])
            if bool(source.get("admitted")) and bool(source.get("authoritative", True))
        )

    @staticmethod
    def _strategy_representative(rows: list[dict[str, object]], mixed_state: str):
        matching = [row for row in rows if str(row.get("state") or "collecting") == mixed_state]
        pool = matching or rows
        if not pool:
            return None
        return max(
            pool,
            key=lambda row: (
                int(row.get("independent_forward_outcome_count") or 0),
                int(row.get("settled_allocator_outcome_count") or 0),
                int(row.get("candidate_local_forward_outcome_count") or 0),
                int(row.get("forward_signal_count") or 0),
            ),
        )

    def _alpha_runtime_status(self, existing, lane, strategy_rows: list[dict[str, object]]):
        """Reconcile an alpha lane without pooling unrelated strategy cohorts.

        Strategy evidence already mirrors executable candidate-local qualification:
        local samples remain full-weight, same-direction cross-asset samples remain
        correlation-discounted, and every existing statistical/regime hurdle remains
        intact. Lane status is therefore a roll-up of strategy states, never a new
        family-wide pseudo-cohort.
        """

        source = classify_lane_source_dimensions(lane)
        source_count = self._authoritative_source_observation_count(lane)
        authoritative_count = max(existing.authoritative_observation_count, source_count)
        rows = [dict(row) for row in strategy_rows]
        mixed = _mixed_lane_state(rows) if rows else "collecting"
        representative = self._strategy_representative(rows, mixed)

        source_blockers: list[str] = []
        if not source.research_eligible:
            source_blockers.append("no fresh admitted authoritative source is available for research")
        if source.research_eligible and not source.forward_test_eligible:
            source_blockers.extend(
                f"missing evidence class:{item}" for item in lane.missing_evidence_classes
            )
        if source.forward_test_eligible and not source.allocation_source_qualified:
            source_blockers.append("authoritative source redundancy target is not satisfied")

        if not source.research_eligible:
            state = "provider_gap"
            stage = f"waiting_for_source:{source.source_sufficiency_state}"
            reason = (
                "no fresh admitted authoritative source is currently usable for this lane; "
                f"provider connectivity is {source.provider_connectivity_state}"
            )
            next_action = (
                "restore a fresh authoritative source before interpreting strategy economics; "
                "do not weaken source or qualification thresholds"
            )
        elif not source.forward_test_eligible:
            state = "collecting"
            stage = "research_active_waiting_for_complete_forward_evidence"
            missing = ", ".join(lane.missing_evidence_classes) or "required evidence classes"
            reason = (
                "authoritative research evidence is connected, but forward-test evidence is incomplete "
                f"({missing})"
            )
            next_action = (
                "collect the missing forward evidence classes while preserving the current economic, "
                "statistical and source gates"
            )
        elif mixed in {"certifying", "certified"} and not source.allocation_source_qualified:
            state = "collecting"
            stage = "provisional_forward_positive"
            reason = (
                f"runtime-equivalent strategy evidence is positive ({_state_counts(rows)}), but independent "
                "authoritative source redundancy is still required before portfolio promotion"
            )
            next_action = (
                "continue forward learning and independently restore source redundancy; do not lower "
                "economic, statistical, execution, risk, settlement, or source gates"
            )
        else:
            state = mixed
            stage = (
                "profitability_certifiable"
                if source.allocation_source_qualified
                or mixed in {"poor_economics", "statistical_failure"}
                else "forward_learning_active_redundancy_pending"
            )
            reason = (
                f"runtime-equivalent strategy evidence: {_state_counts(rows)}; negative conclusions remain "
                "attached to the strategy + asset + direction cohort that produced them rather than being "
                "pooled across the entire lane"
                if rows
                else "forward evidence is connected and waiting for the first runtime-equivalent strategy cohort"
            )
            next_action = (
                "continue independent strategy evidence and preserve failed cohorts without lowering thresholds"
                if state in {"collecting", "poor_economics", "statistical_failure"}
                else "continue allocator settlement and revoke automatically if realized evidence degrades"
            )

        failed_gates = [
            str(item)
            for item in list((representative or {}).get("failed_gates") or [])
        ]
        blockers = [] if state in {"certifying", "certified"} else [*source_blockers, *failed_gates]
        if not blockers and state not in {"certifying", "certified"}:
            blockers = list(existing.blockers)

        update: dict[str, object] = {
            "state": state,
            "stage": stage,
            "provider_ready": source.provider_ready,
            "authoritative_observation_count": authoritative_count,
            "primary_reason": reason,
            "next_action": next_action,
            "blockers": blockers,
            "profitability_certified": bool(
                state == "certified" and source.allocation_source_qualified
            ),
        }
        if rows:
            update["forward_signal_count"] = sum(
                int(row.get("forward_signal_count") or 0) for row in rows
            )
            # This field represents the strongest independently qualifying cohort,
            # never the sum of unrelated strategies/assets/directions.
            update["independent_forward_outcome_count"] = max(
                int(row.get("independent_forward_outcome_count") or 0) for row in rows
            )
        if representative is not None:
            update.update(
                {
                    "mean_forward_net_return": representative.get("mean_forward_net_return"),
                    "mean_forward_net_return_ci_lower": representative.get(
                        "mean_forward_net_return_ci_lower"
                    ),
                    "forward_hit_rate": representative.get("forward_hit_rate"),
                    "forward_hit_rate_ci_lower": representative.get(
                        "forward_hit_rate_ci_lower"
                    ),
                    "settled_allocator_outcome_count": int(
                        representative.get("settled_allocator_outcome_count") or 0
                    ),
                }
            )
        return existing.model_copy(update=update)

    def _source_reconciled_status(
        self,
        existing,
        lane,
        strategy_rows: list[dict[str, object]] | None = None,
    ):
        """Refresh structural-lane source truth and current durable allocator state."""

        source = classify_lane_source_dimensions(lane)
        source_count = self._authoritative_source_observation_count(lane)
        authoritative_count = max(existing.authoritative_observation_count, source_count)
        rows = [dict(row) for row in list(strategy_rows or [])]
        blockers: list[str] = []
        if not source.research_eligible:
            state = "provider_gap"
            stage = f"waiting_for_source:{source.source_sufficiency_state}"
            reason = (
                "no fresh admitted authoritative provider is currently usable; "
                f"provider connectivity is {source.provider_connectivity_state}"
            )
            next_action = "restore fresh authoritative evidence; qualification remains fail-closed"
            blockers.append("no fresh admitted authoritative provider is currently usable")
        elif not source.forward_test_eligible:
            state = "collecting"
            stage = "research_active_waiting_for_complete_forward_evidence"
            reason = "provider evidence is connected, but required forward evidence classes are incomplete"
            next_action = "collect the missing evidence classes without lowering source or economic gates"
            blockers.extend(lane.missing_evidence_classes)
        elif not source.allocation_source_qualified:
            state = "collecting"
            stage = "forward_learning_active_redundancy_pending"
            reason = (
                "complete authoritative forward evidence is connected, but independent-source redundancy "
                "remains an allocation-only blocker"
            )
            next_action = "restore independent source redundancy while preserving all qualification thresholds"
            blockers.append("authoritative source redundancy target is not satisfied")
        elif rows:
            state = _mixed_lane_state(rows)
            stage = "profitability_certifiable"
            reason = (
                f"current allocator strategy evidence: {_state_counts(rows)}; each structural strategy keeps "
                "its own settlement/profitability conclusion rather than being pooled into a synthetic lane cohort"
            )
            next_action = (
                "continue allocator settlement under unchanged gates"
                if state in {"collecting", "certifying", "certified"}
                else "continue observation and preserve the failed strategy cohort without lowering thresholds"
            )
            representative = self._strategy_representative(rows, state)
            update: dict[str, object] = {
                "state": state,
                "stage": stage,
                "provider_ready": source.provider_ready,
                "authoritative_observation_count": authoritative_count,
                "primary_reason": reason,
                "next_action": next_action,
                "blockers": [] if state in {"certifying", "certified"} else list(existing.blockers),
                "profitability_certified": state == "certified",
            }
            if representative is not None:
                update.update(
                    {
                        "forward_signal_count": int(
                            representative.get("forward_signal_count") or 0
                        ),
                        "settled_allocator_outcome_count": int(
                            representative.get("settled_allocator_outcome_count") or 0
                        ),
                        "allocator_realized_profit_usd": representative.get(
                            "allocator_realized_profit_usd"
                        ),
                        "allocator_mean_net_return_ci_lower": representative.get(
                            "allocator_mean_net_return_ci_lower"
                        ),
                        "allocator_profitable_rate_ci_lower": representative.get(
                            "allocator_profitable_rate_ci_lower"
                        ),
                    }
                )
            return existing.model_copy(update=update)
        else:
            # A newly recovered complete source plane with no current allocator rows
            # must not retain a stale provider-gap label from an older snapshot.
            state = "collecting"
            stage = "profitability_certifiable"
            reason = "complete authoritative source evidence is connected and awaiting an eligible allocator cohort"
            next_action = "continue current evidence collection and allocator settlement under unchanged gates"

        return existing.model_copy(
            update={
                "state": state,
                "stage": stage,
                "provider_ready": source.provider_ready,
                "authoritative_observation_count": authoritative_count,
                "primary_reason": reason,
                "next_action": next_action,
                "blockers": blockers,
                "profitability_certified": False,
            }
        )

    def reconcile_latest_runtime_truth(self, *, stage_reporter=None):
        """Persist one post-evidence operating snapshot using durable state only.

        This intentionally does *not* call provider collection, live market scans, L2
        probes, strategy discovery or allocation. It is safe to run after every
        disposable research cycle and closes the timing gap where the dashboard could
        otherwise publish evidence newer than its operating-certification snapshot.
        """

        if callable(stage_reporter):
            stage_reporter("latest_operating_snapshot")
        latest = self.ledger.latest()
        if latest is None:
            return None
        if callable(stage_reporter):
            stage_reporter("strategy_evidence")
        strategy_evidence = _load_strategy_evidence(self.store, self.core.settings)
        if callable(stage_reporter):
            stage_reporter("source_snapshot")
        source_snapshot = self.source_coverage.snapshot()
        source_by_id = {row.lane_id: row for row in source_snapshot.lanes}
        if callable(stage_reporter):
            stage_reporter("mechanism_readiness")
        mechanism_readiness = (
            self.mechanism_execution.readiness_summary()
            if any(row.mechanism_id in MECHANISM_IDS for row in latest.mechanisms)
            else {}
        )
        if callable(stage_reporter):
            stage_reporter("status_rollup")
        statuses = []
        for existing in latest.mechanisms:
            lane = source_by_id.get(existing.mechanism_id)
            if lane is None:
                lane = self.source_coverage.lane(existing.mechanism_id)
            if existing.mechanism_id in MECHANISM_IDS:
                status = self._mechanism_status(
                    existing,
                    lane=lane,
                    readiness=mechanism_readiness.get(existing.mechanism_id),
                )
            else:
                strategy_rows = list(strategy_evidence.get(existing.mechanism_id, []))
                if existing.mechanism_id in _ALPHA_LANES:
                    status = self._alpha_runtime_status(existing, lane, strategy_rows)
                else:
                    status = self._source_reconciled_status(existing, lane, strategy_rows)
            statuses.append(status)

        corrected = latest.model_copy(
            update={
                "snapshot_id": uuid.uuid4().hex,
                "observed_at": datetime.now(timezone.utc),
                "mechanisms": statuses,
                "provider_gap_count": sum(row.state == "provider_gap" for row in statuses),
                "collecting_count": sum(row.state == "collecting" for row in statuses),
                "poor_economics_count": sum(row.state == "poor_economics" for row in statuses),
                "blocked_count": sum(row.state in _BLOCKED_STATES for row in statuses),
                "certifying_count": sum(row.state == "certifying" for row in statuses),
                "certified_count": sum(row.state == "certified" for row in statuses),
            }
        )
        if callable(stage_reporter):
            stage_reporter("operating_ledger_record")
        self.ledger.record(corrected)
        return corrected

    def _mechanism_status(self, existing, *, lane=None, readiness=None):
        status = super()._mechanism_status(
            existing,
            lane=lane,
            readiness=readiness,
        )
        if lane is None:
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
