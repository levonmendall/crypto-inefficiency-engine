from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.evidence_velocity import provisional_forward_positive
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.operating_certification import OperatingCertificationLedger
from inefficiency_engine.source_coverage import SourceCoveragePlane
from inefficiency_engine.source_coverage_catalog import LANES
from inefficiency_engine.source_state_dimensions import classify_lane_source_dimensions


class LaneExecutableReadiness(BaseModel):
    lane_id: str
    name: str
    source_layer_sufficient: bool
    research_eligible: bool = False
    forward_test_eligible: bool = False
    allocation_source_qualified: bool = False
    provider_connectivity_state: str
    source_sufficiency_state: str
    source_headline_state: str
    qualification_stage: str
    evidence_producer_implemented: bool = True
    production_evidence_path_connected: bool = True
    economics_model_implemented: bool
    forward_loop_implemented: bool
    statistical_gate_implemented: bool
    allocation_bridge_implemented: bool
    settlement_contract_implemented: bool
    architecture_execution_capable: bool
    decision_grade_outcome_qualified: bool = False
    paper_execution_capable: bool
    provisional_forward_positive: bool = False
    currently_qualified: bool
    profitability_certified: bool
    execution_state: str
    # Backward-compatible legacy source taxonomy from SourceCoveragePlane. New
    # consumers should use provider_connectivity_state + source_sufficiency_state.
    source_state: str
    forward_outcome_count: int = Field(default=0, ge=0)
    current_promoted_candidate_count: int = Field(default=0, ge=0)
    blockers: list[str] = Field(default_factory=list)
    paper_only: bool = True
    live_execution_capable: bool = False


class LaneExecutableReadinessSnapshot(BaseModel):
    observed_at: datetime
    lane_count: int
    architecture_executable_count: int
    production_evidence_connected_count: int = 0
    all_lanes_production_evidence_connected: bool = False
    research_eligible_count: int = 0
    forward_test_eligible_count: int = 0
    provisional_forward_positive_count: int = 0
    decision_grade_outcome_qualified_count: int = 0
    currently_qualified_count: int
    paper_execution_capable_count: int = 0
    profitability_certified_count: int
    all_lanes_paper_execution_capable: bool
    lanes: list[LaneExecutableReadiness]
    interpretation: str
    paper_only: bool = True
    live_execution_capable: bool = False


# Downstream code capability and upstream evidence connectivity are deliberately
# separate. All 13 lanes have economics/forward/statistics/allocation/settlement
# contracts. Twelve currently have a production evidence producer feeding that path.
# Capital-location requires empirical transfer-cost/latency telemetry; its ledger
# writer and downstream tests exist, but no production collector currently produces
# those observations.
_ARCHITECTURE = {
    lane_id: {
        "evidence_producer": lane_id != "capital_location_settlement",
        "economics": True,
        "forward": True,
        "statistics": True,
        "allocation": True,
        "settlement": True,
    }
    for lane_id in LANES
}

_CAPITAL_LOCATION_PRODUCER_BLOCKER = (
    "production authoritative transfer-cost/transfer-latency evidence producer is not implemented; "
    "do not infer empirical transfer telemetry from paper policy, fee configuration, or chain timing"
)


def build_lane_executable_readiness(core, store) -> LaneExecutableReadinessSnapshot:
    source_plane = SourceCoveragePlane(store)
    source_snapshot = source_plane.snapshot()
    source_by_id = {row.lane_id: row for row in source_snapshot.lanes}
    operating = OperatingCertificationLedger(store).latest()
    operating_by_id = {
        row.mechanism_id: row
        for row in (operating.mechanisms if operating is not None else [])
    }
    mechanisms = GovernedMechanismExecutionService(core, store)
    mechanism_readiness = mechanisms.readiness_summary()

    rows: list[LaneExecutableReadiness] = []
    for lane_id, definition in LANES.items():
        architecture = _ARCHITECTURE[lane_id]
        evidence_producer = bool(architecture["evidence_producer"])
        architecture_capable = all(
            bool(architecture[key])
            for key in ("economics", "forward", "statistics", "allocation", "settlement")
        )
        production_connected = architecture_capable and evidence_producer
        source = source_by_id.get(lane_id)
        dimensions = classify_lane_source_dimensions(source) if source is not None else None
        source_ready = bool(
            dimensions.allocation_source_qualified if dimensions is not None else False
        )
        research_eligible = bool(
            dimensions.research_eligible if dimensions is not None else False
        )
        forward_test_eligible = bool(
            dimensions.forward_test_eligible if dimensions is not None else False
        )
        allocation_source_qualified = source_ready
        source_state = source.source_state if source is not None else "unobserved"
        provider_connectivity_state = (
            dimensions.provider_connectivity_state if dimensions is not None else "missing"
        )
        source_sufficiency_state = (
            dimensions.source_sufficiency_state if dimensions is not None else "provider_gap"
        )
        source_headline_state = (
            dimensions.source_headline_state if dimensions is not None else "provider_gap"
        )
        operating_row = operating_by_id.get(lane_id)

        if lane_id in MECHANISM_IDS:
            mechanism = mechanism_readiness[lane_id]
            forward_count = int(mechanism["forward_outcome_count"])
            promoted_count = int(mechanism["current_promoted_candidate_count"])
            currently_qualified = bool(mechanism["currently_qualified"])
        else:
            forward_count = int(
                operating_row.independent_forward_outcome_count
                if operating_row is not None
                else 0
            )
            promoted_count = int(
                operating_row.current_promoted_count if operating_row is not None else 0
            )
            if lane_id in {"price_discrepancy", "carry"}:
                currently_qualified = bool(
                    operating_row is not None
                    and operating_row.state in {"certifying", "certified"}
                    and allocation_source_qualified
                )
            else:
                currently_qualified = promoted_count > 0 and allocation_source_qualified

        forward_mean = (
            operating_row.mean_forward_net_return if operating_row is not None else None
        )
        forward_hit = operating_row.forward_hit_rate if operating_row is not None else None
        provisional = provisional_forward_positive(
            outcome_count=forward_count,
            mean_net_return=forward_mean,
            hit_rate=forward_hit,
        )
        profitability_certified = bool(
            operating_row is not None and operating_row.profitability_certified
        )

        # A positive decision-grade lane conclusion is broader than the presence of
        # a candidate right now, but narrower than research/code readiness. A
        # certifying/certified operating state means the durable qualification plane
        # has reached a positive allocation-grade conclusion; a currently promoted
        # candidate is also sufficient proof because promotion itself is gated by
        # the canonical statistical/economic qualification path.
        operating_positive = bool(
            operating_row is not None
            and operating_row.state in {"certifying", "certified"}
        )
        decision_grade_outcome_qualified = bool(
            currently_qualified or operating_positive
        )
        paper_capable = bool(
            architecture_capable
            and production_connected
            and allocation_source_qualified
            and decision_grade_outcome_qualified
        )

        blockers: list[str] = []
        if not evidence_producer:
            blockers.append(_CAPITAL_LOCATION_PRODUCER_BLOCKER)
        if source is None:
            blockers.append("source coverage has not been observed")
        else:
            if not research_eligible:
                blockers.append("no fresh authoritative source is available for research")
            if not forward_test_eligible:
                blockers.extend(source.missing_evidence_classes)
            if forward_test_eligible and not allocation_source_qualified:
                blockers.append(
                    "independent authoritative source redundancy remains required for allocation"
                )
            if source_sufficiency_state == "stale":
                blockers.append("authoritative source evidence is stale")
        if operating_row is not None:
            blockers.extend(item for item in operating_row.blockers if item not in blockers)

        if not evidence_producer:
            qualification_stage = "upstream_evidence_producer_missing"
        elif not research_eligible:
            qualification_stage = f"waiting_for_source:{source_sufficiency_state}"
        elif not forward_test_eligible:
            qualification_stage = "research_active_waiting_for_complete_forward_evidence"
        elif provisional and not decision_grade_outcome_qualified:
            qualification_stage = "provisional_forward_positive"
        elif not allocation_source_qualified:
            qualification_stage = "forward_learning_active_redundancy_pending"
        elif operating_row is not None:
            qualification_stage = operating_row.stage
        elif decision_grade_outcome_qualified:
            qualification_stage = "qualified_for_paper_allocation"
        else:
            qualification_stage = "awaiting_operating_snapshot"

        if not evidence_producer:
            execution_state = "execution_code_present_upstream_evidence_missing"
        elif not architecture_capable:
            execution_state = "architecture_incomplete"
        elif paper_capable:
            execution_state = "qualified_for_paper_allocation"
        elif not research_eligible:
            execution_state = "source_blocked"
        elif not forward_test_eligible:
            execution_state = "research_active_forward_evidence_incomplete"
        elif provisional:
            execution_state = "research_active_provisional_forward_positive"
        elif not allocation_source_qualified:
            execution_state = "research_active_allocation_source_redundancy_pending"
        elif forward_count < 3:
            execution_state = "collecting_forward_evidence"
        else:
            execution_state = "awaiting_decision_grade_qualification"

        rows.append(
            LaneExecutableReadiness(
                lane_id=lane_id,
                name=str(definition["name"]),
                source_layer_sufficient=source_ready,
                research_eligible=research_eligible,
                forward_test_eligible=forward_test_eligible,
                allocation_source_qualified=allocation_source_qualified,
                provider_connectivity_state=provider_connectivity_state,
                source_sufficiency_state=source_sufficiency_state,
                source_headline_state=source_headline_state,
                qualification_stage=qualification_stage,
                evidence_producer_implemented=evidence_producer,
                production_evidence_path_connected=production_connected,
                economics_model_implemented=bool(architecture["economics"]),
                forward_loop_implemented=bool(architecture["forward"]),
                statistical_gate_implemented=bool(architecture["statistics"]),
                allocation_bridge_implemented=bool(architecture["allocation"]),
                settlement_contract_implemented=bool(architecture["settlement"]),
                architecture_execution_capable=architecture_capable,
                decision_grade_outcome_qualified=decision_grade_outcome_qualified,
                paper_execution_capable=paper_capable,
                provisional_forward_positive=provisional,
                currently_qualified=currently_qualified,
                profitability_certified=profitability_certified,
                execution_state=execution_state,
                source_state=source_state,
                forward_outcome_count=forward_count,
                current_promoted_candidate_count=promoted_count,
                blockers=blockers,
            )
        )

    return LaneExecutableReadinessSnapshot(
        observed_at=datetime.now(timezone.utc),
        lane_count=len(rows),
        architecture_executable_count=sum(
            row.architecture_execution_capable for row in rows
        ),
        production_evidence_connected_count=sum(
            row.production_evidence_path_connected for row in rows
        ),
        all_lanes_production_evidence_connected=all(
            row.production_evidence_path_connected for row in rows
        ),
        research_eligible_count=sum(row.research_eligible for row in rows),
        forward_test_eligible_count=sum(row.forward_test_eligible for row in rows),
        provisional_forward_positive_count=sum(
            row.provisional_forward_positive for row in rows
        ),
        decision_grade_outcome_qualified_count=sum(
            row.decision_grade_outcome_qualified for row in rows
        ),
        currently_qualified_count=sum(row.currently_qualified for row in rows),
        paper_execution_capable_count=sum(row.paper_execution_capable for row in rows),
        profitability_certified_count=sum(row.profitability_certified for row in rows),
        all_lanes_paper_execution_capable=all(row.paper_execution_capable for row in rows),
        lanes=rows,
        interpretation=(
            "architecture_execution_capable describes downstream code capability only. "
            "decision_grade_outcome_qualified means the durable qualification plane has reached a positive allocation-grade "
            "conclusion; currently_qualified separately reports whether a current promoted opportunity/candidate exists. "
            "paper_execution_capable requires the architecture, a connected production evidence path, allocation-grade source "
            "qualification, and a positive decision-grade outcome. Research eligibility, forward-test eligibility, and "
            "provisional positive evidence are diagnostic only and grant no portfolio authority. Capital-location remains "
            "fail-closed until authoritative transfer cost/latency evidence is genuinely produced. Final economic, statistical, "
            "execution, risk, settlement and profitability thresholds remain unchanged."
        ),
    )
