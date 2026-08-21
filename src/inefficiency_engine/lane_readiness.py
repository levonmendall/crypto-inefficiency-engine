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
    currently_qualified_count: int
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
        paper_capable = all(
            bool(architecture[key])
            for key in ("economics", "forward", "statistics", "allocation", "settlement")
        )
        production_connected = paper_capable and evidence_producer
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
        elif provisional and not currently_qualified:
            qualification_stage = "provisional_forward_positive"
        elif not allocation_source_qualified:
            qualification_stage = "forward_learning_active_redundancy_pending"
        elif operating_row is not None:
            qualification_stage = operating_row.stage
        elif currently_qualified:
            qualification_stage = "qualified_for_paper_allocation"
        else:
            qualification_stage = "awaiting_operating_snapshot"

        if not evidence_producer:
            execution_state = "execution_code_present_upstream_evidence_missing"
        elif not paper_capable:
            execution_state = "architecture_incomplete"
        elif currently_qualified:
            execution_state = "qualified_for_paper_allocation"
        elif not research_eligible:
            execution_state = "execution_capable_source_blocked"
        elif not forward_test_eligible:
            execution_state = "research_active_forward_evidence_incomplete"
        elif provisional:
            execution_state = "execution_capable_provisional_forward_positive"
        elif not allocation_source_qualified:
            execution_state = "research_active_allocation_source_redundancy_pending"
        elif forward_count < 3:
            execution_state = "execution_capable_collecting_forward_evidence"
        else:
            execution_state = "execution_capable_awaiting_qualification"

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
        architecture_executable_count=sum(row.paper_execution_capable for row in rows),
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
        currently_qualified_count=sum(row.currently_qualified for row in rows),
        profitability_certified_count=sum(row.profitability_certified for row in rows),
        all_lanes_paper_execution_capable=all(row.paper_execution_capable for row in rows),
        lanes=rows,
        interpretation=(
            "paper_execution_capable describes downstream code capability; production_evidence_path_connected separately "
            "requires a real upstream evidence producer. research_eligible and forward_test_eligible show evidence-learning "
            "progress; allocation_source_qualified/source_layer_sufficient preserve the decision-grade two-source gate. "
            "capital-location remains fail-closed until authoritative transfer cost/latency evidence is genuinely produced. "
            "provisional_forward_positive is diagnostic only and grants no portfolio authority. Final qualification, execution, "
            "risk, settlement and profitability thresholds remain unchanged."
        ),
    )
