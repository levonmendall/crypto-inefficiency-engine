from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.operating_certification import OperatingCertificationLedger
from inefficiency_engine.source_coverage import SourceCoveragePlane
from inefficiency_engine.source_coverage_catalog import LANES


class LaneExecutableReadiness(BaseModel):
    lane_id: str
    name: str
    source_layer_sufficient: bool
    economics_model_implemented: bool
    forward_loop_implemented: bool
    statistical_gate_implemented: bool
    allocation_bridge_implemented: bool
    settlement_contract_implemented: bool
    paper_execution_capable: bool
    currently_qualified: bool
    profitability_certified: bool
    execution_state: str
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
    currently_qualified_count: int
    profitability_certified_count: int
    all_lanes_paper_execution_capable: bool
    lanes: list[LaneExecutableReadiness]
    interpretation: str
    paper_only: bool = True
    live_execution_capable: bool = False


# These contracts are code capabilities, not claims about current profitability.
# Every entry is true only because a concrete forward/statistical/allocation/settlement
# path exists in the canonical paper architecture after the all-lane closure.
_ARCHITECTURE = {
    lane_id: {
        "economics": True,
        "forward": True,
        "statistics": True,
        "allocation": True,
        "settlement": True,
    }
    for lane_id in LANES
}


def build_lane_executable_readiness(core, store) -> LaneExecutableReadinessSnapshot:
    source_plane = SourceCoveragePlane(store)
    source_snapshot = source_plane.snapshot()
    source_by_id = {row.lane_id: row for row in source_snapshot.lanes}
    operating = OperatingCertificationLedger(store).latest()
    operating_by_id = {
        row.mechanism_id: row for row in (operating.mechanisms if operating is not None else [])
    }
    mechanisms = ExecutableMechanismExecutionService(core, store)
    mechanism_readiness = mechanisms.readiness_summary()

    rows: list[LaneExecutableReadiness] = []
    for lane_id, definition in LANES.items():
        architecture = _ARCHITECTURE[lane_id]
        source = source_by_id.get(lane_id)
        source_ready = bool(source and source.source_layer_sufficient)
        source_state = source.source_state if source is not None else "unobserved"
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
                )
            else:
                currently_qualified = promoted_count > 0

        profitability_certified = bool(
            operating_row is not None and operating_row.profitability_certified
        )
        paper_capable = all(bool(value) for value in architecture.values())
        blockers: list[str] = []
        if not source_ready:
            if source is not None:
                blockers.extend(source.missing_evidence_classes)
                if source.source_state == "concentration_risk":
                    blockers.append("authoritative source redundancy target is not satisfied")
            else:
                blockers.append("source coverage has not been observed")
        if operating_row is not None:
            blockers.extend(item for item in operating_row.blockers if item not in blockers)

        if not paper_capable:
            execution_state = "architecture_incomplete"
        elif currently_qualified:
            execution_state = "qualified_for_paper_allocation"
        elif not source_ready:
            execution_state = "execution_capable_source_blocked"
        elif forward_count < 3 and lane_id in MECHANISM_IDS:
            execution_state = "execution_capable_collecting_forward_evidence"
        else:
            execution_state = "execution_capable_awaiting_qualification"

        rows.append(LaneExecutableReadiness(
            lane_id=lane_id,
            name=str(definition["name"]),
            source_layer_sufficient=source_ready,
            economics_model_implemented=bool(architecture["economics"]),
            forward_loop_implemented=bool(architecture["forward"]),
            statistical_gate_implemented=bool(architecture["statistics"]),
            allocation_bridge_implemented=bool(architecture["allocation"]),
            settlement_contract_implemented=bool(architecture["settlement"]),
            paper_execution_capable=paper_capable,
            currently_qualified=currently_qualified,
            profitability_certified=profitability_certified,
            execution_state=execution_state,
            source_state=source_state,
            forward_outcome_count=forward_count,
            current_promoted_candidate_count=promoted_count,
            blockers=blockers,
        ))

    return LaneExecutableReadinessSnapshot(
        observed_at=datetime.now(timezone.utc),
        lane_count=len(rows),
        architecture_executable_count=sum(row.paper_execution_capable for row in rows),
        currently_qualified_count=sum(row.currently_qualified for row in rows),
        profitability_certified_count=sum(row.profitability_certified for row in rows),
        all_lanes_paper_execution_capable=all(row.paper_execution_capable for row in rows),
        lanes=rows,
        interpretation=(
            "paper_execution_capable means a fail-closed source/economics/forward/statistical/allocation/settlement "
            "path exists in code; currently_qualified means real accumulated evidence presently authorizes a paper allocation."
        ),
    )
