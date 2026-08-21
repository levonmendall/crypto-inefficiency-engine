from __future__ import annotations

from types import SimpleNamespace

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.lane_readiness import build_lane_executable_readiness


def core():
    return SimpleNamespace(settings=Settings())


def test_capital_location_distinguishes_downstream_code_from_missing_producer(tmp_path):
    store = EvidenceStore(tmp_path / "capital-location-readiness.sqlite3")
    snapshot = build_lane_executable_readiness(core(), store)
    lane = next(
        row for row in snapshot.lanes
        if row.lane_id == "capital_location_settlement"
    )

    # The economics/forward/settlement code exists and remains testable if genuine
    # transfer telemetry is supplied, but production does not currently generate it.
    assert lane.paper_execution_capable is True
    assert lane.evidence_producer_implemented is False
    assert lane.production_evidence_path_connected is False
    assert lane.qualification_stage == "upstream_evidence_producer_missing"
    assert lane.execution_state == "execution_code_present_upstream_evidence_missing"
    assert any("transfer-cost/transfer-latency" in item for item in lane.blockers)

    assert snapshot.lane_count == 13
    assert snapshot.architecture_executable_count == 13
    assert snapshot.production_evidence_connected_count == 12
    assert snapshot.all_lanes_paper_execution_capable is True
    assert snapshot.all_lanes_production_evidence_connected is False
