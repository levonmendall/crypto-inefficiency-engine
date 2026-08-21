from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane
from inefficiency_engine.source_coverage_catalog import LANES


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_source_plane_has_exactly_thirteen_canonical_lanes(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    snapshot = plane.snapshot()
    assert len(LANES) == 13
    assert snapshot.lane_count == 13
    assert {row.lane_id for row in snapshot.lanes} == set(LANES)
    assert snapshot.paper_only is True
    assert snapshot.allocation_authority is False
    assert snapshot.live_execution_authority is False


def test_two_independent_event_sources_satisfy_source_layer(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    now = _now()
    for source_id in ("bybit-catalog", "snapshot-governance"):
        plane.record(SourceCoverageObservation(
            source_id=source_id,
            lane_id="event_driven",
            observed_at=now,
            healthy=True,
            item_count=3,
            evidence_classes=["timestamped_events", "event_identity"],
        ))
    row = plane.lane("event_driven")
    assert row.independent_authoritative_source_count >= 2
    assert row.source_redundancy_satisfied is True
    assert row.evidence_class_coverage_satisfied is True
    assert row.source_layer_sufficient is True
    assert row.downstream_evidence_gaps == []


def test_stale_source_does_not_count_toward_redundancy(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store, max_age_hours=24)
    now = _now()
    plane.record(SourceCoverageObservation(
        source_id="bybit-catalog", lane_id="event_driven", observed_at=now,
        healthy=True, item_count=1, evidence_classes=["timestamped_events", "event_identity"],
    ))
    plane.record(SourceCoverageObservation(
        source_id="snapshot-governance", lane_id="event_driven", observed_at=now - timedelta(hours=30),
        healthy=True, item_count=1, evidence_classes=["timestamped_events", "event_identity"],
    ))
    row = next(item for item in plane.snapshot(now=now).lanes if item.lane_id == "event_driven")
    assert row.independent_authoritative_source_count == 1
    assert row.source_layer_sufficient is False
    assert row.source_state == "concentration_risk"


def test_optional_paid_source_is_explicitly_credential_gated(tmp_path, monkeypatch):
    monkeypatch.delenv("CIE_TOKENOMIST_API_KEY", raising=False)
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    row = plane.lane("event_driven")
    tokenomist = next(item for item in row.sources if item["source_id"] == "tokenomist-unlocks")
    assert tokenomist["state"] == "credential_required"
    assert tokenomist["admitted"] is False


def test_secondary_source_never_satisfies_authoritative_redundancy(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    now = _now()
    plane.record(SourceCoverageObservation(
        source_id="ethereum-publicnode", lane_id="fundamental_onchain", observed_at=now,
        healthy=True, item_count=1, evidence_classes=["chain_activity"], authoritative=True,
    ))
    plane.record(SourceCoverageObservation(
        source_id="defillama-protocols", lane_id="fundamental_onchain", observed_at=now,
        healthy=True, item_count=100, evidence_classes=["protocol_fundamentals"], authoritative=False,
    ))
    row = plane.lane("fundamental_onchain")
    defillama = next(item for item in row.sources if item["source_id"] == "defillama-protocols")
    assert defillama["authoritative"] is False
    assert defillama["admitted"] is False
    assert row.independent_authoritative_source_count == 1
    assert row.source_layer_sufficient is False


def test_liquidation_source_layer_separates_downstream_calibration(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    now = _now()
    plane.record(SourceCoverageObservation(
        source_id="bybit-liquidations", lane_id="liquidation_distress", observed_at=now,
        healthy=True, evidence_classes=["liquidation_events"], item_count=0,
    ))
    plane.record(SourceCoverageObservation(
        source_id="aave-liquidations", lane_id="liquidation_distress", observed_at=now,
        healthy=True, evidence_classes=["liquidation_events"], item_count=0,
    ))
    plane.record(SourceCoverageObservation(
        source_id="hyperliquid-distress", lane_id="liquidation_distress", observed_at=now,
        healthy=True, evidence_classes=["distress_state"], item_count=10,
    ))
    row = plane.lane("liquidation_distress")
    assert row.source_layer_sufficient is True
    assert "capture-probability calibration" in row.downstream_evidence_gaps
    assert "recovery/settlement outcomes" in row.downstream_evidence_gaps
    assert row.allocation_authority is False
