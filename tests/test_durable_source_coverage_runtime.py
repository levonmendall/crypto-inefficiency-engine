from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.control_cycle_executor import run_one_control_cycle
from inefficiency_engine.durable_source_coverage_runtime import (
    SOURCE_COVERAGE_SNAPSHOT_WORKER_ID,
    DurableSourceCoverageSnapshotMissing,
    DurableSourceCoverageSnapshotStale,
    load_persisted_source_coverage_snapshot,
    persist_source_coverage_snapshot,
)
from inefficiency_engine.source_coverage import LaneSourceCoverage, SourceCoverageSnapshot
from inefficiency_engine.source_runtime_safety import (
    install_source_coverage_reconciliation_runtime,
)


NOW = datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc)


class _HeartbeatStore:
    def __init__(self) -> None:
        self.heartbeat = None
        self.worker_id = None

    def record_worker_heartbeat(self, *, worker_id, state, detail, **_kwargs):
        self.worker_id = worker_id
        self.heartbeat = SimpleNamespace(
            worker_id=worker_id,
            state=state,
            detail=detail,
            observed_at=NOW,
        )

    def latest_worker_heartbeat(self, worker_id):
        if self.worker_id != worker_id:
            return None
        return self.heartbeat


def _snapshot(*, observed_at: datetime = NOW) -> SourceCoverageSnapshot:
    sources = [
        {
            "source_id": "coinbase-market",
            "name": "Coinbase market data",
            "classes": ["market_quotes"],
            "group": "coinbase",
            "tier": "first_party",
            "authoritative": True,
            "state": "healthy",
            "healthy": True,
            "fresh": True,
            "admitted": True,
            "observed_at": (observed_at - timedelta(seconds=10)).isoformat(),
            "age_seconds": 10.0,
            "age_hours": 10.0 / 3600.0,
            "freshness_ttl_seconds": 60.0,
            "item_count": 1,
        },
        {
            "source_id": "okx-market",
            "name": "OKX market data",
            "classes": ["market_quotes"],
            "group": "okx",
            "tier": "first_party",
            "authoritative": True,
            "state": "healthy",
            "healthy": True,
            "fresh": True,
            # Preserve an original non-admission reason fail-closed. The persisted
            # reader may age evidence down but must never promote this row later.
            "admitted": False,
            "observed_at": (observed_at - timedelta(seconds=5)).isoformat(),
            "age_seconds": 5.0,
            "age_hours": 5.0 / 3600.0,
            "freshness_ttl_seconds": 60.0,
            "item_count": 1,
        },
    ]
    lane = LaneSourceCoverage(
        lane_id="price_discrepancy",
        name="Price discrepancy / arbitrage",
        required_evidence_classes=["market_quotes"],
        covered_evidence_classes=["market_quotes"],
        missing_evidence_classes=[],
        downstream_evidence_gaps=[],
        healthy_source_count=1,
        independent_authoritative_source_count=1,
        source_redundancy_satisfied=False,
        evidence_class_coverage_satisfied=True,
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=False,
        source_layer_sufficient=False,
        source_state="concentration_risk",
        sources=sources,
    )
    return SourceCoverageSnapshot(
        observed_at=observed_at,
        lane_count=1,
        sufficient_lane_count=0,
        insufficient_lane_count=1,
        research_eligible_lane_count=1,
        forward_test_eligible_lane_count=1,
        allocation_source_qualified_lane_count=0,
        priority_order=["price_discrepancy"],
        lanes=[lane],
    )


def test_persisted_snapshot_reages_sources_without_promoting_prior_nonadmission():
    store = _HeartbeatStore()
    snapshot = _snapshot()

    assert persist_source_coverage_snapshot(store, snapshot) is True
    assert store.worker_id == SOURCE_COVERAGE_SNAPSHOT_WORKER_ID

    current = load_persisted_source_coverage_snapshot(
        store,
        now=NOW + timedelta(seconds=30),
        max_age_seconds=90.0,
    )
    lane = current.lanes[0]
    by_id = {row["source_id"]: row for row in lane.sources}
    assert by_id["coinbase-market"]["admitted"] is True
    assert by_id["okx-market"]["admitted"] is False
    assert lane.research_eligible is True
    assert lane.forward_test_eligible is True
    assert lane.allocation_source_qualified is False

    expired = load_persisted_source_coverage_snapshot(
        store,
        now=NOW + timedelta(seconds=70),
        max_age_seconds=90.0,
    )
    lane = expired.lanes[0]
    assert all(row["admitted"] is False for row in lane.sources)
    assert lane.research_eligible is False
    assert lane.forward_test_eligible is False
    assert lane.allocation_source_qualified is False
    assert lane.source_state == "provider_gap"


def test_missing_or_old_persisted_snapshot_fails_closed():
    empty = _HeartbeatStore()
    with pytest.raises(DurableSourceCoverageSnapshotMissing):
        load_persisted_source_coverage_snapshot(empty, now=NOW)

    store = _HeartbeatStore()
    persist_source_coverage_snapshot(store, _snapshot(observed_at=NOW - timedelta(seconds=100)))
    with pytest.raises(DurableSourceCoverageSnapshotStale):
        load_persisted_source_coverage_snapshot(
            store,
            now=NOW,
            max_age_seconds=90.0,
        )


def test_runtime_wires_priority_publication_before_control_persisted_read():
    source_runtime = inspect.getsource(install_source_coverage_reconciliation_runtime)
    control_executor = inspect.getsource(run_one_control_cycle)

    assert "install_source_coverage_snapshot_publisher_runtime()" in source_runtime
    assert "install_control_source_coverage_snapshot_reader_runtime()" in control_executor
    assert control_executor.index(
        "install_source_coverage_reconciliation_runtime()"
    ) < control_executor.index(
        "install_control_source_coverage_snapshot_reader_runtime()"
    )
