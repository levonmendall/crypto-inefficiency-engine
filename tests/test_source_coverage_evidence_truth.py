from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane


def _source(snapshot, lane_id: str, source_id: str) -> dict[str, object]:
    lane = next(row for row in snapshot.lanes if row.lane_id == lane_id)
    return next(row for row in lane.sources if row["source_id"] == source_id)


def test_newer_failed_attempt_does_not_erase_still_fresh_success(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store, max_age_hours=1)
    now = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
    success_at = now - timedelta(minutes=5)
    failure_at = now - timedelta(minutes=1)

    plane.record(
        SourceCoverageObservation(
            source_id="bybit-catalog",
            lane_id="event_driven",
            observed_at=success_at,
            healthy=True,
            item_count=4,
            evidence_classes=["timestamped_events", "event_identity"],
            source_reference="bybit:success",
        )
    )
    plane.record(
        SourceCoverageObservation(
            source_id="bybit-catalog",
            lane_id="event_driven",
            observed_at=failure_at,
            healthy=False,
            item_count=0,
            evidence_classes=["timestamped_events", "event_identity"],
            error_type="TimeoutError",
            source_reference="bybit:failed-refresh",
        )
    )

    row = _source(plane.snapshot(now=now), "event_driven", "bybit-catalog")

    assert row["state"] == "healthy"
    assert row["healthy"] is True
    assert row["fresh"] is True
    assert row["admitted"] is True
    assert row["observed_at"] == success_at.isoformat()
    assert row["source_reference"] == "bybit:success"
    assert row["error_type"] is None
    assert row["using_prior_fresh_evidence"] is True
    assert row["latest_attempt_state"] == "failed"
    assert row["latest_attempt_observed_at"] == failure_at.isoformat()
    assert row["latest_attempt_error_type"] == "TimeoutError"
    assert row["latest_attempt_source_reference"] == "bybit:failed-refresh"


def test_source_truth_fails_closed_after_last_success_expires(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store, max_age_hours=1)
    now = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

    plane.record(
        SourceCoverageObservation(
            source_id="bybit-catalog",
            lane_id="event_driven",
            observed_at=now - timedelta(hours=2),
            healthy=True,
            item_count=4,
            evidence_classes=["timestamped_events", "event_identity"],
            source_reference="bybit:expired-success",
        )
    )
    plane.record(
        SourceCoverageObservation(
            source_id="bybit-catalog",
            lane_id="event_driven",
            observed_at=now - timedelta(minutes=1),
            healthy=False,
            item_count=0,
            evidence_classes=["timestamped_events", "event_identity"],
            error_type="TimeoutError",
            source_reference="bybit:failed-refresh",
        )
    )

    row = _source(plane.snapshot(now=now), "event_driven", "bybit-catalog")

    assert row["state"] == "failed"
    assert row["admitted"] is False
    assert row["using_prior_fresh_evidence"] is False
    assert row["error_type"] == "TimeoutError"
    assert row["source_reference"] == "bybit:failed-refresh"


def test_append_only_attempt_history_remains_visible(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store, max_age_hours=1)
    now = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

    success = SourceCoverageObservation(
        source_id="bybit-catalog",
        lane_id="event_driven",
        observed_at=now - timedelta(minutes=5),
        healthy=True,
        item_count=4,
        evidence_classes=["timestamped_events", "event_identity"],
    )
    failure = SourceCoverageObservation(
        source_id="bybit-catalog",
        lane_id="event_driven",
        observed_at=now - timedelta(minutes=1),
        healthy=False,
        item_count=0,
        evidence_classes=["timestamped_events", "event_identity"],
        error_type="TimeoutError",
    )
    plane.record(success)
    plane.record(failure)

    recent = plane.ledger.recent(limit=10)
    latest = plane.ledger.latest()[("bybit-catalog", "event_driven")]

    assert [row.observation_id for row in recent[:2]] == [
        failure.observation_id,
        success.observation_id,
    ]
    assert latest.observation_id == failure.observation_id
    assert latest.error_type == "TimeoutError"
