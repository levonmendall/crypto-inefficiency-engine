from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.candidate_observatory_lane_coverage import summarize_lane_coverage
from inefficiency_engine.source_coverage_catalog import LANES


START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
BOUNDARY = datetime(2026, 8, 25, 16, 30, tzinfo=timezone.utc)


def _record(observed_at: datetime, lanes: list[str], *, omitted: list[str] | None = None):
    return {
        "source_table": "worker_heartbeats",
        "observed_at": observed_at.isoformat(),
        "payload": {
            "observed_at": observed_at.isoformat(),
            "funnels": {lane: {"raw_candidate_count": 0, "emitted_candidate_count": 0} for lane in lanes},
            "omitted_untrusted_legacy_funnels": omitted or [],
        },
    }


def _history(
    lane_id: str,
    *,
    classes: set[str] | None = None,
    source_start: datetime | None = None,
    source_end: datetime | None = None,
    source_count: int = 0,
    operating_count: int = 0,
):
    result = {}
    for key in LANES:
        result[key] = {
            "source_count": 0,
            "source_earliest": None,
            "source_latest": None,
            "source_ids": set(),
            "evidence_classes": set(),
            "operating_count": 0,
            "operating_earliest": None,
            "operating_latest": None,
            "latest_operating_at": None,
            "latest_operating_state": None,
            "max_authoritative_observation_count": 0,
            "max_economic_candidate_count": 0,
            "max_forward_signal_count": 0,
            "max_independent_forward_outcome_count": 0,
        }
    row = result[lane_id]
    row["source_count"] = source_count
    row["source_earliest"] = source_start
    row["source_latest"] = source_end
    row["source_ids"] = {"test-source"} if source_count else set()
    row["evidence_classes"] = set(classes or set())
    row["operating_count"] = operating_count
    row["operating_earliest"] = START + timedelta(hours=1) if operating_count else None
    row["operating_latest"] = BOUNDARY - timedelta(hours=1) if operating_count else None
    row["latest_operating_at"] = row["operating_latest"]
    row["latest_operating_state"] = "collecting" if operating_count else None
    row["max_authoritative_observation_count"] = 12 if operating_count else 0
    row["max_economic_candidate_count"] = 3 if operating_count else 0
    return result


def test_all_thirteen_lanes_must_be_present_before_backfill_certifies():
    lane_ids = list(LANES)
    result = summarize_lane_coverage(
        [
            _record(START + timedelta(hours=1), lane_ids),
            _record(BOUNDARY - timedelta(hours=1), lane_ids),
        ],
        start=START,
        boundary=BOUNDARY,
    )

    assert result["required_lane_count"] == 13
    assert result["complete_lane_count"] == 13
    assert result["required_lanes_complete"] is True
    assert all(row["state"] == "complete" for row in result["lanes"].values())


def test_missing_required_lane_keeps_global_backfill_incomplete():
    missing = "capital_location_settlement"
    present = [lane for lane in LANES if lane != missing]
    result = summarize_lane_coverage(
        [
            _record(START + timedelta(hours=1), present),
            _record(BOUNDARY - timedelta(hours=1), present),
        ],
        start=START,
        boundary=BOUNDARY,
    )

    assert result["required_lanes_complete"] is False
    assert result["lanes"][missing]["state"] == "unavailable"
    assert result["unavailable_lane_count"] == 1


def test_lane_that_only_appears_late_is_partial_not_complete():
    lane_ids = list(LANES)
    late_lane = "yield"
    early = [lane for lane in lane_ids if lane != late_lane]
    result = summarize_lane_coverage(
        [
            _record(START + timedelta(hours=1), early),
            _record(START + timedelta(days=2), [late_lane]),
            _record(BOUNDARY - timedelta(hours=1), lane_ids),
        ],
        start=START,
        boundary=BOUNDARY,
    )

    assert result["required_lanes_complete"] is False
    assert result["lanes"][late_lane]["state"] == "partial"
    assert "starts after" in str(result["lanes"][late_lane]["reason"])


def test_untrusted_legacy_microstructure_cannot_falsely_certify():
    lane_ids = list(LANES)
    result = summarize_lane_coverage(
        [
            _record(START + timedelta(hours=1), lane_ids),
            _record(
                BOUNDARY - timedelta(hours=1),
                lane_ids,
                omitted=["microstructure"],
            ),
        ],
        start=START,
        boundary=BOUNDARY,
    )

    assert result["required_lanes_complete"] is False
    assert result["lanes"]["microstructure"]["state"] == "partial"
    assert result["lanes"]["microstructure"]["omitted_untrusted_records"] == 1


def test_zero_candidate_funnel_still_counts_as_lane_observation():
    result = summarize_lane_coverage(
        [
            _record(START + timedelta(hours=1), list(LANES)),
            _record(BOUNDARY - timedelta(hours=1), list(LANES)),
        ],
        start=START,
        boundary=BOUNDARY,
    )

    assert result["lanes"]["volatility"]["recovered_funnel_records"] == 2
    assert result["lanes"]["volatility"]["state"] == "complete"


def test_exact_source_history_can_reconstruct_lane_without_inventing_funnels():
    lane_id = "volatility"
    required = set(LANES[lane_id]["required"])
    history = _history(
        lane_id,
        classes=required,
        source_start=START + timedelta(hours=1),
        source_end=BOUNDARY - timedelta(hours=1),
        source_count=17,
        operating_count=9,
    )
    result = summarize_lane_coverage(
        [],
        start=START,
        boundary=BOUNDARY,
        persisted_history=history,
    )

    row = result["lanes"][lane_id]
    assert row["state"] == "complete"
    assert row["reconstruction_quality"] == "exact_source_evidence_history"
    assert row["recovered_funnel_records"] == 0
    assert row["recovered_source_observations"] == 17
    assert row["recovered_operating_snapshots"] == 9
    assert row["candidate_level_rejections_reconstructable"] is False


def test_source_history_missing_one_required_class_remains_partial():
    lane_id = "yield"
    required = set(LANES[lane_id]["required"])
    missing = next(iter(required))
    history = _history(
        lane_id,
        classes=required - {missing},
        source_start=START + timedelta(hours=1),
        source_end=BOUNDARY - timedelta(hours=1),
        source_count=20,
    )
    result = summarize_lane_coverage(
        [],
        start=START,
        boundary=BOUNDARY,
        persisted_history=history,
    )

    row = result["lanes"][lane_id]
    assert row["state"] == "partial"
    assert missing in row["missing_historical_evidence_classes"]
    assert "missing historical evidence classes" in str(row["reason"])


def test_operating_history_is_visible_but_cannot_replace_missing_source_evidence():
    lane_id = "event_driven"
    history = _history(lane_id, operating_count=14)
    result = summarize_lane_coverage(
        [],
        start=START,
        boundary=BOUNDARY,
        persisted_history=history,
    )

    row = result["lanes"][lane_id]
    assert row["state"] == "partial"
    assert row["reconstruction_quality"] == "operating_history_only"
    assert row["max_authoritative_observation_count"] == 12
    assert row["max_economic_candidate_count"] == 3


def test_capital_location_does_not_certify_without_transfer_classes():
    lane_id = "capital_location_settlement"
    history = _history(
        lane_id,
        classes={"venue_opportunity_history"},
        source_start=START + timedelta(hours=1),
        source_end=BOUNDARY - timedelta(hours=1),
        source_count=22,
        operating_count=12,
    )
    result = summarize_lane_coverage(
        [],
        start=START,
        boundary=BOUNDARY,
        persisted_history=history,
    )

    row = result["lanes"][lane_id]
    assert row["state"] == "partial"
    assert "transfer_costs" in row["missing_historical_evidence_classes"]
    assert "transfer_latency" in row["missing_historical_evidence_classes"]
