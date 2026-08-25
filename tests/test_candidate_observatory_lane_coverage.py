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

    # Coverage is about whether a lane actually ran and persisted its funnel, not
    # whether it happened to emit an opportunity.
    assert result["lanes"]["volatility"]["recovered_funnel_records"] == 2
    assert result["lanes"]["volatility"]["state"] == "complete"
