from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine import durable_lane_history as history
from inefficiency_engine.source_coverage_catalog import LANES


def _empty_state() -> dict[str, object]:
    return {
        "source_count": 0,
        "source_earliest": None,
        "source_latest": None,
        "source_ids": set(),
        "evidence_classes": set(),
        "source_ledgers": set(),
        "operating_count": 0,
        "operating_earliest": None,
        "operating_latest": None,
        "latest_operating_state": None,
        "max_authoritative_observation_count": 0,
        "max_economic_candidate_count": 0,
        "max_forward_signal_count": 0,
        "max_independent_forward_outcome_count": 0,
    }


def test_durable_history_reports_all_lanes_without_claiming_prelive_completion(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    persisted = {lane_id: _empty_state() for lane_id in LANES}
    lane_id = "trend_momentum"
    persisted[lane_id] = {
        **_empty_state(),
        "source_count": 14,
        "source_earliest": datetime(2026, 8, 22, tzinfo=timezone.utc),
        "source_latest": datetime(2026, 8, 25, tzinfo=timezone.utc),
        "source_ids": {"coinbase-market"},
        "evidence_classes": {"market_history", "execution_costs"},
        "source_ledgers": {"market_quotes"},
    }

    monkeypatch.setattr(
        history,
        "_read_persisted_lane_history",
        lambda _store, *, start, boundary: persisted,
    )

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"][lane_id]

    assert payload["lane_count"] == 13
    assert payload["lanes_with_durable_history"] == 1
    assert row["history_available"] is True
    assert row["evidence_class_history_complete"] is True
    assert row["recovered_evidence_class_count"] == 2
    assert row["required_evidence_class_count"] == 2
    assert row["earliest_recovered_at"] == "2026-08-22T00:00:00+00:00"
    assert row["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert "post-live evidence does not certify the strict pre-live backfill" in payload["history_contract"]


def test_durable_history_keeps_missing_evidence_classes_visible(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    persisted = {lane_id: _empty_state() for lane_id in LANES}
    lane_id = "liquidity_provision"
    persisted[lane_id] = {
        **_empty_state(),
        "source_count": 3,
        "source_earliest": datetime(2026, 8, 23, tzinfo=timezone.utc),
        "source_latest": datetime(2026, 8, 25, tzinfo=timezone.utc),
        "source_ids": {"coinbase-l2"},
        "evidence_classes": {"order_book"},
        "source_ledgers": {"order_books"},
    }
    monkeypatch.setattr(
        history,
        "_read_persisted_lane_history",
        lambda _store, *, start, boundary: persisted,
    )

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"][lane_id]

    assert row["history_available"] is True
    assert row["evidence_class_history_complete"] is False
    assert row["recovered_evidence_class_count"] == 1
    assert row["required_evidence_class_count"] == 2
    assert row["missing_historical_evidence_classes"] == ["trade_flow"]
    assert row["evidence_class_fill_ratio"] == 0.5


def test_durable_history_never_turns_source_records_into_candidate_counts(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    persisted = {lane_id: _empty_state() for lane_id in LANES}
    persisted["event_driven"] = {
        **_empty_state(),
        "source_count": 99,
        "source_earliest": datetime(2026, 8, 24, tzinfo=timezone.utc),
        "source_latest": datetime(2026, 8, 25, tzinfo=timezone.utc),
        "source_ids": {"coinbase-catalog"},
        "evidence_classes": {"timestamped_events", "event_identity"},
        "source_ledgers": {"source_event_observations"},
    }
    monkeypatch.setattr(
        history,
        "_read_persisted_lane_history",
        lambda _store, *, start, boundary: persisted,
    )

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"]["event_driven"]

    assert row["recovered_source_observations"] == 99
    assert "candidate_count" not in row
    assert row["candidate_level_history_synthesized"] is False
