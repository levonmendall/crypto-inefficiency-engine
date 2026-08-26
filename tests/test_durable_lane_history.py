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
        "canonical_snapshot_count": 0,
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


def _empty_history() -> dict[str, dict[str, object]]:
    return {lane_id: _empty_state() for lane_id in LANES}


def _install_canonical(
    monkeypatch,
    *,
    summary: dict[str, dict[str, object]] | None = None,
    first: datetime | None = None,
    complete: bool = True,
) -> None:
    class FakeLedger:
        def __init__(self, _store):
            pass

        def migration_status(self):
            return {
                "checkpoint_heartbeat_id": 123,
                "complete": complete,
                "updated_at": "2026-08-25T00:00:00+00:00",
            }

        def first_snapshot_at(self):
            return first

        def summary(self, *, start, end):
            return summary or {}

    monkeypatch.setattr(history, "SourceCoverageHistoryLedger", FakeLedger)


def _disable_secondary_reads(monkeypatch) -> None:
    monkeypatch.setattr(
        history,
        "_read_bounded_source_history",
        lambda *_args, **_kwargs: _empty_history(),
    )
    monkeypatch.setattr(
        history,
        "_read_bounded_operating_history",
        lambda *_args, **_kwargs: _empty_history(),
    )


def test_durable_history_reports_all_lanes_without_claiming_prelive_completion(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    persisted = _empty_history()
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
    _install_canonical(monkeypatch, complete=True)
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda _store, *, start, boundary: persisted,
    )
    _disable_secondary_reads(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"][lane_id]

    assert payload["lane_count"] == 13
    assert payload["lanes_with_durable_history"] == 1
    assert payload["read_degraded"] is False
    assert row["history_available"] is True
    assert row["evidence_class_history_complete"] is True
    assert row["recovered_evidence_class_count"] == 2
    assert row["required_evidence_class_count"] == 2
    assert row["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert "post-live evidence does not certify the strict pre-live backfill" in payload["history_contract"]


def test_durable_history_keeps_missing_evidence_classes_visible(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    persisted = _empty_history()
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
    _install_canonical(monkeypatch, complete=True)
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda _store, *, start, boundary: persisted,
    )
    _disable_secondary_reads(monkeypatch)

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
    persisted = _empty_history()
    persisted["event_driven"] = {
        **_empty_state(),
        "source_count": 99,
        "source_earliest": datetime(2026, 8, 24, tzinfo=timezone.utc),
        "source_latest": datetime(2026, 8, 25, tzinfo=timezone.utc),
        "source_ids": {"coinbase-catalog"},
        "evidence_classes": {"timestamped_events", "event_identity"},
        "source_ledgers": {"source_event_observations"},
    }
    _install_canonical(monkeypatch, complete=True)
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda _store, *, start, boundary: persisted,
    )
    _disable_secondary_reads(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"]["event_driven"]

    assert row["recovered_source_observations"] == 99
    assert "candidate_count" not in row
    assert row["candidate_level_history_synthesized"] is False


def test_durable_history_fail_soft_keeps_canonical_denominators(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)

    class FailingLedger:
        def __init__(self, _store):
            raise RuntimeError("simulated canonical history failure")

    def fail(*_args, **_kwargs):
        raise RuntimeError("simulated operating read failure")

    monkeypatch.setattr(history, "SourceCoverageHistoryLedger", FailingLedger)
    monkeypatch.setattr(history, "_read_bounded_operating_history", fail)
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw reconstruction must remain disabled without a completed migration")
        ),
    )

    payload = history.build_durable_lane_history(object(), start=start, end=end)

    assert payload["lane_count"] == 13
    assert payload["read_degraded"] is True
    assert {item["stage"] for item in payload["read_errors"]} == {
        "canonical_source_coverage_history",
        "bounded_operating_history",
    }
    for lane_id, definition in LANES.items():
        row = payload["lanes"][lane_id]
        assert row["required_evidence_class_count"] == len(definition["required"])
        assert row["required_evidence_class_count"] > 0
        assert row["recovered_evidence_class_count"] == 0
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False


def test_incomplete_canonical_migration_skips_raw_http_reconstruction(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    canonical = {
        "yield": {
            **_empty_state(),
            "canonical_snapshot_count": 4,
            "source_count": 4,
            "source_earliest": datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
            "source_latest": datetime(2026, 8, 25, 2, tzinfo=timezone.utc),
            "source_ids": {"morpho-markets"},
            "evidence_classes": {"yield_rate", "capacity", "exit_liquidity"},
            "source_ledgers": {"source_coverage_history"},
        }
    }
    _install_canonical(
        monkeypatch,
        summary=canonical,
        first=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
        complete=False,
    )
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw reconstruction must not run during archive migration")
        ),
    )
    monkeypatch.setattr(
        history,
        "_read_bounded_source_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy source reconstruction must not run during migration")
        ),
    )
    monkeypatch.setattr(
        history,
        "_read_bounded_operating_history",
        lambda *_args, **_kwargs: _empty_history(),
    )

    payload = history.build_durable_lane_history(object(), start=start, end=end)
    row = payload["lanes"]["yield"]

    assert payload["canonical_history_migration_complete"] is False
    assert row["canonical_source_snapshot_count"] == 4
    assert row["recovered_evidence_class_count"] == 3
    assert row["history_available"] is True


def test_completed_migration_limits_raw_recovery_to_precanonical_boundary(monkeypatch):
    start = datetime(2026, 8, 21, tzinfo=timezone.utc)
    first = datetime(2026, 8, 22, tzinfo=timezone.utc)
    end = datetime(2026, 8, 25, tzinfo=timezone.utc)
    calls: list[datetime] = []

    _install_canonical(monkeypatch, summary={}, first=first, complete=True)

    def raw(_store, *, start, boundary):
        calls.append(boundary)
        return _empty_history()

    monkeypatch.setattr(history, "recover_raw_lane_history", raw)
    _disable_secondary_reads(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=start, end=end)

    assert calls == [first]
    assert payload["canonical_history_migration_complete"] is True
    assert payload["canonical_history_started_at"] == first.isoformat()
