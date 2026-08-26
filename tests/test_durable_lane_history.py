from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine import durable_lane_history as history
from inefficiency_engine.candidate_observatory_historical_replay import REPLAY_WORKER_ID
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.source_coverage_catalog import LANES


START = datetime(2026, 8, 21, tzinfo=timezone.utc)
BOUNDARY = datetime(2026, 8, 22, tzinfo=timezone.utc)
END = datetime(2026, 8, 25, tzinfo=timezone.utc)


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


def _disable_operating(monkeypatch) -> None:
    monkeypatch.setattr(
        history,
        "_read_bounded_operating_history",
        lambda *_args, **_kwargs: _empty_history(),
    )


def _install_materialized(
    monkeypatch,
    rows: dict[str, dict[str, object]],
    *,
    available: bool = True,
) -> None:
    monkeypatch.setattr(
        history,
        "_read_materialized_prehistory",
        lambda *_args, **_kwargs: (
            rows,
            {
                "available": available,
                "heartbeat_observed_at": "2026-08-25T00:00:00+00:00",
                "replay_start": START.isoformat(),
                "replay_boundary": BOUNDARY.isoformat(),
                "overlap_rejected": False,
            },
        ),
    )


def _assert_raw_http_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        history,
        "recover_raw_lane_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("raw reconstruction must never run on the dashboard HTTP path")
        ),
    )
    monkeypatch.setattr(
        history,
        "_read_bounded_source_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy source-table scan must never run on the dashboard HTTP path")
        ),
    )


def test_durable_history_merges_materialized_prehistory_without_raw_http(monkeypatch):
    materialized = _empty_history()
    materialized["trend_momentum"] = {
        **_empty_state(),
        "source_count": 14,
        "source_earliest": START + timedelta(hours=1),
        "source_latest": BOUNDARY - timedelta(minutes=1),
        "source_ids": {"coinbase-market"},
        "evidence_classes": {"market_history", "execution_costs"},
        "source_ledgers": {history.MATERIALIZED_PREHISTORY_LEDGER},
    }
    _install_canonical(monkeypatch, first=BOUNDARY + timedelta(hours=1), complete=False)
    _install_materialized(monkeypatch, materialized)
    _disable_operating(monkeypatch)
    _assert_raw_http_disabled(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=START, end=END)
    row = payload["lanes"]["trend_momentum"]

    assert payload["lane_count"] == 13
    assert payload["materialized_prehistory_available"] is True
    assert payload["raw_history_reconstruction_on_http"] is False
    assert payload["read_model"] == "canonical_source_coverage_history_plus_materialized_prehistory"
    assert row["history_available"] is True
    assert row["evidence_class_history_complete"] is True
    assert row["recovered_source_observations"] == 14
    assert row["recovered_evidence_class_count"] == 2
    assert row["required_evidence_class_count"] == 2
    assert row["candidate_level_history_synthesized"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False


def test_incomplete_archive_migration_does_not_hide_materialized_prehistory(monkeypatch):
    materialized = _empty_history()
    materialized["event_driven"] = {
        **_empty_state(),
        "source_count": 9,
        "source_earliest": START,
        "source_latest": BOUNDARY,
        "source_ids": {"coinbase-catalog"},
        "evidence_classes": {"timestamped_events", "event_identity"},
        "source_ledgers": {history.MATERIALIZED_PREHISTORY_LEDGER},
    }
    _install_canonical(monkeypatch, first=BOUNDARY + timedelta(hours=1), complete=False)
    _install_materialized(monkeypatch, materialized)
    _disable_operating(monkeypatch)
    _assert_raw_http_disabled(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=START, end=END)

    assert payload["canonical_history_migration_complete"] is False
    assert payload["materialized_prehistory_available"] is True
    assert payload["lanes"]["event_driven"]["recovered_source_observations"] == 9
    assert payload["lanes"]["event_driven"]["recovered_evidence_class_count"] == 2


def test_materialized_source_history_never_becomes_candidate_history(monkeypatch):
    materialized = _empty_history()
    materialized["event_driven"] = {
        **_empty_state(),
        "source_count": 99,
        "source_earliest": START,
        "source_latest": BOUNDARY,
        "source_ids": {"coinbase-catalog"},
        "evidence_classes": {"timestamped_events", "event_identity"},
        "source_ledgers": {history.MATERIALIZED_PREHISTORY_LEDGER},
    }
    _install_canonical(monkeypatch, first=BOUNDARY + timedelta(hours=1))
    _install_materialized(monkeypatch, materialized)
    _disable_operating(monkeypatch)
    _assert_raw_http_disabled(monkeypatch)

    row = history.build_durable_lane_history(object(), start=START, end=END)["lanes"][
        "event_driven"
    ]

    assert row["recovered_source_observations"] == 99
    assert "candidate_count" not in row
    assert row["candidate_level_history_synthesized"] is False
    assert row["qualification_authority"] is False
    assert row["allocation_authority"] is False
    assert row["live_execution_authority"] is False


def test_materialized_history_keeps_missing_classes_visible(monkeypatch):
    materialized = _empty_history()
    materialized["liquidity_provision"] = {
        **_empty_state(),
        "source_count": 3,
        "source_earliest": START,
        "source_latest": BOUNDARY,
        "source_ids": {"coinbase-l2"},
        "evidence_classes": {"order_book"},
        "source_ledgers": {history.MATERIALIZED_PREHISTORY_LEDGER},
    }
    _install_canonical(monkeypatch, first=BOUNDARY + timedelta(hours=1))
    _install_materialized(monkeypatch, materialized)
    _disable_operating(monkeypatch)

    row = history.build_durable_lane_history(object(), start=START, end=END)["lanes"][
        "liquidity_provision"
    ]

    assert row["history_available"] is True
    assert row["evidence_class_history_complete"] is False
    assert row["recovered_evidence_class_count"] == 1
    assert row["required_evidence_class_count"] == 2
    assert row["missing_historical_evidence_classes"] == ["trade_flow"]
    assert row["evidence_class_fill_ratio"] == 0.5


def test_materialized_prehistory_reads_persisted_certifier_heartbeat(tmp_path):
    store = EvidenceStore(tmp_path / "history.sqlite3")
    store.record_worker_heartbeat(
        worker_id=REPLAY_WORKER_ID,
        state="degraded",
        observed_at=BOUNDARY + timedelta(minutes=1),
        error_type="HistoricalLaneCoverageIncomplete",
        detail={
            "replay_start": START.isoformat(),
            "replay_boundary": BOUNDARY.isoformat(),
            "live_observatory_started_at": BOUNDARY.isoformat(),
            "lane_coverage": {
                "lanes": {
                    "trend_momentum": {
                        "lane_id": "trend_momentum",
                        "state": "partial",
                        "recovered_source_observations": 17,
                        "earliest_recovered_at": (START + timedelta(hours=2)).isoformat(),
                        "latest_recovered_at": (BOUNDARY - timedelta(minutes=2)).isoformat(),
                        "historical_evidence_classes": ["market_history", "execution_costs"],
                        "source_ids": ["coinbase-market"],
                        "source_ledgers": ["market_history"],
                    }
                }
            },
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        },
    )

    rows, meta = history._read_materialized_prehistory(
        store,
        requested_start=START,
        first_canonical_snapshot=BOUNDARY + timedelta(hours=1),
    )

    row = rows["trend_momentum"]
    assert meta["available"] is True
    assert meta["replay_start"] == START.isoformat()
    assert meta["replay_boundary"] == BOUNDARY.isoformat()
    assert row["source_count"] == 17
    assert row["evidence_classes"] == {"market_history", "execution_costs"}
    assert row["source_ids"] == {"coinbase-market"}
    assert history.MATERIALIZED_PREHISTORY_LEDGER in row["source_ledgers"]


def test_materialized_prehistory_rejects_overlap_with_canonical_history(tmp_path):
    store = EvidenceStore(tmp_path / "overlap.sqlite3")
    store.record_worker_heartbeat(
        worker_id=REPLAY_WORKER_ID,
        state="degraded",
        observed_at=BOUNDARY + timedelta(minutes=1),
        detail={
            "replay_start": START.isoformat(),
            "replay_boundary": BOUNDARY.isoformat(),
            "lane_coverage": {"lanes": {"trend_momentum": {"recovered_source_observations": 5}}},
        },
    )

    rows, meta = history._read_materialized_prehistory(
        store,
        requested_start=START,
        first_canonical_snapshot=BOUNDARY - timedelta(minutes=1),
    )

    assert meta["available"] is False
    assert meta["overlap_rejected"] is True
    assert rows["trend_momentum"]["source_count"] == 0


def test_durable_history_fail_soft_keeps_all_denominators(monkeypatch):
    class FailingLedger:
        def __init__(self, _store):
            raise RuntimeError("simulated canonical history failure")

    def fail(*_args, **_kwargs):
        raise RuntimeError("simulated read failure")

    monkeypatch.setattr(history, "SourceCoverageHistoryLedger", FailingLedger)
    monkeypatch.setattr(history, "_read_materialized_prehistory", fail)
    monkeypatch.setattr(history, "_read_bounded_operating_history", fail)
    _assert_raw_http_disabled(monkeypatch)

    payload = history.build_durable_lane_history(object(), start=START, end=END)

    assert payload["lane_count"] == 13
    assert payload["read_degraded"] is True
    assert {item["stage"] for item in payload["read_errors"]} == {
        "canonical_source_coverage_history",
        "materialized_prehistory",
        "bounded_operating_history",
    }
    for lane_id, definition in LANES.items():
        row = payload["lanes"][lane_id]
        assert row["required_evidence_class_count"] == len(definition["required"])
        assert row["required_evidence_class_count"] > 0
        assert row["recovered_evidence_class_count"] == 0
    assert payload["raw_history_reconstruction_on_http"] is False
    assert payload["historical_counts_as_forward"] is False
    assert payload["qualification_authority"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False


def test_dashboard_history_cache_is_short_bounded():
    assert history.DEFAULT_CACHE_SECONDS == 30.0
