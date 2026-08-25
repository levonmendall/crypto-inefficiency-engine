from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import Boolean, Column, Integer, MetaData, Table, Text, create_engine, insert

from inefficiency_engine.historical_raw_lane_evidence import recover_raw_lane_history


START = datetime(2026, 8, 21, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)


def _store():
    engine = create_engine("sqlite://")
    metadata = MetaData()
    statuses = Table(
        "provider_statuses",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("provider", Text, nullable=False),
        Column("ok", Boolean, nullable=False),
        Column("observed_at", Text, nullable=False),
    )
    admissions = Table(
        "provider_gap_admissions",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("provider", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(engine)
    return SimpleNamespace(engine=engine), statuses, admissions


def _times():
    return START + timedelta(hours=1), END - timedelta(hours=1)


def test_successful_provider_status_history_recovers_catalog_event_classes():
    store, statuses, _ = _store()
    early, late = _times()
    with store.engine.begin() as db:
        db.execute(
            insert(statuses),
            [
                {"provider": "coinbase-exchange:product-catalog", "ok": True, "observed_at": early.isoformat()},
                {"provider": "coinbase-exchange:product-catalog", "ok": True, "observed_at": late.isoformat()},
            ],
        )

    history = recover_raw_lane_history(store, start=START, boundary=END)
    lane = history["event_driven"]
    assert {"timestamped_events", "event_identity"} <= lane["evidence_classes"]
    assert lane["source_earliest"] == early
    assert lane["source_latest"] == late
    assert "coinbase-catalog" in lane["source_ids"]
    assert "provider_statuses" in lane["source_ledgers"]


def test_failed_provider_status_attempts_do_not_become_historical_evidence():
    store, statuses, _ = _store()
    early, late = _times()
    with store.engine.begin() as db:
        db.execute(
            insert(statuses),
            [
                {"provider": "coinbase-exchange:product-catalog", "ok": False, "observed_at": early.isoformat()},
                {"provider": "coinbase-exchange:product-catalog", "ok": False, "observed_at": late.isoformat()},
            ],
        )

    history = recover_raw_lane_history(store, start=START, boundary=END)
    assert history["event_driven"]["source_count"] == 0
    assert history["event_driven"]["evidence_classes"] == set()


def test_only_admitted_provider_gap_history_is_recovered():
    store, _, admissions = _store()
    early, late = _times()

    def payload(at, **overrides):
        data = {
            "provider": "lido:steth-apr-sma",
            "observed_at": at.isoformat(),
            "healthy": True,
            "authoritative": True,
            "commercial_use_permitted": True,
            "point_in_time": True,
        }
        data.update(overrides)
        return json.dumps(data)

    with store.engine.begin() as db:
        db.execute(
            insert(admissions),
            [
                {"provider": "lido:steth-apr-sma", "observed_at": early.isoformat(), "payload_json": payload(early)},
                {"provider": "lido:steth-apr-sma", "observed_at": late.isoformat(), "payload_json": payload(late)},
                {"provider": "lido:steth-apr-sma", "observed_at": late.isoformat(), "payload_json": payload(late, authoritative=False)},
                {"provider": "lido:steth-apr-sma", "observed_at": late.isoformat(), "payload_json": payload(late, healthy=False)},
            ],
        )

    history = recover_raw_lane_history(store, start=START, boundary=END)
    lane = history["yield"]
    assert lane["source_count"] == 2
    assert "yield_rate" in lane["evidence_classes"]
    assert lane["source_earliest"] == early
    assert lane["source_latest"] == late
    assert "provider_gap_admissions" in lane["source_ledgers"]
