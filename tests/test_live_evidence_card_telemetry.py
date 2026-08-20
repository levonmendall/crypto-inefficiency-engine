from datetime import datetime, timedelta, timezone

from sqlalchemy import insert

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification_api import _live_evidence_overlay


NOW = datetime(2026, 8, 20, 13, 45, tzinfo=timezone.utc)


def _seed_authoritative_rows(store: EvidenceStore) -> None:
    scan_id = store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=NOW - timedelta(seconds=15),
        completed_at=NOW - timedelta(seconds=5),
    )
    common = {
        "scan_id": scan_id,
        "observed_at": (NOW - timedelta(seconds=6)).isoformat(),
        "payload_json": "{}",
        "lineage_hash": "test",
    }
    with store.engine.begin() as db:
        db.execute(insert(store.market_quotes), [
            {**common, "venue": "A", "asset": "BTC"},
            {**common, "venue": "B", "asset": "BTC"},
        ])
        db.execute(insert(store.funding_quotes), [
            {**common, "venue": "B", "asset": "BTC"},
        ])
        db.execute(insert(store.order_books), [
            {**common, "venue": "A", "asset": "BTC", "market_kind": "spot"},
        ])
        db.execute(insert(store.dex_route_quotes), [{
            "record_id": "route-1",
            "cycle_id": "route-cycle-1",
            "phase": "initial",
            "horizon_seconds": "0",
            "route_signature": "ETH:buy",
            "asset": "ETH",
            "direction": "buy_asset",
            "observed_at": (NOW - timedelta(seconds=7)).isoformat(),
            "payload_json": "{}",
            "lineage_hash": "test",
        }])


def test_live_overlay_replaces_na_with_real_worker_persistence_and_durable_counts(tmp_path):
    store = EvidenceStore(tmp_path / "telemetry.sqlite3")
    _seed_authoritative_rows(store)
    store.record_worker_heartbeat(
        worker_id="shadow-research-auxiliary",
        state="success",
        observed_at=NOW - timedelta(seconds=30),
        detail={"alpha_forward_evidence_cycle_id": "alpha-cycle"},
    )
    store.record_worker_heartbeat(
        worker_id="shadow-research-auxiliary",
        state="success",
        observed_at=NOW - timedelta(seconds=10),
        detail={"cycle_attempt": 5},
    )
    store.record_worker_heartbeat(
        worker_id="shadow-research-auxiliary",
        state="running",
        observed_at=NOW,
        detail={"cycle_attempt": 6},
    )

    rows = [
        {"mechanism_id": "price_discrepancy", "authoritative_observation_count": 1},
        {"mechanism_id": "carry", "authoritative_observation_count": 1},
        {"mechanism_id": "trend_momentum", "authoritative_observation_count": 1},
        {"mechanism_id": "liquidity_provision", "authoritative_observation_count": 0},
    ]
    live, telemetry = _live_evidence_overlay(
        store,
        Settings(
            shadow_horizons_seconds=(1.0, 5.0, 15.0, 30.0, 60.0),
            shadow_cycle_interval_seconds=30.0,
            alpha_evidence_every_cycles=10,
            worker_heartbeat_stale_seconds=180.0,
        ),
        rows,
        now=NOW,
    )
    by_id = {row["mechanism_id"]: row for row in live}

    assert telemetry["worker_healthy"] is True
    assert telemetry["persistence_healthy"] is True
    assert telemetry["durable_counts"] == {
        "market": 2,
        "funding": 1,
        "order_book": 1,
        "dex_route": 1,
    }
    assert by_id["price_discrepancy"]["authoritative_observation_count"] == 3
    assert by_id["carry"]["authoritative_observation_count"] == 3
    assert by_id["trend_momentum"]["authoritative_observation_count"] == 2
    assert by_id["liquidity_provision"]["authoritative_observation_count"] == 1
    assert all(row["forward_evidence_worker_healthy"] is True for row in live)
    assert all(row["forward_evidence_persistence_healthy"] is True for row in live)
    assert by_id["price_discrepancy"]["forward_evidence_last_cycle_at"] == (NOW - timedelta(seconds=5)).isoformat()
    assert by_id["trend_momentum"]["forward_evidence_last_cycle_at"] == (NOW - timedelta(seconds=30)).isoformat()
    assert by_id["price_discrepancy"]["authoritative_observation_last_at"] == (NOW - timedelta(seconds=6)).isoformat()


def test_live_overlay_marks_stale_research_worker_unhealthy_without_erasing_persisted_data(tmp_path):
    store = EvidenceStore(tmp_path / "stale.sqlite3")
    _seed_authoritative_rows(store)
    store.record_worker_heartbeat(
        worker_id="shadow-research-auxiliary",
        state="success",
        observed_at=NOW - timedelta(minutes=10),
    )

    live, telemetry = _live_evidence_overlay(
        store,
        Settings(worker_heartbeat_stale_seconds=180.0),
        [{"mechanism_id": "carry", "authoritative_observation_count": 0}],
        now=NOW,
    )

    assert telemetry["worker_healthy"] is False
    assert telemetry["persistence_healthy"] is True
    assert live[0]["authoritative_observation_count"] == 3
    assert live[0]["forward_evidence_worker_healthy"] is False
    assert live[0]["forward_evidence_persistence_healthy"] is True
