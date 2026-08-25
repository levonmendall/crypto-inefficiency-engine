from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine import dashboard_source_connectivity as connectivity


SOURCE_SPEC = {
    "id": "public-trade-flow",
    "name": "Public trade-flow capture",
    "lanes": ["microstructure"],
    "classes": ["trade_flow"],
    "group": "coinbase-public",
    "tier": "public",
    "authoritative": True,
    "active": True,
}


def _direct_row(*, observed_at: datetime, healthy: bool, error_type: str | None = None):
    return {
        "lane_id": "microstructure",
        "source_id": "public-trade-flow",
        "observed_at": observed_at.isoformat(),
        "healthy": healthy,
        "item_count": 100 if healthy else 0,
        "evidence_classes": ["trade_flow"],
        "authoritative": True,
        "commercial_use_permitted": True,
        "point_in_time": True,
        "error_type": error_type,
        "source_reference": "https://api.exchange.coinbase.com/products/{product_id}/trades",
    }


def test_latest_failed_refresh_does_not_erase_still_fresh_success():
    now = datetime(2026, 8, 25, 15, 30, tzinfo=timezone.utc)
    row = connectivity._source_row(
        SOURCE_SPEC,
        current=now,
        direct=[
            _direct_row(
                observed_at=now - timedelta(seconds=10),
                healthy=False,
                error_type="TimeoutError",
            ),
            _direct_row(observed_at=now - timedelta(seconds=60), healthy=True),
        ],
        providers=[],
        admissions=[],
        table_candidates={},
        fallback_seconds=3600.0,
    )

    assert row["state"] == "healthy"
    assert row["admitted"] is True
    assert row["using_prior_fresh_evidence"] is True
    assert row["refresh_degraded"] is True
    assert row["latest_attempt_state"] == "failed"
    assert row["latest_attempt_error_type"] == "TimeoutError"
    assert row["error_type"] is None


def test_failed_refresh_becomes_failure_after_prior_evidence_ttl_expires():
    now = datetime(2026, 8, 25, 15, 30, tzinfo=timezone.utc)
    row = connectivity._source_row(
        SOURCE_SPEC,
        current=now,
        direct=[
            _direct_row(
                observed_at=now - timedelta(seconds=10),
                healthy=False,
                error_type="TimeoutError",
            ),
            _direct_row(observed_at=now - timedelta(minutes=10), healthy=True),
        ],
        providers=[],
        admissions=[],
        table_candidates={},
        fallback_seconds=3600.0,
    )

    assert row["state"] == "failed"
    assert row["admitted"] is False
    assert row["using_prior_fresh_evidence"] is False
    assert row["latest_attempt_error_type"] == "TimeoutError"


def test_diagnostic_read_failure_serves_last_success_and_ages_it_fail_closed(monkeypatch):
    now = datetime(2026, 8, 25, 15, 30, tzinfo=timezone.utc)
    store = object()
    connectivity._LAST_SUCCESSFUL_BY_STORE.clear()
    monkeypatch.setattr(connectivity, "SOURCES", (SOURCE_SPEC,))

    def successful_read(_store):
        return (
            {"source_coverage_observations"},
            [_direct_row(observed_at=now - timedelta(seconds=30), healthy=True)],
            [],
            [],
            {},
        )

    monkeypatch.setattr(connectivity, "_read_source_input_history", successful_read)
    first = connectivity.read_source_connectivity(store, now=now)
    assert first["available"] is True
    assert first["summary"]["healthy"] == 1

    def failed_read(_store):
        raise RuntimeError("temporary database read failure")

    monkeypatch.setattr(connectivity, "_read_source_input_history", failed_read)
    retained = connectivity.read_source_connectivity(
        store,
        now=now + timedelta(seconds=30),
    )
    assert retained["available"] is False
    assert retained["served_last_successful_snapshot"] is True
    assert retained["summary"]["healthy"] == 1
    assert retained["sources"][0]["state"] == "healthy"

    expired = connectivity.read_source_connectivity(
        store,
        now=now + timedelta(minutes=10),
    )
    assert expired["available"] is False
    assert expired["served_last_successful_snapshot"] is True
    assert expired["summary"]["healthy"] == 0
    assert expired["summary"]["stale"] == 1
    assert expired["sources"][0]["cache_expired_during_read_failure"] is True


def test_mobile_dashboard_uses_permanent_keyed_source_cards():
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    script = mobile._STABLE_SOURCE_CONNECTIVITY_JS
    assert "const sourceCardNodes=new Map()" in script
    assert "sourceCardNodes.has(sourceId)" in script
    assert "host.appendChild(nodes.card)" in script
    assert "for(const row of rows)patchSourceCard(row)" in script
    assert "attention=rows.filter" not in script
    assert "sourceProblems').innerHTML" not in script
    assert "Latest refresh warning" in script
    assert "prior evidence remains valid" in script


def test_mobile_dashboard_retains_board_and_serializes_source_polling():
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    script = mobile._STABLE_SOURCE_CONNECTIVITY_JS
    assert "lastGoodSourceConnectivity" in script
    assert "retaining existing source board" in script
    assert "if(sourceConnectivityInFlight)return sourceConnectivityInFlight" in script
    assert "sourceConnectivityRequestSequence" in script
    assert "sourceConnectivityAppliedSequence" in script


def test_mobile_dashboard_staggers_reads_and_never_refetches_on_resize():
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    boot = mobile._STAGGERED_BOOT_JS
    assert "window.addEventListener('resize',()=>renderChart(window.__history||[]))" in boot
    assert "window.addEventListener('resize',()=>refresh())" not in boot
    assert "setTimeout(()=>refreshSourceConnectivity(),5000)" in boot
    assert "document.visibilityState==='visible'" in boot
    assert "refresh().finally(()=>refreshSourceConnectivity())" in boot


def test_repaired_dashboard_embeds_stable_source_board_contract():
    from inefficiency_engine import read_api_mobile_truth_deploy as mobile

    html = mobile.repaired_dashboard_html()
    assert "sourceCardNodes=new Map()" in html
    assert "source-board" in html
    assert "Latest refresh warning" in html
    assert "setTimeout(()=>refreshSourceConnectivity(),5000)" in html
