from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, text

from inefficiency_engine.bounded_capital_location import (
    MemoryBoundedCapitalLocationResearchService,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.research_closure_worker import (
    RESEARCH_CLOSURE_WORKER_ID,
    run_research_closure_cycle,
)


class _NoOperatingSnapshotLedger:
    def latest(self):
        return None


class _NoOperatingSnapshot:
    ledger = _NoOperatingSnapshotLedger()


def _store_with_scan(tmp_path):
    store = EvidenceStore(tmp_path / "closure-runtime.sqlite3")
    now = datetime.now(timezone.utc)
    store.record_scan(
        funding_quotes=[],
        market_quotes=[],
        opportunities=[],
        providers=[],
        order_books=[],
        executability=[],
        started_at=now,
        completed_at=now,
        scan_id="closure-runtime-scan",
    )
    return store, now


@pytest.mark.asyncio
async def test_production_closure_cycle_persists_first_summary(tmp_path):
    store, _ = _store_with_scan(tmp_path)
    service = SimpleNamespace(settings=Settings())

    summary = await run_research_closure_cycle(
        service=service,
        store=store,
        alpha_factory=object(),
        operating_certification=_NoOperatingSnapshot(),
        total_capital_usd=250_000.0,
    )

    assert summary is not None
    assert summary.source_scan_id == "closure-runtime-scan"
    assert summary.diagnostic_errors == {}
    assert set(summary.rejection_funnels) == {
        "price_discrepancy",
        "carry",
        "microstructure",
    }
    with store.engine.connect() as db:
        count = db.execute(
            text("SELECT COUNT(*) FROM research_closure_cycle_summaries")
        ).scalar_one()
    assert count == 1
    heartbeat = store.latest_worker_heartbeat(RESEARCH_CLOSURE_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "success"
    assert heartbeat.detail["summary_recorded"] is True


@pytest.mark.asyncio
async def test_substage_failure_is_visible_but_does_not_suppress_summary(tmp_path, monkeypatch):
    store, _ = _store_with_scan(tmp_path)
    service = SimpleNamespace(settings=Settings())

    def fail_location(*args, **kwargs):
        raise RuntimeError("legacy history cannot be parsed")

    monkeypatch.setattr(
        MemoryBoundedCapitalLocationResearchService,
        "plan",
        fail_location,
    )

    summary = await run_research_closure_cycle(
        service=service,
        store=store,
        alpha_factory=object(),
        operating_certification=_NoOperatingSnapshot(),
        total_capital_usd=250_000.0,
    )

    assert summary is not None
    assert summary.diagnostic_errors == {"capital_location_forward": "RuntimeError"}
    assert summary.capital_location_forward == {}
    assert set(summary.rejection_funnels) == {
        "price_discrepancy",
        "carry",
        "microstructure",
    }
    heartbeat = store.latest_worker_heartbeat(RESEARCH_CLOSURE_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "degraded"
    assert heartbeat.error_type == "RuntimeError"
    assert heartbeat.detail["summary_recorded"] is True


def test_capital_location_history_is_bounded_and_legacy_payloads_fail_soft(tmp_path):
    store, now = _store_with_scan(tmp_path)
    with store.engine.begin() as db:
        db.execute(
            insert(store.opportunities),
            {
                "scan_id": "closure-runtime-scan",
                "opportunity_id": "legacy-incompatible",
                "strategy": "cex_spot_dislocation",
                "asset": "BTC",
                "observed_at": now.isoformat(),
                "payload_json": "{}",
                "lineage_hash": "legacy",
            },
        )

    service = MemoryBoundedCapitalLocationResearchService(
        store,
        history_hours=72.0,
        max_history_records=100,
    )
    plan = service.plan(reserve_capital_usd=250_000.0, now=now)

    assert plan.historical_opportunity_count == 0
    assert plan.recommendations == []
    assert any("incompatible legacy opportunity payloads" in row for row in plan.blockers)
