from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.operating_worker import PORTFOLIO_WORKER_ID


@pytest.mark.asyncio
async def test_canonical_loop_completes_without_any_certification_dependency():
    heartbeats: list[dict[str, object]] = []

    class FakeStore:
        def record_worker_heartbeat(self, **payload):
            heartbeats.append(payload)

    class FakeLedger:
        def __init__(self):
            self.snapshot = SimpleNamespace(
                observed_at=SimpleNamespace(isoformat=lambda: "2026-08-19T23:00:00+00:00"),
                nav_usd=250000.0,
            )

        def ensure_genesis(self):
            return None

        def latest_snapshot(self):
            return self.snapshot

        def current_state(self, observed_at=None):
            return self.snapshot

        def record_snapshot(self, snapshot):
            self.snapshot = snapshot

    class FakeIntegrity:
        def ensure_initial(self, snapshot):
            return None

        def latest(self):
            return SimpleNamespace(
                cycle_status="success",
                cycle_error_type=None,
                allocation_family_failures=[],
                stale_position_count=0,
                market_evidence_at=SimpleNamespace(
                    isoformat=lambda: "2026-08-19T23:00:00+00:00"
                ),
                valuation_status="cash_only",
            )

    class FakePortfolio:
        def __init__(self):
            self.ledger = FakeLedger()
            self.integrity = FakeIntegrity()

        async def run_cycle(self):
            return SimpleNamespace(cycle_id="portfolio-cycle")

    stop = asyncio.Event()
    service = SimpleNamespace(settings=SimpleNamespace(shadow_cycle_interval_seconds=30.0))
    attempted = await run_canonical_portfolio_loop(
        service,  # type: ignore[arg-type]
        FakeStore(),  # type: ignore[arg-type]
        portfolio=FakePortfolio(),  # type: ignore[arg-type]
        stop_event=stop,
        interval_seconds=60.0,
        max_cycles=1,
    )

    assert attempted == 1
    portfolio_rows = [row for row in heartbeats if row["worker_id"] == PORTFOLIO_WORKER_ID]
    assert portfolio_rows[0]["state"] == "running"
    assert portfolio_rows[1]["state"] == "success"
    assert portfolio_rows[1]["detail"]["certification_decoupled"] is True
    assert portfolio_rows[1]["detail"]["portfolio_nav_usd"] == pytest.approx(250000.0)
