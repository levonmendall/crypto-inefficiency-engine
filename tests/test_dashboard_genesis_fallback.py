from __future__ import annotations

import asyncio
from types import SimpleNamespace

from inefficiency_engine import read_api_research_deploy as deploy
from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.canonical_worker import run_canonical_portfolio_loop
from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_projection import DashboardProjectionLedger, PORTFOLIO_WORKER_ID
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger


class _ProjectionProbePortfolio:
    def __init__(self, store: EvidenceStore):
        self.store = store
        self.ledger = CanonicalPaperPortfolioLedger(store)
        self.integrity = PortfolioIntegrityLedger(store)
        self.saw_genesis_projection = False

    async def run_cycle(self):
        # The presentation snapshot must already exist before potentially slow
        # provider-backed portfolio work begins.
        payload = DashboardProjectionLedger(self.store).latest()
        assert payload is not None
        assert payload["portfolio"]["available"] is True
        assert payload["portfolio"]["nav_usd"] == 250_000.0
        assert payload["performance"]["current_nav_usd"] == 250_000.0
        assert payload["performance"]["cash_usd"] == 250_000.0
        assert payload["runtime"]["valuation_status"] == "cash_only"
        self.saw_genesis_projection = True
        return SimpleNamespace(cycle_id="projection-probe")


def test_canonical_worker_publishes_genesis_before_first_cycle(tmp_path):
    store = EvidenceStore(tmp_path / "genesis-projection.db")
    portfolio = _ProjectionProbePortfolio(store)
    service = SimpleNamespace(settings=Settings())

    attempted = asyncio.run(
        run_canonical_portfolio_loop(
            service,
            store,
            portfolio=portfolio,
            stop_event=asyncio.Event(),
            max_cycles=1,
        )
    )

    assert attempted == 1
    assert portfolio.saw_genesis_projection is True


def test_dashboard_reconstructs_durable_cash_portfolio_without_compact_projection(
    monkeypatch,
    tmp_path,
):
    store = EvidenceStore(tmp_path / "durable-fallback.db")
    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()
    snapshot = ledger.current_state()
    ledger.record_snapshot(snapshot)
    PortfolioIntegrityLedger(store).ensure_initial(snapshot)
    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="starting",
        detail={
            "stage": "genesis_ready",
            "portfolio_nav_usd": snapshot.nav_usd,
            "paper_only": True,
        },
    )

    # Deliberately do not construct DashboardProjectionLedger: its compact table is
    # absent, reproducing the production state that previously returned HTTP 503.
    monkeypatch.setattr(deploy._base, "evidence_store", store)

    payload = deploy.dashboard_snapshot()

    assert payload["projection_mode"] == "durable_portfolio_fallback"
    assert payload["presentation_fallback"] is True
    assert payload["portfolio"]["available"] is True
    assert payload["portfolio"]["nav_usd"] == 250_000.0
    assert payload["performance"]["current_nav_usd"] == 250_000.0
    assert payload["performance"]["cash_usd"] == 250_000.0
    assert payload["runtime"]["valuation_status"] == "cash_only"
    assert payload["positions"]["positions"] == []
    assert payload["trades"]["trades"] == []
    assert payload["mechanisms"]["mechanisms"] == []
    assert payload["paper_only"] is True
    assert payload["live_execution_authority"] is False
