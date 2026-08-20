from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.dashboard_projection import (
    PORTFOLIO_WORKER_ID,
    DashboardProjectionLedger,
)
from inefficiency_engine.dashboard_resilience import RESILIENT_DASHBOARD_HTML
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger


def test_worker_projection_publishes_complete_cash_genesis_snapshot(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    ledger = CanonicalPaperPortfolioLedger(store)
    ledger.ensure_genesis()
    snapshot = ledger.current_state()
    ledger.record_snapshot(snapshot)
    PortfolioIntegrityLedger(store).ensure_initial(snapshot)
    store.record_worker_heartbeat(
        worker_id=PORTFOLIO_WORKER_ID,
        state="success",
        detail={"paper_only": True},
    )

    projection = DashboardProjectionLedger(store)
    payload = projection.publish(forward_target=30, settled_target=20)

    assert payload["projection_version"] == 1
    assert payload["paper_only"] is True
    assert payload["live_execution_authority"] is False
    assert payload["portfolio"]["available"] is True
    assert payload["portfolio"]["nav_usd"] == 250_000.0
    assert payload["performance"]["current_nav_usd"] == 250_000.0
    assert payload["runtime"]["operational"] is True
    assert payload["runtime"]["valuation_status"] == "cash_only"
    assert payload["positions"] == {"positions": []}
    assert payload["trades"] == {"trades": []}
    assert payload["skips"] == {"skips": []}
    assert payload["mechanisms"]["requirements"]["independent_forward_outcomes"] == 30
    assert projection.latest()["source_portfolio_observed_at"] == snapshot.observed_at.isoformat()


def test_dashboard_uses_one_projection_request_instead_of_endpoint_fanout():
    assert "/v3/dashboard/snapshot" in RESILIENT_DASHBOARD_HTML
    assert "dashboardSnapshot.then" in RESILIENT_DASHBOARD_HTML
    assert "getJSON('/v3/portfolio/canonical')" not in RESILIENT_DASHBOARD_HTML
    assert "safeJSON('/v3/portfolio/positions'" not in RESILIENT_DASHBOARD_HTML
    assert "getJSON('/v3/operations/mechanisms')" not in RESILIENT_DASHBOARD_HTML
