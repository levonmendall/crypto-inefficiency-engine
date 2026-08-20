from datetime import datetime, timezone

from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.dashboard_projection import (
    PORTFOLIO_WORKER_ID,
    RESEARCH_WORKER_ID,
    DashboardProjectionLedger,
    ResearchDashboardProjectionLedger,
)
from inefficiency_engine.dashboard_resilience import RESILIENT_DASHBOARD_HTML
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification import (
    MechanismOperatingStatus,
    OperatingCertificationLedger,
    OperatingCertificationSnapshot,
)
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
    persisted = projection.latest()["source_portfolio_observed_at"]
    assert persisted.replace("Z", "+00:00") == snapshot.observed_at.isoformat()


def test_research_projection_refreshes_cards_from_research_heartbeat(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    status = MechanismOperatingStatus(
        mechanism_id="price_discrepancy",
        name="Price discrepancy / arbitrage",
        state="collecting",
        stage="profitability_certifiable",
        provider_ready=True,
        authoritative_observation_count=856,
        primary_reason="collecting independent forward evidence",
        next_action="keep allocation certification running",
    )
    OperatingCertificationLedger(store).record(OperatingCertificationSnapshot(
        observed_at=datetime.now(timezone.utc),
        version="test",
        public_market_provider_healthy=True,
        public_market_surface_count=1,
        public_market_surface_ok_count=1,
        public_order_book_probe_count=0,
        public_order_book_probe_ok_count=0,
        market_quote_count=856,
        funding_quote_count=0,
        mechanism_count=1,
        provider_gap_count=0,
        collecting_count=1,
        poor_economics_count=0,
        blocked_count=0,
        certifying_count=0,
        certified_count=0,
        mechanisms=[status],
    ))
    store.record_worker_heartbeat(
        worker_id=RESEARCH_WORKER_ID,
        state="success",
        detail={"cycle_attempt": 12, "observation_count": 25, "paper_only": True},
    )

    projection = ResearchDashboardProjectionLedger(store)
    payload = projection.publish(
        forward_target=30,
        settled_target=20,
        shadow_horizons_seconds=(60.0,),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=10,
        heartbeat_stale_seconds=180.0,
    )

    assert payload["projection_version"] == 2
    assert payload["projection_kind"] == "research"
    assert payload["source_research_heartbeat_at"] is not None
    row = payload["mechanisms"]["mechanisms"][0]
    assert row["authoritative_observation_count"] == 856
    assert row["forward_evidence_worker_healthy"] is True
    assert row["forward_evidence_last_cycle_at"] is not None
    assert row["research_projection_observed_at"] == payload["observed_at"]
    assert payload["mechanisms"]["observed_at"] == payload["observed_at"]
    assert projection.latest()["observed_at"] == payload["observed_at"]


def test_dashboard_uses_one_projection_request_instead_of_endpoint_fanout():
    assert "/v3/dashboard/snapshot" in RESILIENT_DASHBOARD_HTML
    assert "dashboardSnapshot.then" in RESILIENT_DASHBOARD_HTML
    assert "getJSON('/v3/portfolio/canonical')" not in RESILIENT_DASHBOARD_HTML
    assert "safeJSON('/v3/portfolio/positions'" not in RESILIENT_DASHBOARD_HTML
    assert "getJSON('/v3/operations/mechanisms')" not in RESILIENT_DASHBOARD_HTML
