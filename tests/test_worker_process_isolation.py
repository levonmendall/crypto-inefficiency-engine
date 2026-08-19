from datetime import datetime, timedelta, timezone

from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_worker import PORTFOLIO_WORKER_ID
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger, PortfolioIntegritySnapshot
from inefficiency_engine.worker_supervisor import (
    default_worker_child_commands,
    record_portfolio_watchdog_fallback,
)


def test_worker_supervisor_uses_distinct_research_and_portfolio_process_commands():
    commands = default_worker_child_commands()

    assert set(commands) == {"research", "portfolio"}
    assert commands["research"] != commands["portfolio"]
    assert commands["research"][-1] == "research-worker"
    assert commands["portfolio"][-1] == "portfolio-worker"
    assert commands["research"][:-1] == commands["portfolio"][:-1]


def test_portfolio_watchdog_records_fresh_fail_closed_account_snapshot(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    ledger = CanonicalPaperPortfolioLedger(store)
    integrity = PortfolioIntegrityLedger(store)
    old_time = datetime.now(timezone.utc) - timedelta(minutes=30)

    ledger.ensure_genesis(observed_at=old_time)
    old_snapshot = ledger.current_state(observed_at=old_time)
    ledger.record_snapshot(old_snapshot)
    integrity.record(PortfolioIntegritySnapshot(
        observed_at=old_time,
        account_snapshot_at=old_time,
        market_evidence_at=old_time,
        valuation_status="cash_only",
        cycle_status="degraded",
        allocation_family_failures=[{
            "family": "cex_dex",
            "error_type": "RuntimeError",
            "reason": "prior provider degradation",
        }],
    ))

    record_portfolio_watchdog_fallback(store)

    latest = ledger.latest_snapshot()
    assert latest is not None
    assert latest.observed_at > old_snapshot.observed_at
    assert latest.nav_usd == 250000.0
    assert latest.cash_usd == 250000.0
    assert latest.open_position_count == 0

    latest_integrity = integrity.latest()
    assert latest_integrity is not None
    assert latest_integrity.account_snapshot_at == latest.observed_at
    assert latest_integrity.valuation_status == "cash_only"
    assert latest_integrity.cycle_status == "failed"
    assert latest_integrity.fallback_snapshot is True
    assert latest_integrity.cycle_error_type == "PortfolioProcessWatchdogTimeout"
    assert latest_integrity.market_evidence_at == old_time
    assert latest_integrity.allocation_family_failures[0]["family"] == "cex_dex"

    heartbeat = store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
    assert heartbeat is not None
    assert heartbeat.state == "error"
    assert heartbeat.error_type == "PortfolioProcessWatchdogTimeout"
    assert heartbeat.detail["supervisor_fallback_recorded"] is True
