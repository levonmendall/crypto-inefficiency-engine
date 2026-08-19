from datetime import datetime, timedelta, timezone

from inefficiency_engine.canonical_paper_portfolio import CanonicalPaperPortfolioLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.portfolio_integrity import PortfolioIntegrityLedger, PortfolioIntegritySnapshot


def test_integrity_ledger_is_append_only_and_recovers_latest(tmp_path):
    store = EvidenceStore(tmp_path / "integrity.sqlite3")
    account_ledger = CanonicalPaperPortfolioLedger(store)
    account_ledger.ensure_genesis(observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc))
    account = account_ledger.current_state(observed_at=datetime(2026, 8, 19, tzinfo=timezone.utc))
    integrity = PortfolioIntegrityLedger(store)

    initial = integrity.ensure_initial(account)
    assert initial.valuation_status == "cash_only"
    assert initial.cycle_status == "accounting_only"

    later = PortfolioIntegritySnapshot(
        observed_at=account.observed_at + timedelta(minutes=5),
        account_snapshot_at=account.observed_at + timedelta(minutes=5),
        market_evidence_at=account.observed_at + timedelta(minutes=5),
        valuation_status="cash_only",
        cycle_status="degraded",
        allocation_family_failures=[{
            "family": "cex_dex",
            "error_type": "ConnectionError",
            "reason": "CEX↔DEX candidate family failed closed",
        }],
    )
    integrity.record(later)

    rows = integrity.history(limit=10)
    assert len(rows) == 2
    assert rows[0].integrity_id == later.integrity_id
    assert rows[1].integrity_id == initial.integrity_id
    assert integrity.latest() == later


def test_ensure_initial_does_not_duplicate_existing_integrity(tmp_path):
    store = EvidenceStore(tmp_path / "integrity.sqlite3")
    account_ledger = CanonicalPaperPortfolioLedger(store)
    account_ledger.ensure_genesis()
    account = account_ledger.current_state()
    integrity = PortfolioIntegrityLedger(store)

    first = integrity.ensure_initial(account)
    second = integrity.ensure_initial(account)

    assert first.integrity_id == second.integrity_id
    assert len(integrity.history(limit=10)) == 1
