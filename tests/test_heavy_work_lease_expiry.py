from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.heavy_work_lease import HeavyWorkLeaseLedger


NOW = datetime(2026, 8, 20, 19, 30, tzinfo=timezone.utc)


def test_expired_heavy_work_lease_is_recoverable_after_hard_kill(tmp_path):
    store = EvidenceStore(tmp_path / "expiry.sqlite3")
    ledger = HeavyWorkLeaseLedger(store)

    assert ledger.try_acquire("research:dead", ttl_seconds=60, now=NOW) is True
    assert ledger.try_acquire("history:new", now=NOW + timedelta(seconds=30)) is False
    assert ledger.try_acquire("history:new", now=NOW + timedelta(seconds=61)) is True
