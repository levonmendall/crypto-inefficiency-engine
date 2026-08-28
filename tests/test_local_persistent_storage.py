from __future__ import annotations

import multiprocessing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from inefficiency_engine.evidence import EvidenceStore, evidence_location_from_env
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


def _quote(index: int, observed_at: datetime) -> MarketQuote:
    return MarketQuote(
        venue="coinbase", asset="BTC", market_kind=MarketKind.SPOT,
        symbol="BTC-USD", quote_currency="USD", contract_key="spot-reference",
        mid=50_000 + index, observed_at=observed_at, source=f"test:{index}",
    )


def _append_worker(root: str, start: int) -> None:
    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    PartitionedMarketHistory(root).append(_quote(i, now + timedelta(seconds=i)) for i in range(start, start + 20))


def test_sqlite_wal_is_canonical_when_storage_root_is_configured(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("CIE_DATABASE_URL", raising=False)
    monkeypatch.setenv("CIE_STORAGE_ROOT", str(tmp_path))
    location = evidence_location_from_env()
    assert location == tmp_path / "metadata" / "cie.sqlite3"
    store = EvidenceStore(location)
    with store.engine.connect() as db:
        assert db.execute(text("PRAGMA journal_mode")).scalar_one().lower() == "wal"
        assert int(db.execute(text("PRAGMA synchronous")).scalar_one()) == 2


def test_atomic_partition_write_deduplicates_and_ignores_temporary_files(tmp_path):
    history = PartitionedMarketHistory(tmp_path)
    observed = datetime(2026, 1, 2, tzinfo=timezone.utc)
    quote = _quote(1, observed)
    assert history.append([quote, quote]) == 1
    assert history.append([quote]) == 0
    incomplete = history.root / "venue=x" / "asset=x" / "date=2026-01-02" / ".part.parquet.tmp"
    incomplete.parent.mkdir(parents=True)
    incomplete.write_bytes(b"interrupted")
    assert history.range(start=observed - timedelta(seconds=1), end=observed + timedelta(seconds=1)) == [quote]


def test_multi_process_partition_writes_are_serialized_and_restart_safe(tmp_path):
    processes = [multiprocessing.Process(target=_append_worker, args=(str(tmp_path), start)) for start in (0, 10, 20)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    history = PartitionedMarketHistory(tmp_path)
    start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    rows = history.range(start=start, end=start + timedelta(minutes=1))
    assert len(rows) == 40
    assert [row.mid for row in rows] == sorted(row.mid for row in rows)
    assert PartitionedMarketHistory(tmp_path).readiness()["row_count"] == 40


def test_partition_readiness_is_fail_closed_for_empty_storage(tmp_path):
    status = PartitionedMarketHistory(tmp_path).readiness(
        required_start=datetime.now(timezone.utc) - timedelta(days=180),
        required_end=datetime.now(timezone.utc),
    )
    assert status["ready"] is False
    assert status["reason"] == "partition_coverage_incomplete"
    assert status["live_execution_authority"] is False
