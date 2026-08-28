from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.evidence import EvidenceStore, ProviderStatus
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory
from inefficiency_engine.postgres_local_migration import migrate_engines


def _quote(index: int) -> MarketQuote:
    return MarketQuote(
        venue="coinbase", asset="BTC", market_kind=MarketKind.SPOT,
        symbol="BTC-USD", quote_currency="USD", contract_key="spot-reference",
        mid=50_000 + index,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
        source=f"migration-test:{index}",
    )


def _stores(tmp_path):
    source = EvidenceStore(tmp_path / "source.sqlite3")
    target = EvidenceStore(tmp_path / "target.sqlite3")
    for index in range(3):
        observed = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index)
        source.record_scan(
            scan_id=f"scan-{index}", started_at=observed, completed_at=observed,
            providers=[ProviderStatus(provider="test", ok=True, observed_at=observed)],
            funding_quotes=[], market_quotes=[_quote(index)], opportunities=[],
        )
    duplicate_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    source.record_scan(
        scan_id="scan-duplicate", started_at=duplicate_at, completed_at=duplicate_at,
        providers=[], funding_quotes=[], market_quotes=[_quote(2)], opportunities=[],
    )
    return source, target, PartitionedMarketHistory(tmp_path / "history")


def test_migration_is_idempotent_and_proves_market_lineage_equivalence(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    first = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    second = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    assert first["state"] == second["state"] == "verified"
    market = second["tables"]["market_quotes"]
    assert market["source_rows"] == 4
    assert market["source_lineage_count"] == 3
    assert market["destination_inventory"]["lineage_count"] == 3
    assert market["destination_inventory"]["valid"] is True
    assert second["forward_evidence_granted"] is False


def test_interrupted_migration_resumes_from_durable_primary_key_checkpoint(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    with pytest.raises(InterruptedError):
        migrate_engines(
            source.engine, target, history, progress_path=progress,
            batch_size=1, interrupt_after_batches=1,
        )
    interrupted = json.loads(progress.read_text())
    checkpoints = [value.get("last_primary_key") for value in interrupted["tables"].values()]
    assert any(checkpoint for checkpoint in checkpoints)
    result = migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    assert result["state"] == "verified"
    assert result["resumed_at"] is not None
    assert result["tables"]["market_quotes"]["last_primary_key"] == [4]


def test_migration_fails_closed_when_a_committed_partition_is_missing(tmp_path):
    source, target, history = _stores(tmp_path)
    progress = tmp_path / "progress.json"
    migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    with history._connect() as db:
        relative = db.execute("SELECT path FROM partitions ORDER BY path LIMIT 1").fetchone()[0]
    (history.root / relative).unlink()
    with pytest.raises(RuntimeError, match="market_quotes equivalence mismatch"):
        migrate_engines(source.engine, target, history, progress_path=progress, batch_size=1)
    failed = json.loads(progress.read_text())
    assert failed["state"] == "failed"
    assert failed["forward_evidence_granted"] is False
