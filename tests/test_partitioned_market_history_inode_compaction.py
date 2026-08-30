from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.partitioned_market_history import PartitionedMarketHistory


def _quote(index: int) -> MarketQuote:
    return MarketQuote(
        venue="coinbase",
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        quote_currency="USD",
        contract_key="spot-reference",
        mid=50_000 + index,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=index),
        source=f"inode-compaction-test:{index}",
    )


def _record(index: int):
    quote = _quote(index)
    payload = quote.model_dump_json()
    return index + 1, hashlib.sha256(payload.encode()).hexdigest(), quote


def test_compaction_collapses_fragments_without_changing_inventory(tmp_path):
    history = PartitionedMarketHistory(
        tmp_path / "store",
        max_partition_files_per_group=100,
    )
    for index in range(6):
        assert history.append_records([_record(index)]) == 1

    before = history.inventory(verify_files=True)
    assert before["valid"] is True
    assert before["partition_count"] == 6
    assert before["row_count"] == 6
    assert before["lineage_count"] == 6

    result = history.compact_partition_group("coinbase", "BTC", "2026-01-01")

    assert result["compacted"] is True
    assert result["files_before"] == 6
    assert result["files_after"] == 1
    after = history.inventory(verify_files=True)
    assert after["valid"] is True
    assert after["partition_count"] == 1
    assert after["row_count"] == before["row_count"]
    assert after["lineage_count"] == before["lineage_count"]
    assert after["lineage_digest"] == before["lineage_digest"]
    observed = history.range(
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assert [quote.mid for quote in observed] == [50_000 + index for index in range(6)]


def test_append_keeps_physical_fragment_count_bounded(tmp_path):
    history = PartitionedMarketHistory(
        tmp_path / "store",
        max_partition_files_per_group=2,
    )
    for index in range(9):
        assert history.append_records([_record(index)]) == 1

    inventory = history.inventory(verify_files=True)
    assert inventory["valid"] is True
    assert inventory["row_count"] == 9
    assert inventory["lineage_count"] == 9
    assert inventory["partition_count"] <= 2


def test_bulk_recovery_compacts_redundant_groups(tmp_path):
    history = PartitionedMarketHistory(
        tmp_path / "store",
        max_partition_files_per_group=100,
    )
    for index in range(5):
        history.append_records([_record(index)])

    result = history.compact_redundant_partitions()

    assert result["compacted_groups"] == 1
    assert result["files_collapsed"] == 4
    assert result["rows_rewritten"] == 5
    assert result["target_reached"] is True
    assert history.inventory(verify_files=True)["partition_count"] == 1
