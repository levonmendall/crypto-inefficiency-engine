from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from inefficiency_engine.coarse_partitioned_market_history import (
    MULTI_ASSET_PARTITION,
    CoarsePartitionedMarketHistory,
)
from inefficiency_engine.market_history_inode_recovery import (
    MARKET_HISTORY_LAYOUT,
    market_history_rebuild_required,
    prepare_unverified_market_history_rebuild,
)
from inefficiency_engine.models import MarketKind, MarketQuote


def _quote(asset: str, hour: int) -> MarketQuote:
    return MarketQuote(
        venue="coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot-reference",
        mid=50_000 + hour,
        observed_at=datetime(2026, 1, 1, hour, tzinfo=timezone.utc),
        source=f"coarse-layout-test:{asset}:{hour}",
    )


def _record(history_id: int, quote: MarketQuote):
    payload = quote.model_dump_json()
    return history_id, hashlib.sha256(payload.encode()).hexdigest(), quote


def test_coarse_layout_groups_multiple_assets_by_venue_and_day(tmp_path: Path) -> None:
    history = CoarsePartitionedMarketHistory(
        tmp_path / "history",
        max_partition_files_per_group=0,
    )
    btc = _quote("BTC", 1)
    eth = _quote("ETH", 2)

    assert history.append_records([_record(11, btc), _record(12, eth)]) == 2

    with history._connect() as db:
        partitions = list(db.execute("SELECT venue, asset, day, row_count FROM partitions"))
    assert partitions == [("coinbase", MULTI_ASSET_PARTITION, "2026-01-01", 2)]

    inventory = history.inventory(verify_files=True)
    assert inventory["valid"] is True
    assert inventory["row_count"] == 2
    assert inventory["lineage_count"] == 2
    assert inventory["identities"] == ["coinbase|BTC", "coinbase|ETH"]

    btc_only = history.range(
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        assets=["BTC"],
    )
    assert [quote.asset for quote in btc_only] == ["BTC"]

    readiness = history.readiness(required_identities=[("coinbase", "BTC"), ("coinbase", "ETH")])
    assert readiness["ready"] is True


def _progress() -> dict[str, object]:
    tables = {
        f"verified_{index}": {"verified": True, "sentinel": index}
        for index in range(55)
    }
    tables["market_quotes"] = {
        "verified": None,
        "migration_mode": "captured_primary_key_high_water",
        "source_rows": 2_794_738,
        "source_lineage_count": 2_787_792,
        "high_water_primary_key": [2_812_933],
        "last_primary_key": [1_748_641],
        "destination_inventory": {"lineage_count": 1_700_000, "valid": False},
        "row_digest": "partial",
    }
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": tables,
    }


def test_targeted_rebuild_preserves_55_verified_tables_and_source_high_water(
    tmp_path: Path,
) -> None:
    progress_path = tmp_path / "migration" / "postgres-import-progress.json"
    progress_path.parent.mkdir(parents=True)
    before = _progress()
    progress_path.write_text(json.dumps(before))
    root = tmp_path / "market-history"
    old_partition = root / "venue=x" / "asset=one-row" / "date=2026-01-01"
    old_partition.mkdir(parents=True)
    (old_partition / "part-old.parquet").write_bytes(b"old unverified target")

    result = prepare_unverified_market_history_rebuild(
        progress_path,
        market_history_root=root,
    )

    assert result is not None
    assert result["state"] == "complete"
    assert result["preserved_high_water_primary_key"] == [2_812_933]
    assert result["previous_last_primary_key"] == [1_748_641]
    assert root.is_dir()
    assert list(root.iterdir()) == []

    after = json.loads(progress_path.read_text())
    market = after["tables"]["market_quotes"]
    assert market["high_water_primary_key"] == [2_812_933]
    assert market["source_rows"] == 2_794_738
    assert market["source_lineage_count"] == 2_787_792
    assert market["local_history_layout"] == MARKET_HISTORY_LAYOUT
    assert market["market_history_rebuild_pending"] is False
    assert market["verified"] is False
    assert "last_primary_key" not in market
    assert "destination_inventory" not in market
    assert "row_digest" not in market
    assert after["current_table"] == "market_quotes"
    assert after["state"] == "running"
    for index in range(55):
        assert after["tables"][f"verified_{index}"] == before["tables"][f"verified_{index}"]


def test_pending_rebuild_is_idempotent_after_partial_delete(tmp_path: Path) -> None:
    progress = _progress()
    market = progress["tables"]["market_quotes"]
    market["local_history_layout"] = MARKET_HISTORY_LAYOUT
    market["market_history_rebuild_pending"] = True
    market.pop("last_primary_key")
    progress_path = tmp_path / "migration" / "postgres-import-progress.json"
    progress_path.parent.mkdir(parents=True)
    progress_path.write_text(json.dumps(progress))
    root = tmp_path / "market-history"
    root.mkdir()
    (root / "partial.parquet").write_bytes(b"partial")

    assert market_history_rebuild_required(progress) is True
    result = prepare_unverified_market_history_rebuild(
        progress_path,
        market_history_root=root,
    )

    assert result is not None
    after = json.loads(progress_path.read_text())
    market_after = after["tables"]["market_quotes"]
    assert market_after["market_history_rebuild_pending"] is False
    assert market_after["high_water_primary_key"] == [2_812_933]
    assert list(root.iterdir()) == []


def test_verified_market_history_is_never_selected_for_targeted_rebuild() -> None:
    progress = _progress()
    progress["tables"]["market_quotes"]["verified"] = True
    assert market_history_rebuild_required(progress) is False
