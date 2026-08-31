from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from inefficiency_engine import stage_one_bounded_coarse_market_history as bounded
from inefficiency_engine.models import MarketKind, MarketQuote


def _quote(index: int) -> MarketQuote:
    asset = "BTC" if index % 2 == 0 else "ETH"
    return MarketQuote(
        venue="coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot-reference",
        mid=50_000 + index,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=index),
        source=f"bounded-stage-one-verification:{index}",
    )


def _record(history_id: int, quote: MarketQuote):
    payload = quote.model_dump_json()
    return history_id, hashlib.sha256(payload.encode()).hexdigest(), quote


def test_stage_one_inventory_streams_bounded_columns_without_payload_materialization(
    tmp_path, monkeypatch
) -> None:
    history = bounded.BoundedStageOneCoarsePartitionedMarketHistory(
        tmp_path / "history",
        max_partition_files_per_group=0,
    )
    quotes = [_quote(index) for index in range(6)]
    assert history.append_records(
        [_record(index + 1, quote) for index, quote in enumerate(quotes)]
    ) == 6

    real_parquet_file = bounded.pq.ParquetFile
    calls: list[tuple[int | None, tuple[str, ...]]] = []

    class RecordingParquetFile:
        def __init__(self, *args, **kwargs):
            self._inner = real_parquet_file(*args, **kwargs)

        @property
        def schema_arrow(self):
            return self._inner.schema_arrow

        @property
        def metadata(self):
            return self._inner.metadata

        def iter_batches(self, *args, **kwargs):
            calls.append(
                (
                    kwargs.get("batch_size"),
                    tuple(kwargs.get("columns") or ()),
                )
            )
            yield from self._inner.iter_batches(*args, **kwargs)

    monkeypatch.setattr(bounded, "VERIFICATION_BATCH_ROWS", 2)
    monkeypatch.setattr(bounded.pq, "ParquetFile", RecordingParquetFile)

    inventory = history.inventory(verify_files=True)

    assert inventory["valid"] is True
    assert inventory["row_count"] == 6
    assert inventory["lineage_count"] == 6
    assert inventory["identities"] == ["coinbase|BTC", "coinbase|ETH"]
    assert inventory["stream_times"] == {}
    assert calls
    assert all(batch_size == 2 for batch_size, _columns in calls)
    assert all(columns == bounded.VERIFICATION_COLUMNS for _size, columns in calls)
    assert all("payload_json" not in columns for _size, columns in calls)


def test_stage_one_inventory_fails_closed_on_manifest_physical_membership_mismatch(
    tmp_path,
) -> None:
    history = bounded.BoundedStageOneCoarsePartitionedMarketHistory(
        tmp_path / "history",
        max_partition_files_per_group=0,
    )
    quotes = [_quote(index) for index in range(3)]
    history.append_records(
        [_record(index + 1, quote) for index, quote in enumerate(quotes)]
    )

    with history._connect() as db:
        db.execute(
            "DELETE FROM quote_lineage WHERE lineage_hash = "
            "(SELECT lineage_hash FROM quote_lineage ORDER BY lineage_hash LIMIT 1)"
        )

    inventory = history.inventory(verify_files=True)

    assert inventory["valid"] is False
    assert "lineage_manifest_mismatch" in inventory["errors"]
    assert any(
        str(error).startswith("lineage_partition_row_count_mismatch:")
        for error in inventory["errors"]
    )
