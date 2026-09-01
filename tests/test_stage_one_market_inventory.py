from __future__ import annotations

import hashlib

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine, insert
from sqlalchemy.exc import OperationalError

import inefficiency_engine.postgres_local_migration as migration
import inefficiency_engine.stage_one_market_inventory as inventory


def _market_source():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    table = Table(
        "market_quotes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("lineage_hash", Text, nullable=False),
        Column("venue", Text, nullable=False),
        Column("asset", Text, nullable=False),
        Column("observed_at", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(engine)
    with engine.begin() as db:
        db.execute(
            insert(table),
            [
                {
                    "id": 1,
                    "lineage_hash": "lineage-b",
                    "venue": "venue-2",
                    "asset": "ETH",
                    "observed_at": "2026-08-29T02:00:00+00:00",
                    "payload_json": "{}",
                },
                {
                    "id": 2,
                    "lineage_hash": "lineage-a",
                    "venue": "venue-1",
                    "asset": "BTC",
                    "observed_at": "2026-08-29T01:00:00+00:00",
                    "payload_json": "{}",
                },
                {
                    "id": 3,
                    "lineage_hash": "lineage-a",
                    "venue": "venue-1",
                    "asset": "BTC",
                    "observed_at": "2026-08-29T00:00:00+00:00",
                    "payload_json": "{}",
                },
                {
                    "id": 4,
                    "lineage_hash": "lineage-c",
                    "venue": "venue-3",
                    "asset": "SOL",
                    "observed_at": "2026-08-29T03:00:00+00:00",
                    "payload_json": "{}",
                },
            ],
        )
    return engine, table


def _report(table_report: dict[str, object]) -> dict[str, object]:
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": {"market_quotes": table_report},
        "postgresql_authoritative": True,
        "cutover_ready": False,
    }


def _transient_eof() -> OperationalError:
    return OperationalError(
        "SELECT market_quotes inventory batch",
        {},
        Exception("consuming input failed: SSL error: unexpected eof while reading"),
    )


def test_bounded_market_inventory_preserves_exact_equivalence(monkeypatch, tmp_path) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"

    observed = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        report,
        progress,
        high_water=[3],
    )

    digest = hashlib.sha256()
    digest.update(b"lineage-a\n")
    digest.update(b"lineage-b\n")
    assert observed == {
        "source_rows": 3,
        "lineage_count": 2,
        "lineage_digest": digest.hexdigest(),
        "min_observed_at": "2026-08-29T00:00:00+00:00",
        "max_observed_at": "2026-08-29T02:00:00+00:00",
        "identities": ["venue-1|BTC", "venue-2|ETH"],
    }
    assert table_report["source_inventory_mode"] == inventory.MARKET_INVENTORY_MODE
    assert table_report["source_inventory_phase"] == "verified"
    assert table_report["source_inventory_high_water_primary_key"] == [3]
    assert table_report["source_inventory_last_primary_key"] == [3]
    assert table_report["source_inventory_rows_scanned"] == 3
    assert table_report["source_inventory_final_summary_source"] == "exact_recompute"
    assert table_report.get("last_primary_key") is None


def test_bounded_market_inventory_resumes_committed_batch_after_child_failure(
    monkeypatch,
    tmp_path,
) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    original = inventory._read_market_inventory_batch
    calls = 0

    def fail_after_first_batch(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected child failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(inventory, "_read_market_inventory_batch", fail_after_first_batch)
    with pytest.raises(RuntimeError, match="injected child failure"):
        inventory.bounded_market_source_inventory(
            migration,
            source,
            table,
            table_report,
            report,
            progress,
            high_water=[3],
        )

    assert table_report["source_inventory_last_primary_key"] == [2]
    assert table_report["source_inventory_rows_scanned"] == 2
    assert table_report.get("last_primary_key") is None

    observed_checkpoints: list[int | None] = []

    def record_resume(*args, **kwargs):
        observed_checkpoints.append(kwargs["checkpoint"])
        return original(*args, **kwargs)

    monkeypatch.setattr(inventory, "_read_market_inventory_batch", record_resume)
    observed = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        report,
        progress,
        high_water=[3],
    )

    assert observed_checkpoints == [2]
    assert observed["source_rows"] == 3
    assert table_report["source_inventory_last_primary_key"] == [3]
    assert table_report["source_inventory_rows_scanned"] == 3


def test_bounded_market_inventory_transient_eof_replays_only_current_batch(
    monkeypatch,
    tmp_path,
) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    original = inventory._read_market_inventory_batch
    observed_checkpoints: list[int | None] = []
    failed = False
    dispose_calls = 0

    def dispose() -> None:
        nonlocal dispose_calls
        dispose_calls += 1

    def transient_once(*args, **kwargs):
        nonlocal failed
        observed_checkpoints.append(kwargs["checkpoint"])
        if not failed:
            failed = True
            raise _transient_eof()
        return original(*args, **kwargs)

    monkeypatch.setattr(source, "dispose", dispose)
    monkeypatch.setattr(inventory, "_read_market_inventory_batch", transient_once)

    observed = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        report,
        progress,
        high_water=[2],
    )

    assert observed_checkpoints == [None, None]
    assert dispose_calls == 1
    assert observed["source_rows"] == 2
    assert table_report["source_transport_retries"] == 1
    assert table_report["last_source_retry_phase"] == "market_source_inventory_batch"
    assert table_report["last_source_retry_recovered"] is True
    assert table_report["source_inventory_last_primary_key"] == [2]


def test_bounded_market_inventory_new_high_water_resets_only_inventory_accumulator(
    monkeypatch,
    tmp_path,
) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    table_report: dict[str, object] = {"last_primary_key": [900]}
    report = _report(table_report)
    progress = tmp_path / "progress.json"

    first = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        report,
        progress,
        high_water=[2],
    )
    second = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        report,
        progress,
        high_water=[3],
    )

    assert first["source_rows"] == 2
    assert second["source_rows"] == 3
    assert table_report["source_inventory_high_water_primary_key"] == [3]
    assert table_report["source_inventory_rows_scanned"] == 3
    assert table_report["source_inventory_final_summary_source"] == "exact_recompute"
    assert table_report["last_primary_key"] == [900]


def test_bounded_market_inventory_reuses_exact_final_summary_after_child_restart(
    monkeypatch,
    tmp_path,
) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    progress = tmp_path / "progress.json"
    first_report: dict[str, object] = {"last_primary_key": [2996655]}
    first = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        first_report,
        _report(first_report),
        progress,
        high_water=[3],
    )
    assert first_report["source_inventory_final_summary_source"] == "exact_recompute"

    def forbidden(*_args, **_kwargs):
        raise AssertionError("completed inventory must not be rescanned on child restart")

    monkeypatch.setattr(inventory, "_read_market_inventory_batch", forbidden)
    monkeypatch.setattr(inventory, "_finalize_inventory", forbidden)
    second_report: dict[str, object] = {"last_primary_key": [2996655]}
    second = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        second_report,
        _report(second_report),
        progress,
        high_water=[3],
    )

    assert second == first
    assert second_report["source_inventory_final_summary_source"] == "durable_cache"
    assert second_report["source_inventory_final_summary_version"] == (
        inventory.MARKET_INVENTORY_FINAL_SUMMARY_VERSION
    )
    assert second_report["last_primary_key"] == [2996655]


def test_final_summary_cache_is_invalidated_by_new_inventory_progress(monkeypatch, tmp_path) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    progress = tmp_path / "progress.json"
    table_report: dict[str, object] = {}

    inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        table_report,
        _report(table_report),
        progress,
        high_water=[2],
    )
    path = inventory._inventory_path(progress)
    assert inventory._read_finalized_inventory(path, [2]) is not None

    inventory._accumulate_batch(
        path,
        [
            {
                "id": 3,
                "lineage_hash": "lineage-c",
                "venue": "venue-3",
                "asset": "SOL",
                "observed_at": "2026-08-29T03:00:00+00:00",
            }
        ],
        previous_source_rows=2,
    )

    assert inventory._read_finalized_inventory(path, [2]) is None


def test_corrupt_final_summary_falls_back_to_exact_recompute(monkeypatch, tmp_path) -> None:
    source, table = _market_source()
    monkeypatch.setattr(inventory, "MARKET_INVENTORY_BATCH_SIZE", 2)
    progress = tmp_path / "progress.json"
    first_report: dict[str, object] = {}
    expected = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        first_report,
        _report(first_report),
        progress,
        high_water=[3],
    )
    path = inventory._inventory_path(progress)
    with inventory.closing(inventory._connect_inventory(path)) as db:
        inventory._meta_set(db, "final_summary_payload", "{not-json")

    original_finalize = inventory._finalize_inventory
    finalize_calls = 0

    def record_finalize(*args, **kwargs):
        nonlocal finalize_calls
        finalize_calls += 1
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(inventory, "_finalize_inventory", record_finalize)
    second_report: dict[str, object] = {}
    observed = inventory.bounded_market_source_inventory(
        migration,
        source,
        table,
        second_report,
        _report(second_report),
        progress,
        high_water=[3],
    )

    assert observed == expected
    assert finalize_calls == 1
    assert second_report["source_inventory_final_summary_source"] == "exact_recompute"
