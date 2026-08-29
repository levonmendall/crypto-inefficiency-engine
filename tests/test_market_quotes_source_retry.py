from __future__ import annotations

import json

import pytest
from sqlalchemy.exc import OperationalError

import inefficiency_engine.postgres_local_migration as migration


class _Source:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def _transient_eof(sql: str = "SELECT market_quotes.id") -> OperationalError:
    return OperationalError(
        sql,
        {},
        Exception("consuming input failed: SSL error: unexpected eof while reading"),
    )


def _report(table_report: dict[str, object]) -> dict[str, object]:
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": {"market_quotes": table_report},
        "postgresql_authoritative": True,
        "cutover_ready": False,
    }


def test_market_high_water_transient_eof_retries_inside_child(monkeypatch, tmp_path) -> None:
    source = _Source()
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    attempts = 0

    def capture_high_water(_source, _table):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _transient_eof()
        return [5714625]

    monkeypatch.setattr(migration, "_capture_high_water", capture_high_water)
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)

    assert migration._market_source_high_water_with_retry(
        source,
        object(),
        table_report,
        report,
        progress,
    ) == [5714625]

    assert attempts == 2
    assert source.dispose_calls == 1
    assert table_report["source_transport_retries"] == 1
    assert table_report["last_source_retry_phase"] == "market_high_water_capture"
    assert table_report["last_source_retry_recovered"] is True
    persisted = json.loads(progress.read_text())
    assert persisted["current_table"] == "market_quotes"
    assert persisted["tables"]["market_quotes"]["source_transport_retries"] == 1
    assert persisted["postgresql_authoritative"] is True
    assert persisted["cutover_ready"] is False


def test_market_inventory_retries_whole_finite_inventory_on_fresh_connection(
    monkeypatch,
    tmp_path,
) -> None:
    source = _Source()
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    attempts = 0
    expected = {
        "source_rows": 25,
        "lineage_count": 3,
        "lineage_digest": "digest",
        "min_observed_at": "2026-08-29T00:00:00+00:00",
        "max_observed_at": "2026-08-29T01:00:00+00:00",
        "identities": ["venue|BTC"],
    }

    def source_inventory(_source, _table, *, high_water):
        nonlocal attempts
        attempts += 1
        assert high_water == [5714625]
        if attempts < 3:
            raise _transient_eof("SELECT count(*) FROM market_quotes")
        return expected

    monkeypatch.setattr(migration, "_source_market_inventory", source_inventory)
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)

    assert migration._market_source_inventory_with_retry(
        source,
        object(),
        table_report,
        report,
        progress,
        high_water=[5714625],
    ) == expected

    assert attempts == 3
    assert source.dispose_calls == 2
    assert table_report["source_transport_retries"] == 2
    assert table_report["last_source_retry_phase"] == "market_source_inventory"
    assert table_report["last_source_retry_recovered"] is True


def test_market_batch_retry_replays_same_durable_checkpoint(monkeypatch, tmp_path) -> None:
    source = _Source()
    table_report: dict[str, object] = {"last_primary_key": [4100]}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    observed_checkpoints: list[list[object] | None] = []
    row = object()

    def rows_after_checkpoint(_source, _statement, _primary_key, checkpoint):
        observed_checkpoints.append(checkpoint)
        if len(observed_checkpoints) == 1:
            raise _transient_eof("SELECT market_quotes batch")
        return [row]

    monkeypatch.setattr(migration, "_market_rows_after_checkpoint", rows_after_checkpoint)
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)

    assert migration._market_source_batch_with_retry(
        source,
        object(),
        [object()],
        [4100],
        table_report,
        report,
        progress,
    ) == [row]

    assert observed_checkpoints == [[4100], [4100]]
    assert source.dispose_calls == 1
    assert table_report["last_primary_key"] == [4100]
    assert table_report["last_source_retry_phase"] == "market_copy_batch"
    assert table_report["last_source_retry_recovered"] is True


def test_market_source_nontransient_operational_error_still_fails_closed(
    monkeypatch,
    tmp_path,
) -> None:
    source = _Source()
    table_report: dict[str, object] = {}
    report = _report(table_report)
    progress = tmp_path / "progress.json"
    attempts = 0

    def capture_high_water(_source, _table):
        nonlocal attempts
        attempts += 1
        raise OperationalError(
            "SELECT market_quotes.id",
            {},
            Exception("permission denied for relation market_quotes"),
        )

    monkeypatch.setattr(migration, "_capture_high_water", capture_high_water)
    monkeypatch.setattr(migration.time, "sleep", lambda _delay: None)

    with pytest.raises(OperationalError):
        migration._market_source_high_water_with_retry(
            source,
            object(),
            table_report,
            report,
            progress,
        )

    assert attempts == 1
    assert source.dispose_calls == 0
    assert table_report.get("source_transport_retries") is None
