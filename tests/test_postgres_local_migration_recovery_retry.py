from __future__ import annotations

import json

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from inefficiency_engine import postgres_local_migration as migration


def _recovery_mode_error() -> OperationalError:
    return OperationalError(
        "SELECT cycle_historical_quotes",
        {},
        RuntimeError("FATAL: the database system is in recovery mode"),
    )


def test_cycle_history_classifier_treats_database_recovery_mode_as_transient():
    assert migration._is_transient_source_read_error(_recovery_mode_error()) is True
    non_transient = OperationalError(
        "SELECT cycle_historical_quotes",
        {},
        RuntimeError("permission denied for relation cycle_historical_quotes"),
    )
    assert migration._is_transient_source_read_error(non_transient) is False


def test_cycle_history_recovery_mode_read_retries_on_fresh_connection(tmp_path, monkeypatch):
    source = create_engine(f"sqlite:///{tmp_path / 'source.sqlite3'}")
    progress_path = tmp_path / "progress.json"
    table_report: dict[str, object] = {}
    report: dict[str, object] = {
        "state": "running",
        "tables": {"cycle_historical_quotes": table_report},
    }
    attempts = 0
    dispose_calls = 0
    original_dispose = source.dispose

    def dispose() -> None:
        nonlocal dispose_calls
        dispose_calls += 1
        original_dispose()

    def reader():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _recovery_mode_error()
        return ["recovered"]

    monkeypatch.setattr(source, "dispose", dispose)
    monkeypatch.setattr(migration.time, "sleep", lambda _seconds: None)

    result = migration._source_read_with_retry(
        source,
        reader,
        table_report,
        report,
        progress_path,
        phase="copy_batch",
    )

    assert result == ["recovered"]
    assert attempts == 2
    assert dispose_calls == 1
    assert table_report["source_transport_retries"] == 1
    assert table_report["last_source_retry_phase"] == "copy_batch"
    assert table_report["last_source_retry_recovered"] is True
    persisted = json.loads(progress_path.read_text())
    persisted_table = persisted["tables"]["cycle_historical_quotes"]
    assert persisted_table["source_transport_retries"] == 1
    assert persisted_table["last_source_retry_recovered"] is True
