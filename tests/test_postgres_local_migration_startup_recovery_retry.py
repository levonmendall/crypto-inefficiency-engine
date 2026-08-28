from __future__ import annotations

import json

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from inefficiency_engine import postgres_local_migration as migration


def _startup_recovery_error() -> OperationalError:
    return OperationalError(
        "SELECT cycle_historical_quotes",
        {},
        RuntimeError(
            "FATAL: the database system is not yet accepting connections "
            "DETAIL: Consistent recovery state has not been yet reached."
        ),
    )


def test_cycle_history_classifier_treats_startup_recovery_as_transient():
    assert migration._is_transient_source_read_error(_startup_recovery_error()) is True


def test_cycle_history_startup_recovery_read_retries_on_fresh_connection(tmp_path, monkeypatch):
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
            raise _startup_recovery_error()
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
