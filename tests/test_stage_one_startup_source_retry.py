from __future__ import annotations

import json

import pytest
from sqlalchemy.exc import OperationalError

from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine import stage_one_local_persistence_migration_coarse as coarse


def _recovery_error() -> OperationalError:
    return OperationalError(
        "connect",
        {},
        RuntimeError("FATAL: the database system is in recovery mode"),
    )


def _non_transient_error() -> OperationalError:
    return OperationalError("connect", {}, RuntimeError("password authentication failed"))


def _seed_running_progress(path) -> None:
    path.write_text(
        json.dumps(
            {
                "state": "running",
                "current_table": "market_quotes",
                "tables": {
                    "market_quotes": {
                        "last_primary_key": [2996655],
                        "high_water_primary_key": [3094848],
                        "source_transport_retries": 4,
                        "verified": False,
                    }
                },
            }
        )
    )


def test_startup_recovery_mode_retries_with_existing_bounded_delays(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    _seed_running_progress(progress_path)
    monkeypatch.setattr(migration, "_progress_path", lambda: progress_path)

    calls = 0

    def fake_main() -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _recovery_error()
        return 0

    sleeps: list[float] = []
    monkeypatch.setattr(migration, "main", fake_main)
    monkeypatch.setattr(coarse.time, "sleep", sleeps.append)

    assert coarse._run_with_bounded_startup_source_retry() == 0
    assert calls == 3
    assert sleeps == list(migration.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS[:2])

    report = json.loads(progress_path.read_text())
    market = report["tables"]["market_quotes"]
    assert market["last_primary_key"] == [2996655]
    assert market["high_water_primary_key"] == [3094848]
    assert market["source_transport_retries"] == 6
    assert market["last_source_retry_phase"] == "stage_one_source_metadata_reflection"
    assert market["last_source_retry_recovered"] is True


def test_startup_recovery_mode_exhausts_without_expanding_retry_ceiling(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    _seed_running_progress(progress_path)
    monkeypatch.setattr(migration, "_progress_path", lambda: progress_path)

    calls = 0

    def always_fails() -> int:
        nonlocal calls
        calls += 1
        raise _recovery_error()

    sleeps: list[float] = []
    monkeypatch.setattr(migration, "main", always_fails)
    monkeypatch.setattr(coarse.time, "sleep", sleeps.append)

    with pytest.raises(OperationalError, match="recovery mode"):
        coarse._run_with_bounded_startup_source_retry()

    assert calls == len(migration.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS) + 1
    assert sleeps == list(migration.APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS)
    report = json.loads(progress_path.read_text())
    assert report["tables"]["market_quotes"]["source_transport_retries"] == 7


def test_startup_retry_never_reenters_after_durable_terminal_truth(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    _seed_running_progress(progress_path)
    report = json.loads(progress_path.read_text())
    report["state"] = "failed"
    progress_path.write_text(json.dumps(report))
    monkeypatch.setattr(migration, "_progress_path", lambda: progress_path)

    calls = 0

    def fails_after_terminal() -> int:
        nonlocal calls
        calls += 1
        raise _recovery_error()

    sleeps: list[float] = []
    monkeypatch.setattr(migration, "main", fails_after_terminal)
    monkeypatch.setattr(coarse.time, "sleep", sleeps.append)

    with pytest.raises(OperationalError, match="recovery mode"):
        coarse._run_with_bounded_startup_source_retry()

    assert calls == 1
    assert sleeps == []
    persisted = json.loads(progress_path.read_text())
    assert persisted["tables"]["market_quotes"]["source_transport_retries"] == 4


def test_startup_retry_rejects_non_transient_operational_errors(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    _seed_running_progress(progress_path)
    monkeypatch.setattr(migration, "_progress_path", lambda: progress_path)
    monkeypatch.setattr(migration, "main", lambda: (_ for _ in ()).throw(_non_transient_error()))
    sleeps: list[float] = []
    monkeypatch.setattr(coarse.time, "sleep", sleeps.append)

    with pytest.raises(OperationalError, match="password authentication failed"):
        coarse._run_with_bounded_startup_source_retry()

    assert sleeps == []
    persisted = json.loads(progress_path.read_text())
    assert persisted["tables"]["market_quotes"]["source_transport_retries"] == 4
