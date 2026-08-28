from __future__ import annotations

import json

import pytest
from sqlalchemy.exc import OperationalError

import inefficiency_engine.postgres_local_migration as base
import inefficiency_engine.stage_one_local_persistence_migration as stage_one


class _Source:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def _operational_error(message: str) -> OperationalError:
    return OperationalError("SELECT 1", {}, RuntimeError(message))


def _write_progress(path, current_table: str) -> None:
    path.write_text(
        json.dumps(
            {
                "state": "failed",
                "current_table": current_table,
                "tables": {
                    current_table: {
                        "verified": False,
                        "verification_scope": "repeatable_read_primary_key_high_water",
                    }
                },
            }
        )
    )


def test_transient_mutable_table_source_drop_retries_inside_same_child(tmp_path, monkeypatch):
    progress = tmp_path / "progress.json"
    _write_progress(progress, "dashboard_projection_snapshots")
    source = _Source()
    attempts = 0

    def fake_migrate(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _operational_error("SSL error: unexpected eof while reading")
        return {"state": "verified"}

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", fake_migrate)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _delay: None)

    result = stage_one._migrate_engines_with_relational_source_retry(
        source,
        object(),
        object(),
        progress_path=progress,
    )

    assert result == {"state": "verified"}
    assert attempts == 2
    assert source.dispose_calls == 1


def test_transient_mutable_table_retry_is_bounded(tmp_path, monkeypatch):
    progress = tmp_path / "progress.json"
    _write_progress(progress, "dashboard_projection_snapshots")
    source = _Source()
    attempts = 0

    def always_fails(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise _operational_error("SSL error: unexpected eof while reading")

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", always_fails)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _delay: None)

    with pytest.raises(OperationalError):
        stage_one._migrate_engines_with_relational_source_retry(
            source,
            object(),
            object(),
            progress_path=progress,
        )

    assert attempts == 1 + len(stage_one.RELATIONAL_SOURCE_RETRY_DELAYS_SECONDS)
    assert source.dispose_calls == len(stage_one.RELATIONAL_SOURCE_RETRY_DELAYS_SECONDS)


@pytest.mark.parametrize("current_table", ["cycle_historical_quotes", "market_quotes"])
def test_specialized_history_paths_do_not_get_duplicate_retry_layer(
    tmp_path, monkeypatch, current_table
):
    progress = tmp_path / "progress.json"
    _write_progress(progress, current_table)
    source = _Source()
    attempts = 0

    def fails(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise _operational_error("SSL error: unexpected eof while reading")

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", fails)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _delay: None)

    with pytest.raises(OperationalError):
        stage_one._migrate_engines_with_relational_source_retry(
            source,
            object(),
            object(),
            progress_path=progress,
        )

    assert attempts == 1
    assert source.dispose_calls == 0


def test_non_transient_relational_error_still_fails_closed(tmp_path, monkeypatch):
    progress = tmp_path / "progress.json"
    _write_progress(progress, "dashboard_projection_snapshots")
    source = _Source()
    attempts = 0

    def fails(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise _operational_error("permission denied for relation dashboard_projection_snapshots")

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", fails)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _delay: None)

    with pytest.raises(OperationalError):
        stage_one._migrate_engines_with_relational_source_retry(
            source,
            object(),
            object(),
            progress_path=progress,
        )

    assert attempts == 1
    assert source.dispose_calls == 0


def test_stage_one_install_routes_migrate_engines_through_relational_retry(monkeypatch):
    monkeypatch.setattr(base, "migrate_engines", stage_one._BASE_MIGRATE_ENGINES)
    stage_one.install_stage_one_repair()
    assert base.migrate_engines is stage_one._migrate_engines_with_relational_source_retry
