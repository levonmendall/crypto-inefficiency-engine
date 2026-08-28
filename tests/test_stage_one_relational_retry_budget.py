from __future__ import annotations

import json

import pytest
from sqlalchemy.exc import OperationalError

import inefficiency_engine.stage_one_local_persistence_migration as stage_one


class _Source:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def _transient_disconnect() -> OperationalError:
    return OperationalError(
        "SELECT 1",
        {},
        RuntimeError("SSL connection has been closed unexpectedly"),
    )


def _write_failed_table(progress_path, table_name: str) -> None:
    progress_path.write_text(
        json.dumps(
            {
                "state": "failed",
                "current_table": table_name,
                "tables": {table_name: {"verified": False}},
            }
        )
    )


def test_relational_retry_budget_resets_after_advancing_to_new_table(tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    source = _Source()
    attempts = iter(
        [
            "table_a",
            "table_a",
            "table_a",
            "table_b",
            "table_b",
            "table_b",
            "success",
        ]
    )

    def fake_migrate(*args, **kwargs):
        outcome = next(attempts)
        if outcome == "success":
            return {"state": "verified"}
        _write_failed_table(progress_path, outcome)
        raise _transient_disconnect()

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", fake_migrate)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _: None)
    monkeypatch.setattr(stage_one.base, "_is_transient_source_read_error", lambda _: True)

    result = stage_one._migrate_engines_with_relational_source_retry(
        source,
        object(),
        object(),
        progress_path=progress_path,
    )

    assert result == {"state": "verified"}
    assert source.dispose_calls == 6


def test_relational_retry_budget_remains_bounded_for_same_table(tmp_path, monkeypatch):
    progress_path = tmp_path / "progress.json"
    source = _Source()
    calls = 0

    def fake_migrate(*args, **kwargs):
        nonlocal calls
        calls += 1
        _write_failed_table(progress_path, "table_a")
        raise _transient_disconnect()

    monkeypatch.setattr(stage_one, "_BASE_MIGRATE_ENGINES", fake_migrate)
    monkeypatch.setattr(stage_one.time, "sleep", lambda _: None)
    monkeypatch.setattr(stage_one.base, "_is_transient_source_read_error", lambda _: True)

    with pytest.raises(OperationalError):
        stage_one._migrate_engines_with_relational_source_retry(
            source,
            object(),
            object(),
            progress_path=progress_path,
        )

    assert calls == 4
    assert source.dispose_calls == 3
