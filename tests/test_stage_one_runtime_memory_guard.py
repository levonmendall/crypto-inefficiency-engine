from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import inefficiency_engine as package
import inefficiency_engine.postgres_local_migration as migration
import inefficiency_engine.stage_one_local_persistence_migration as stage_one


class _DisposableEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def test_stage_one_runtime_guard_caps_batches_and_skips_verified_rescans_on_retry(monkeypatch):
    calls: list[dict[str, object]] = []
    original_verified = migration._verified_target_is_intact
    original_append = migration._migrate_resumable_append_only_table

    def fake_migrate(
        source,
        target,
        history,
        *,
        progress_path,
        batch_size,
        interrupt_after_batches=None,
    ):
        calls.append(
            {
                "batch_size": batch_size,
                "verified_helper": migration._verified_target_is_intact,
                "append_helper": migration._migrate_resumable_append_only_table,
            }
        )
        return {"state": "verified"}

    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)
    package._install_stage_one_runtime_memory_guard()
    guarded = migration.migrate_engines

    target_engine = _DisposableEngine()
    target = SimpleNamespace(engine=target_engine)

    assert guarded(object(), target, object(), progress_path=object(), batch_size=2_000) == {
        "state": "verified"
    }
    assert calls[0]["batch_size"] == 256
    assert calls[0]["verified_helper"] is original_verified
    # Append-only work is routed even on the first call so proven monotonic integer
    # ledgers can use their finite high-water path; the original handler is restored
    # immediately after the guarded import returns.
    assert calls[0]["append_helper"] is not original_append

    assert guarded(object(), target, object(), progress_path=object(), batch_size=2_000) == {
        "state": "verified"
    }
    assert calls[1]["batch_size"] == 256
    assert calls[1]["verified_helper"] is not original_verified
    assert calls[1]["append_helper"] is not original_append
    assert target_engine.dispose_calls == 1

    # The temporary substitutions are always restored after each call.
    assert migration._verified_target_is_intact is original_verified
    assert migration._migrate_resumable_append_only_table is original_append


def test_retry_verified_helper_accepts_only_already_verified_tables(monkeypatch):
    observed: dict[str, object] = {}

    def fake_verified(target, table, shared, table_report):
        observed["fallback_called"] = True
        return False

    def fake_migrate(source, target, history, *, progress_path, batch_size, interrupt_after_batches=None):
        helper = migration._verified_target_is_intact
        observed["verified_result"] = helper(object(), object(), [], {"verified": True})
        observed["unverified_result"] = helper(object(), object(), [], {"verified": False})
        return {"state": "verified"}

    monkeypatch.setattr(migration, "_verified_target_is_intact", fake_verified)
    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)
    package._install_stage_one_runtime_memory_guard()
    guarded = migration.migrate_engines

    target = SimpleNamespace(engine=_DisposableEngine())
    guarded(object(), target, object(), progress_path=object(), batch_size=2_000)
    guarded(object(), target, object(), progress_path=object(), batch_size=2_000)

    assert observed["verified_result"] is True
    assert observed["unverified_result"] is False
    assert observed["fallback_called"] is True


def test_stage_one_runtime_guard_routes_proven_append_only_ledgers_to_captured_path(monkeypatch):
    routed = set(migration.RESUMABLE_APPEND_ONLY_TABLES)
    routed.discard("dashboard_projection_snapshots")
    routed.discard("source_coverage_history")
    routed.discard("source_event_observations")

    def fake_migrate(source, target, history, *, progress_path, batch_size, interrupt_after_batches=None):
        return {"state": "verified"}

    monkeypatch.setattr(migration, "RESUMABLE_APPEND_ONLY_TABLES", routed)
    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)

    package._install_stage_one_runtime_memory_guard()

    assert "cycle_historical_quotes" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "dashboard_projection_snapshots" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "source_coverage_history" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "source_event_observations" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert package._STAGE_ONE_MONOTONIC_HIGH_WATER_TABLES == {"source_event_observations"}


@pytest.mark.parametrize(
    "table_name",
    [
        "dashboard_projection_snapshots",
        "source_coverage_history",
        "source_event_observations",
    ],
)
def test_captured_append_only_routing_avoids_generic_whole_import_retry(
    table_name, tmp_path, monkeypatch
):
    routed = set(migration.RESUMABLE_APPEND_ONLY_TABLES)
    routed.add(table_name)
    monkeypatch.setattr(migration, "RESUMABLE_APPEND_ONLY_TABLES", routed)

    progress = tmp_path / "progress.json"
    progress.write_text(
        json.dumps(
            {
                "state": "failed",
                "current_table": table_name,
                "tables": {table_name: {"verified": False}},
            }
        )
    )

    assert stage_one._current_unverified_relational_table(progress) is None


def test_stage_one_module_detection_uses_original_python_argv(monkeypatch):
    monkeypatch.setattr(package.sys, "orig_argv", ["python", "-m", package._STAGE_ONE_MODULE])
    assert package._running_stage_one_migration() is True

    monkeypatch.setattr(package.sys, "orig_argv", ["python", "-m", "inefficiency_engine.render_combined"])
    assert package._running_stage_one_migration() is False
