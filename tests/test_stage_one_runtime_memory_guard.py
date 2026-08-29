from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

import inefficiency_engine as package
import inefficiency_engine.local_persistence_migration_supervisor as supervisor
import inefficiency_engine.postgres_local_migration as migration
import inefficiency_engine.stage_one_local_persistence_migration as stage_one


class _DisposableEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    def dispose(self) -> None:
        self.dispose_calls += 1


def test_stage_one_runtime_guard_caps_batches_and_skips_verified_rescans_on_retry(monkeypatch, tmp_path):
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
    progress_path = tmp_path / "progress.json"

    assert guarded(object(), target, object(), progress_path=progress_path, batch_size=2_000) == {
        "state": "verified"
    }
    assert calls[0]["batch_size"] == 256
    assert calls[0]["verified_helper"] is original_verified
    assert calls[0]["append_helper"] is not original_append

    assert guarded(object(), target, object(), progress_path=progress_path, batch_size=2_000) == {
        "state": "verified"
    }
    assert calls[1]["batch_size"] == 256
    assert calls[1]["verified_helper"] is not original_verified
    assert calls[1]["append_helper"] is not original_append
    assert target_engine.dispose_calls == 1

    assert migration._verified_target_is_intact is original_verified
    assert migration._migrate_resumable_append_only_table is original_append


def test_retry_verified_helper_accepts_only_already_verified_tables(monkeypatch, tmp_path):
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
    progress_path = tmp_path / "progress.json"
    guarded(object(), target, object(), progress_path=progress_path, batch_size=2_000)
    guarded(object(), target, object(), progress_path=progress_path, batch_size=2_000)

    assert observed["verified_result"] is True
    assert observed["unverified_result"] is False
    assert observed["fallback_called"] is True


def test_stage_one_runtime_guard_routes_proven_append_only_ledgers_to_captured_path(monkeypatch):
    routed = set(migration.RESUMABLE_APPEND_ONLY_TABLES)
    routed.discard("dashboard_projection_snapshots")
    routed.discard("funding_quotes")
    routed.discard("source_coverage_history")
    routed.discard("source_event_observations")
    routed.discard("worker_heartbeats")

    def fake_migrate(source, target, history, *, progress_path, batch_size, interrupt_after_batches=None):
        return {"state": "verified"}

    monkeypatch.setattr(migration, "RESUMABLE_APPEND_ONLY_TABLES", routed)
    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)

    package._install_stage_one_runtime_memory_guard()

    assert "cycle_historical_quotes" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "dashboard_projection_snapshots" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "funding_quotes" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "source_coverage_history" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "source_event_observations" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert "worker_heartbeats" in migration.RESUMABLE_APPEND_ONLY_TABLES
    assert package._STAGE_ONE_MONOTONIC_HIGH_WATER_TABLES == {
        "funding_quotes",
        "source_event_observations",
        "worker_heartbeats",
    }


@pytest.mark.parametrize(
    "table_name",
    [
        "dashboard_projection_snapshots",
        "funding_quotes",
        "source_coverage_history",
        "source_event_observations",
        "worker_heartbeats",
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


def test_durable_funding_checkpoint_is_selected_before_schema_traversal() -> None:
    progress = {
        "state": "running",
        "current_table": "source_event_observations",
        "tables": {
            "dashboard_projection_snapshots": {"verified": True},
            "funding_quotes": {
                "verified": False,
                "migration_mode": "captured_monotonic_integer_high_water",
                "snapshot_high_water_captured": True,
                "snapshot_high_water_primary_key": [5714625],
                "snapshot_phase": "copying_snapshot",
                "last_primary_key": [4542494],
                "snapshot_rows_copied": 4526080,
            },
            "source_event_observations": {"verified": True},
            "worker_heartbeats": {"verified": True},
        },
    }

    assert package._durable_monotonic_resume_candidates(progress) == ["funding_quotes"]


def test_verified_or_unbounded_monotonic_tables_are_not_priority_resumed() -> None:
    progress = {
        "tables": {
            "funding_quotes": {
                "verified": True,
                "migration_mode": "captured_monotonic_integer_high_water",
                "snapshot_high_water_captured": True,
                "snapshot_phase": "verified",
            },
            "source_event_observations": {
                "verified": False,
                "migration_mode": "captured_monotonic_integer_high_water",
                "snapshot_high_water_captured": False,
                "snapshot_phase": "capturing_high_water",
            },
        }
    }

    assert package._durable_monotonic_resume_candidates(progress) == []


def test_guard_priority_resume_runs_before_normal_migration(monkeypatch, tmp_path):
    events: list[str] = []

    def fake_resume(*args, **kwargs):
        events.append("resume")

    def fake_migrate(source, target, history, *, progress_path, batch_size, interrupt_after_batches=None):
        events.append("traverse")
        return {"state": "verified"}

    monkeypatch.setattr(package, "_resume_durable_monotonic_checkpoints_first", fake_resume)
    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)
    package._install_stage_one_runtime_memory_guard()

    target = SimpleNamespace(engine=_DisposableEngine())
    migration.migrate_engines(
        object(),
        target,
        object(),
        progress_path=tmp_path / "progress.json",
        batch_size=2_000,
    )

    assert events == ["resume", "traverse"]


def test_priority_resume_transient_failure_is_durable_and_supervisor_retryable(
    monkeypatch,
    tmp_path,
) -> None:
    progress_path = tmp_path / "progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "state": "running",
                "current_table": "funding_quotes",
                "tables": {
                    "funding_quotes": {
                        "verified": False,
                        "migration_mode": "captured_monotonic_integer_high_water",
                        "snapshot_high_water_captured": True,
                        "snapshot_high_water_primary_key": [5714625],
                        "snapshot_phase": "copying_snapshot",
                        "last_primary_key": [5665703],
                        "snapshot_rows_copied": 5638400,
                        "source_transport_retries": 41,
                    }
                },
                "postgresql_authoritative": True,
                "cutover_ready": False,
            }
        )
    )
    traversal_calls: list[int] = []

    def fake_resume(*args, **kwargs):
        raise OperationalError(
            "SELECT funding_quotes",
            {},
            Exception("server closed the connection unexpectedly"),
        )

    def fake_migrate(source, target, history, *, progress_path, batch_size, interrupt_after_batches=None):
        traversal_calls.append(1)
        return {"state": "verified"}

    monkeypatch.setattr(package, "_resume_durable_monotonic_checkpoints_first", fake_resume)
    monkeypatch.setattr(migration, "migrate_engines", fake_migrate)
    package._install_stage_one_runtime_memory_guard()

    target = SimpleNamespace(engine=_DisposableEngine())
    with pytest.raises(OperationalError):
        migration.migrate_engines(
            object(),
            target,
            object(),
            progress_path=progress_path,
            batch_size=2_000,
        )

    persisted = json.loads(progress_path.read_text())
    funding = persisted["tables"]["funding_quotes"]
    assert persisted["state"] == "failed"
    assert persisted["error_type"] == "OperationalError"
    assert "server closed the connection unexpectedly" in persisted["error"]
    assert persisted["failure_phase"] == "priority_monotonic_resume"
    assert persisted["failure_table"] == "funding_quotes"
    assert funding["last_primary_key"] == [5665703]
    assert funding["snapshot_high_water_primary_key"] == [5714625]
    assert funding["snapshot_rows_copied"] == 5638400
    assert supervisor._is_transient_source_disconnect(persisted) is True
    assert traversal_calls == []


def test_priority_resume_nontransient_failure_remains_fail_closed(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "state": "running",
                "current_table": "funding_quotes",
                "tables": {
                    "funding_quotes": {
                        "verified": False,
                        "migration_mode": "captured_monotonic_integer_high_water",
                        "snapshot_high_water_captured": True,
                        "snapshot_high_water_primary_key": [5714625],
                        "snapshot_phase": "copying_snapshot",
                        "last_primary_key": [5665703],
                        "snapshot_rows_copied": 5638400,
                    }
                },
            }
        )
    )

    def fake_resume(*args, **kwargs):
        raise RuntimeError("captured monotonic snapshot mismatch for funding_quotes")

    monkeypatch.setattr(package, "_resume_durable_monotonic_checkpoints_first", fake_resume)
    monkeypatch.setattr(
        migration,
        "migrate_engines",
        lambda *args, **kwargs: {"state": "verified"},
    )
    package._install_stage_one_runtime_memory_guard()

    with pytest.raises(RuntimeError):
        migration.migrate_engines(
            object(),
            SimpleNamespace(engine=_DisposableEngine()),
            object(),
            progress_path=progress_path,
            batch_size=2_000,
        )

    persisted = json.loads(progress_path.read_text())
    assert persisted["state"] == "failed"
    assert persisted["error_type"] == "RuntimeError"
    assert persisted["failure_phase"] == "priority_monotonic_resume"
    assert persisted["failure_table"] == "funding_quotes"
    assert supervisor._is_transient_source_disconnect(persisted) is False
    assert persisted["postgresql_authoritative"] is True
    assert persisted["cutover_ready"] is False
