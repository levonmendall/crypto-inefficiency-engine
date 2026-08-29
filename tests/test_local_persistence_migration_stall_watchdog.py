from __future__ import annotations

import json
from pathlib import Path

import inefficiency_engine.local_persistence_migration_supervisor as supervisor


def _paths(tmp_path: Path):
    migration = tmp_path / "migration"
    migration.mkdir(parents=True, exist_ok=True)
    return (
        migration / "postgres-import-supervisor.json",
        migration / "postgres-import-progress.json",
        migration / "postgres-import.lock",
        migration / "postgres-import.stdout.log",
        migration / "postgres-import.stderr.log",
    )


def _configure(monkeypatch, tmp_path: Path):
    paths = _paths(tmp_path)
    monkeypatch.setenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", "true")
    monkeypatch.setenv("CIE_STORAGE_ROOT", "/var/data/cie")
    monkeypatch.setenv("DATABASE_URL", "postgresql://authoritative")
    monkeypatch.setenv("CIE_MIGRATION_POSTGRES_URL", "postgresql://authoritative")
    monkeypatch.setattr(supervisor, "_storage_root_state", lambda: (True, "ready"))
    monkeypatch.setattr(supervisor, "_paths", lambda: paths)
    monkeypatch.setattr(supervisor, "_wait_for_api_bind", lambda stop_event: True)
    monkeypatch.setattr(supervisor, "MONOTONIC_COPY_STALL_SECONDS", 0.0)
    monkeypatch.setattr(supervisor, "STALLED_PROGRESS_RETRY_DELAY_SECONDS", 0.0)
    return paths


class _ImmediateEvent:
    def wait(self, timeout=None):
        return False

    def is_set(self):
        return False


def _funding_progress() -> dict[str, object]:
    return {
        "state": "running",
        "current_table": "funding_quotes",
        "tables": {
            "funding_quotes": {
                "verified": False,
                "migration_mode": "captured_monotonic_integer_high_water",
                "snapshot_phase": "copying_snapshot",
                "snapshot_high_water_primary_key": [5714625],
                "last_primary_key": [4542494],
                "snapshot_rows_copied": 4526080,
                "last_progress_at": "2026-08-28T23:33:27.518593+00:00",
            }
        },
    }


def test_watchdog_marker_is_scoped_to_restart_safe_monotonic_copy() -> None:
    progress = _funding_progress()
    marker = supervisor._monotonic_copy_progress_marker(progress)

    assert marker is not None
    assert marker[0] == "funding_quotes"
    assert marker[3] == 4526080

    progress["tables"]["funding_quotes"]["snapshot_phase"] = "verifying_snapshot"
    assert supervisor._monotonic_copy_progress_marker(progress) is None

    progress["tables"]["funding_quotes"]["snapshot_phase"] = "copying_snapshot"
    progress["tables"]["funding_quotes"]["migration_mode"] = "captured_primary_key_membership_manifest"
    assert supervisor._monotonic_copy_progress_marker(progress) is None


def test_supervisor_restarts_stalled_funding_copy_from_durable_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    paths = _configure(monkeypatch, tmp_path)
    progress_path = paths[1]
    progress_path.write_text(json.dumps(_funding_progress()))
    calls = []
    terminated_checkpoints = []

    class FakeChild:
        def __init__(self, pid: int, returncode):
            self.pid = pid
            self.returncode = returncode

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            return FakeChild(1234, None)
        progress_path.write_text(json.dumps({
            "state": "verified",
            "completed_at": "2026-08-29T01:00:00+00:00",
            "tables": {"funding_quotes": {"verified": True}},
        }))
        return FakeChild(5678, 0)

    def fake_terminate(child):
        progress = json.loads(progress_path.read_text())
        funding = progress["tables"]["funding_quotes"]
        terminated_checkpoints.append((
            funding["last_primary_key"],
            funding["snapshot_rows_copied"],
            funding["snapshot_high_water_primary_key"],
        ))
        child.returncode = -15

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(supervisor, "_terminate_child", fake_terminate)

    supervisor.run_local_persistence_migration_supervisor(_ImmediateEvent())

    status = json.loads(paths[0].read_text())
    assert calls == [1, 2]
    assert terminated_checkpoints == [([4542494], 4526080, [5714625])]
    assert status["state"] == "verified"
    assert status["attempt"] == 2
    assert status["stalled_progress_restarts"] == 1
    assert status["source_disconnect_retries"] == 0


def test_supervisor_fails_closed_when_stalled_restart_budget_is_exhausted(
    tmp_path,
    monkeypatch,
) -> None:
    paths = _configure(monkeypatch, tmp_path)
    progress_path = paths[1]
    progress_path.write_text(json.dumps(_funding_progress()))
    monkeypatch.setattr(supervisor, "MAX_STALLED_PROGRESS_RESTARTS", 1)
    calls = []

    class FakeChild:
        pid = 1234

        def __init__(self):
            self.returncode = None

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(1)
        return FakeChild()

    def fake_terminate(child):
        child.returncode = -15

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(supervisor, "_terminate_child", fake_terminate)

    supervisor.run_local_persistence_migration_supervisor(_ImmediateEvent())

    status = json.loads(paths[0].read_text())
    assert len(calls) == 2
    assert status["state"] == "failed"
    assert status["reason"] == "stalled_monotonic_copy_retry_exhausted"
    assert status["stalled_progress_restarts"] == 2
    assert status["stalled_current_table"] == "funding_quotes"
    assert status["stalled_last_primary_key"] == [4542494]
    assert status["stalled_snapshot_rows_copied"] == 4526080
    assert status["stalled_snapshot_high_water_primary_key"] == [5714625]
    assert status["postgresql_authoritative"] is True
    assert status["cutover_ready"] is False
