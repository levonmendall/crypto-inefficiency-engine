from __future__ import annotations

import json
import threading
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
    monkeypatch.setattr(supervisor, "TRANSIENT_SOURCE_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))
    return paths


def test_exact_production_ssl_eof_is_retryable():
    assert supervisor._is_transient_source_disconnect(
        {
            "error_type": "OperationalError",
            "error": (
                "(psycopg.OperationalError) consuming input failed: "
                "SSL error: unexpected eof while reading"
            ),
        }
    ) is True
    assert supervisor._is_transient_source_disconnect(
        {"error_type": "RuntimeError", "error": "row-content mismatch"}
    ) is False


def test_exact_production_database_recovery_mode_is_retryable():
    assert supervisor._is_transient_source_disconnect(
        {
            "error_type": "OperationalError",
            "error": (
                "(psycopg.OperationalError) connection failed: connection to server "
                "at '10.15.140.76', port 5432 failed: FATAL:  the database system "
                "is in recovery mode"
            ),
        }
    ) is True


def test_exact_production_not_yet_accepting_connections_is_retryable():
    assert supervisor._is_transient_source_disconnect(
        {
            "error_type": "OperationalError",
            "error": (
                "(psycopg.OperationalError) connection failed: connection to server "
                "at '10.15.140.76', port 5432 failed: FATAL:  the database system "
                "is not yet accepting connections DETAIL:  Consistent recovery state "
                "has not been yet reached."
            ),
        }
    ) is True


def test_supervisor_retries_transient_source_disconnect_then_verifies(tmp_path, monkeypatch):
    paths = _configure(monkeypatch, tmp_path)
    progress_path = paths[1]
    calls = []

    class FakeChild:
        pid = 1234

        def __init__(self, returncode: int):
            self.returncode = returncode

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            progress_path.write_text(json.dumps({
                "state": "failed",
                "current_table": "cycle_historical_quotes",
                "error_type": "OperationalError",
                "error": (
                    "(psycopg.OperationalError) consuming input failed: "
                    "SSL error: unexpected eof while reading"
                ),
                "tables": {"cycle_historical_quotes": {"verified": False}},
            }))
            return FakeChild(1)
        progress_path.write_text(json.dumps({
            "state": "verified",
            "completed_at": "2026-08-28T02:30:00+00:00",
            "tables": {"cycle_historical_quotes": {"verified": True}},
        }))
        return FakeChild(0)

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    supervisor.run_local_persistence_migration_supervisor(threading.Event())

    status = json.loads(paths[0].read_text())
    assert calls == [1, 2]
    assert status["state"] == "verified"
    assert status["reason"] == "snapshot_verification_complete"
    assert status["attempt"] == 2
    assert status["source_disconnect_retries"] == 1


def test_supervisor_retries_recovery_mode_then_verifies(tmp_path, monkeypatch):
    paths = _configure(monkeypatch, tmp_path)
    progress_path = paths[1]
    calls = []

    class FakeChild:
        pid = 2468

        def __init__(self, returncode: int):
            self.returncode = returncode

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            progress_path.write_text(json.dumps({
                "state": "failed",
                "current_table": "cycle_historical_quotes",
                "error_type": "OperationalError",
                "error": (
                    "(psycopg.OperationalError) connection failed: FATAL:  "
                    "the database system is in recovery mode"
                ),
                "tables": {"cycle_historical_quotes": {"verified": False}},
            }))
            return FakeChild(1)
        progress_path.write_text(json.dumps({
            "state": "verified",
            "completed_at": "2026-08-28T03:35:00+00:00",
            "tables": {"cycle_historical_quotes": {"verified": True}},
        }))
        return FakeChild(0)

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    supervisor.run_local_persistence_migration_supervisor(threading.Event())

    status = json.loads(paths[0].read_text())
    assert calls == [1, 2]
    assert status["state"] == "verified"
    assert status["reason"] == "snapshot_verification_complete"
    assert status["attempt"] == 2
    assert status["source_disconnect_retries"] == 1


def test_supervisor_does_not_retry_non_transport_failure(tmp_path, monkeypatch):
    paths = _configure(monkeypatch, tmp_path)
    progress_path = paths[1]
    calls = []

    class FakeChild:
        pid = 5678
        returncode = 1

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(1)
        progress_path.write_text(json.dumps({
            "state": "failed",
            "current_table": "candidate_observatory_historical_replay_checkpoints",
            "error_type": "RuntimeError",
            "error": "row-content mismatch",
            "tables": {
                "candidate_observatory_historical_replay_checkpoints": {"verified": False}
            },
        }))
        return FakeChild()

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    supervisor.run_local_persistence_migration_supervisor(threading.Event())

    status = json.loads(paths[0].read_text())
    assert calls == [1]
    assert status["state"] == "failed"
    assert status["reason"] == "migration_child_failed"
    assert status["source_disconnect_retries"] == 0
