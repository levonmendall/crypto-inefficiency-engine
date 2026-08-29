from __future__ import annotations

import json
import threading
from pathlib import Path

from sqlalchemy.exc import OperationalError

import inefficiency_engine.local_persistence_migration_supervisor as supervisor
from inefficiency_engine import postgres_local_migration as migration
from inefficiency_engine.stage_one_market_inventory import install_bounded_market_inventory


PRODUCTION_REFUSAL = (
    'connection failed: connection to server at "10.15.140.76", port 5432 failed: '
    'Connection refused Is the server running on that host and accepting TCP/IP connections?'
)


def _operational_error() -> OperationalError:
    return OperationalError("SELECT market_quotes.id", {}, OSError(PRODUCTION_REFUSAL))


def test_stage_one_source_reader_retries_exact_connection_refused(tmp_path, monkeypatch):
    install_bounded_market_inventory(migration)
    monkeypatch.setattr(migration, "APPEND_ONLY_SOURCE_READ_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))
    monkeypatch.setattr(migration.time, "sleep", lambda _seconds: None)

    class FakeSource:
        def __init__(self) -> None:
            self.disposals = 0

        def dispose(self) -> None:
            self.disposals += 1

    source = FakeSource()
    attempts = 0

    def reader():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _operational_error()
        return "recovered"

    table_report = {"last_primary_key": [1427261]}
    report = {"state": "running", "tables": {"market_quotes": table_report}}
    progress_path = tmp_path / "postgres-import-progress.json"

    result = migration._source_read_with_retry(
        source,
        reader,
        table_report,
        report,
        progress_path,
        phase="market_copy_batch",
    )

    assert result == "recovered"
    assert attempts == 2
    assert source.disposals == 1
    assert table_report["last_primary_key"] == [1427261]
    assert table_report["source_transport_retries"] == 1
    assert table_report["last_source_retry_phase"] == "market_copy_batch"
    assert table_report["last_source_retry_recovered"] is True


def _paths(tmp_path: Path):
    migration_dir = tmp_path / "migration"
    migration_dir.mkdir(parents=True, exist_ok=True)
    return (
        migration_dir / "postgres-import-supervisor.json",
        migration_dir / "postgres-import-progress.json",
        migration_dir / "postgres-import.lock",
        migration_dir / "postgres-import.stdout.log",
        migration_dir / "postgres-import.stderr.log",
    )


def test_supervisor_retries_exact_connection_refused_from_preserved_market_checkpoint(
    tmp_path, monkeypatch
):
    paths = _paths(tmp_path)
    progress_path = paths[1]
    calls: list[int] = []

    monkeypatch.setenv("CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION", "true")
    monkeypatch.setenv("CIE_STORAGE_ROOT", "/var/data/cie")
    monkeypatch.setenv("DATABASE_URL", "postgresql://authoritative")
    monkeypatch.setenv("CIE_MIGRATION_POSTGRES_URL", "postgresql://authoritative")
    monkeypatch.setattr(supervisor, "_storage_root_state", lambda: (True, "ready"))
    monkeypatch.setattr(supervisor, "_paths", lambda: paths)
    monkeypatch.setattr(supervisor, "_wait_for_api_bind", lambda stop_event: True)
    monkeypatch.setattr(supervisor, "TRANSIENT_SOURCE_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))

    assert supervisor._is_transient_source_disconnect(
        {"error_type": "OperationalError", "error": PRODUCTION_REFUSAL}
    ) is True

    class FakeChild:
        pid = 4321

        def __init__(self, returncode: int):
            self.returncode = returncode

        def poll(self):
            return self.returncode

    def fake_popen(*args, **kwargs):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            progress_path.write_text(
                json.dumps(
                    {
                        "state": "failed",
                        "current_table": "market_quotes",
                        "error_type": "OperationalError",
                        "error": PRODUCTION_REFUSAL,
                        "tables": {
                            "market_quotes": {
                                "verified": False,
                                "high_water_primary_key": [2812933],
                                "last_primary_key": [1427261],
                            }
                        },
                    }
                )
            )
            return FakeChild(1)
        progress_path.write_text(
            json.dumps(
                {
                    "state": "verified",
                    "completed_at": "2026-08-29T22:00:00+00:00",
                    "tables": {
                        "market_quotes": {
                            "verified": True,
                            "high_water_primary_key": [2812933],
                            "last_primary_key": [2812933],
                        }
                    },
                }
            )
        )
        return FakeChild(0)

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    supervisor.run_local_persistence_migration_supervisor(threading.Event())

    status = json.loads(paths[0].read_text())
    assert calls == [1, 2]
    assert status["state"] == "verified"
    assert status["attempt"] == 2
    assert status["source_disconnect_retries"] == 1
