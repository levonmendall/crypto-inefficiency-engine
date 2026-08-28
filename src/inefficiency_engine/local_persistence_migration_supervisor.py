from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inefficiency_engine.local_storage import local_storage_paths

AUTO_MIGRATION_ENV = "CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION"
MIGRATION_COMMAND = [sys.executable, "-m", "inefficiency_engine.postgres_local_migration"]
API_BIND_WAIT_SECONDS = 180.0
TRUE_VALUES = {"1", "true", "yes", "on"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _paths() -> tuple[Path, Path, Path, Path, Path]:
    migration = local_storage_paths().migration
    return (
        migration / "postgres-import-supervisor.json",
        migration / "postgres-import-progress.json",
        migration / "postgres-import.lock",
        migration / "postgres-import.stdout.log",
        migration / "postgres-import.stderr.log",
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _publish_status(payload: dict[str, object]) -> None:
    status_path, _, _, _, _ = _paths()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = status_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, status_path)


def migration_preflight() -> tuple[bool, str]:
    if os.getenv(AUTO_MIGRATION_ENV, "").strip().lower() not in TRUE_VALUES:
        return False, "automatic_migration_disabled"
    storage_root = os.getenv("CIE_STORAGE_ROOT", "").strip()
    if not storage_root:
        return False, "storage_root_missing"
    root = Path(storage_root).expanduser()
    if not root.is_absolute() or str(root).startswith("/tmp"):
        return False, "storage_root_not_durable"
    migration_url = os.getenv("CIE_MIGRATION_POSTGRES_URL", "").strip()
    if not migration_url:
        return False, "migration_postgres_url_missing"
    authoritative_url = (
        os.getenv("CIE_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )
    if not authoritative_url:
        return False, "authoritative_postgres_url_missing"
    if migration_url != authoritative_url:
        return False, "migration_source_not_authoritative_database"
    if os.getenv("CIE_MARKET_HISTORY_BACKEND", "").strip().lower() == "parquet":
        return False, "local_history_authority_already_enabled"
    if os.getenv("CIE_EVIDENCE_DB_PATH", "").strip():
        return False, "local_metadata_authority_already_enabled"
    return True, "ready"


def _wait_for_api_bind(stop_event: threading.Event, timeout: float = API_BIND_WAIT_SECONDS) -> bool:
    try:
        port = int(os.getenv("PORT", "10000"))
    except ValueError:
        return False
    deadline = time.monotonic() + timeout
    while not stop_event.is_set() and time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            stop_event.wait(1.0)
    return False


def _terminate_child(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        child.terminate()
    try:
        child.wait(timeout=8.0)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            child.kill()
        child.wait(timeout=8.0)


def migration_status_payload() -> dict[str, object]:
    status_path, progress_path, _, _, _ = _paths()
    supervisor = _read_json(status_path)
    progress = _read_json(progress_path)
    tables = progress.get("tables") if isinstance(progress.get("tables"), dict) else {}
    verified_tables = sum(
        1 for value in tables.values()
        if isinstance(value, dict) and value.get("verified") is True
    )
    current_table = next(
        (
            name for name, value in tables.items()
            if not (isinstance(value, dict) and value.get("verified") is True)
        ),
        None,
    )
    market = tables.get("market_quotes") if isinstance(tables.get("market_quotes"), dict) else {}
    destination = market.get("destination_inventory") if isinstance(market.get("destination_inventory"), dict) else {}
    return {
        "state": supervisor.get("state") or progress.get("state") or "not_started",
        "supervisor_reason": supervisor.get("reason"),
        "supervisor_started_at": supervisor.get("started_at"),
        "supervisor_completed_at": supervisor.get("completed_at"),
        "child_return_code": supervisor.get("child_return_code"),
        "progress_state": progress.get("state"),
        "progress_started_at": progress.get("started_at"),
        "progress_completed_at": progress.get("completed_at"),
        "verification_scope": progress.get("verification_scope"),
        "tables_total": len(tables),
        "tables_verified": verified_tables,
        "current_table": current_table,
        "market_quotes": {
            "verified": market.get("verified"),
            "source_rows": market.get("source_rows"),
            "source_lineage_count": market.get("source_lineage_count"),
            "last_primary_key": market.get("last_primary_key"),
            "high_water_primary_key": market.get("high_water_primary_key"),
            "destination_lineage_count": destination.get("lineage_count"),
            "destination_valid": destination.get("valid"),
        },
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


def run_local_persistence_migration_supervisor(stop_event: threading.Event) -> None:
    ready, reason = migration_preflight()
    if not ready:
        _publish_status(
            {
                "state": "blocked",
                "reason": reason,
                "observed_at": _now(),
                "postgresql_authoritative": True,
                "cutover_ready": False,
                "paper_only": True,
                "live_execution_authority": False,
            }
        )
        return

    status_path, progress_path, lock_path, stdout_path, stderr_path = _paths()
    progress = _read_json(progress_path)
    if progress.get("state") == "verified":
        _publish_status(
            {
                "state": "verified",
                "reason": "durable_progress_already_verified",
                "observed_at": _now(),
                "progress_completed_at": progress.get("completed_at"),
                "postgresql_authoritative": True,
                "cutover_ready": False,
                "paper_only": True,
                "live_execution_authority": False,
            }
        )
        return

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _publish_status(
                {
                    "state": "blocked",
                    "reason": "another_importer_holds_lock",
                    "observed_at": _now(),
                    "postgresql_authoritative": True,
                    "cutover_ready": False,
                    "paper_only": True,
                    "live_execution_authority": False,
                }
            )
            return

        started_at = _now()
        _publish_status(
            {
                "state": "waiting_for_api_bind",
                "reason": None,
                "started_at": started_at,
                "observed_at": _now(),
                "postgresql_authoritative": True,
                "cutover_ready": False,
                "paper_only": True,
                "live_execution_authority": False,
            }
        )
        if not _wait_for_api_bind(stop_event):
            _publish_status(
                {
                    "state": "interrupted" if stop_event.is_set() else "failed",
                    "reason": "service_shutdown" if stop_event.is_set() else "api_bind_deadline_exceeded",
                    "started_at": started_at,
                    "completed_at": _now(),
                    "postgresql_authoritative": True,
                    "cutover_ready": False,
                    "paper_only": True,
                    "live_execution_authority": False,
                }
            )
            return

        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with stdout_path.open("ab") as stdout_file, stderr_path.open("ab") as stderr_file:
            child = subprocess.Popen(
                MIGRATION_COMMAND,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                start_new_session=True,
            )
            _publish_status(
                {
                    "state": "running",
                    "reason": None,
                    "started_at": started_at,
                    "child_pid": child.pid,
                    "observed_at": _now(),
                    "postgresql_authoritative": True,
                    "cutover_ready": False,
                    "paper_only": True,
                    "live_execution_authority": False,
                }
            )
            while child.poll() is None:
                if stop_event.wait(1.0):
                    _terminate_child(child)
                    _publish_status(
                        {
                            "state": "interrupted",
                            "reason": "service_shutdown",
                            "started_at": started_at,
                            "completed_at": _now(),
                            "child_return_code": child.returncode,
                            "postgresql_authoritative": True,
                            "cutover_ready": False,
                            "paper_only": True,
                            "live_execution_authority": False,
                        }
                    )
                    return

            return_code = int(child.returncode or 0)

        progress = _read_json(progress_path)
        verified = return_code == 0 and progress.get("state") == "verified"
        _publish_status(
            {
                "state": "verified" if verified else "failed",
                "reason": "snapshot_verification_complete" if verified else "migration_child_failed",
                "started_at": started_at,
                "completed_at": _now(),
                "child_return_code": return_code,
                "progress_state": progress.get("state"),
                "postgresql_authoritative": True,
                "cutover_ready": False,
                "paper_only": True,
                "live_execution_authority": False,
            }
        )


__all__ = [
    "AUTO_MIGRATION_ENV",
    "MIGRATION_COMMAND",
    "migration_preflight",
    "migration_status_payload",
    "run_local_persistence_migration_supervisor",
]
