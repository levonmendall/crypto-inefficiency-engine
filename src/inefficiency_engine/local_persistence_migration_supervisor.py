from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inefficiency_engine.local_storage import (
    DEFAULT_PRODUCTION_STORAGE_ROOT,
    local_storage_paths,
)

AUTO_MIGRATION_ENV = "CIE_AUTO_LOCAL_PERSISTENCE_MIGRATION"
MIGRATION_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.stage_one_local_persistence_migration",
]
API_BIND_WAIT_SECONDS = 180.0
TRUE_VALUES = {"1", "true", "yes", "on"}
MAX_TRANSIENT_SOURCE_RETRIES = 3
TRANSIENT_SOURCE_RETRY_DELAYS_SECONDS = (1.0, 3.0, 8.0)
MONOTONIC_COPY_STALL_SECONDS = 900.0
MAX_STALLED_PROGRESS_RESTARTS = 3
STALLED_PROGRESS_RETRY_DELAY_SECONDS = 1.0
MONOTONIC_MIGRATION_MODE = "captured_monotonic_integer_high_water"
_URL_CREDENTIALS = re.compile(
    r"(?i)\b(postgres(?:ql)?(?:\+psycopg)?://)([^@\s]+)@"
)
_TRANSIENT_SOURCE_FAILURE_MARKERS = (
    "unexpected eof while reading",
    "consuming input failed",
    "server closed the connection unexpectedly",
    "connection reset by peer",
    "connection refused",
    "ssl connection has been closed unexpectedly",
    "terminating connection due to administrator command",
    "the database system is in recovery mode",
    "the database system is not yet accepting connections",
    "consistent recovery state has not been yet reached",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _configured_storage_root() -> Path:
    configured = os.getenv("CIE_STORAGE_ROOT", "").strip()
    return Path(configured or DEFAULT_PRODUCTION_STORAGE_ROOT).expanduser().resolve()


def _automatic_migration_enabled() -> bool:
    configured = os.getenv(AUTO_MIGRATION_ENV)
    if configured is not None and configured.strip():
        return configured.strip().lower() in TRUE_VALUES
    # This migration guard is installed only in the Render production runtime. Using
    # the immutable release identity as the default means a manually configured
    # service does not depend on Blueprint env-var synchronization to start stage one.
    return bool(os.getenv("RENDER_GIT_COMMIT", "").strip())


def _authoritative_postgres_url() -> str:
    return (
        os.getenv("CIE_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )


def _migration_source_url() -> str:
    return os.getenv("CIE_MIGRATION_POSTGRES_URL", "").strip() or _authoritative_postgres_url()


def _storage_root_state() -> tuple[bool, str]:
    root = _configured_storage_root()
    if not root.is_absolute() or str(root).startswith("/tmp"):
        return False, "storage_root_not_durable"
    if not root.exists():
        return False, "storage_root_missing"
    if not root.is_dir() or not os.access(root, os.W_OK | os.X_OK):
        return False, "storage_root_unwritable"
    return True, "ready"


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


def _bounded_public_error(value: object) -> str | None:
    """Expose enough terminal truth to diagnose migration failures without secrets."""

    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return None
    text = _URL_CREDENTIALS.sub(r"\1***@", text)
    return text[:600]


def _is_transient_source_disconnect(progress: dict[str, Any]) -> bool:
    """Classify only proven transient PostgreSQL source failures as retryable."""

    error_type = str(progress.get("error_type") or "").lower()
    error = str(progress.get("error") or "").lower()
    if "operationalerror" not in error_type and "psycopg.operationalerror" not in error:
        return False
    return any(marker in error for marker in _TRANSIENT_SOURCE_FAILURE_MARKERS)


def _monotonic_copy_progress_marker(
    progress: dict[str, Any],
) -> tuple[object, ...] | None:
    """Return durable copy progress only for restart-safe monotonic snapshots.

    Stage-one monotonic tables publish after every bounded source batch and persist
    both the source high-water and the last committed primary-key checkpoint. A child
    that remains alive while this marker does not move is therefore safe to terminate
    and relaunch from the durable checkpoint. Other migration phases are intentionally
    excluded because they may perform finite work without publishing batch progress.
    """

    current_table = str(progress.get("current_table") or "").strip()
    tables = progress.get("tables")
    if not current_table or not isinstance(tables, dict):
        return None
    table_report = tables.get(current_table)
    if not isinstance(table_report, dict):
        return None
    if table_report.get("verified") is True:
        return None
    if table_report.get("migration_mode") != MONOTONIC_MIGRATION_MODE:
        return None
    if table_report.get("snapshot_phase") != "copying_snapshot":
        return None
    return (
        current_table,
        table_report.get("last_progress_at"),
        json.dumps(table_report.get("last_primary_key"), sort_keys=True, default=str),
        int(table_report.get("snapshot_rows_copied") or 0),
    )


def _stalled_checkpoint_details(progress: dict[str, Any]) -> dict[str, object]:
    current_table = str(progress.get("current_table") or "").strip()
    tables = progress.get("tables")
    table_report = tables.get(current_table) if isinstance(tables, dict) else None
    if not isinstance(table_report, dict):
        table_report = {}
    return {
        "stalled_current_table": current_table or None,
        "stalled_last_progress_at": table_report.get("last_progress_at"),
        "stalled_last_primary_key": table_report.get("last_primary_key"),
        "stalled_snapshot_rows_copied": table_report.get("snapshot_rows_copied"),
        "stalled_snapshot_high_water_primary_key": table_report.get(
            "snapshot_high_water_primary_key"
        ),
    }


def _publish_status(payload: dict[str, object]) -> None:
    status_path, _, _, _, _ = _paths()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = status_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, status_path)


def migration_preflight() -> tuple[bool, str]:
    if not _automatic_migration_enabled():
        return False, "automatic_migration_disabled"
    storage_ready, storage_reason = _storage_root_state()
    if not storage_ready:
        return False, storage_reason
    authoritative_url = _authoritative_postgres_url()
    if not authoritative_url:
        return False, "authoritative_postgres_url_missing"
    explicit_migration_url = os.getenv("CIE_MIGRATION_POSTGRES_URL", "").strip()
    if explicit_migration_url and explicit_migration_url != authoritative_url:
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


def _blocked_status(reason: str) -> dict[str, object]:
    return {
        "state": "blocked",
        "supervisor_reason": reason,
        "storage_root": str(_configured_storage_root()),
        "storage_root_ready": False,
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


def migration_status_payload() -> dict[str, object]:
    storage_ready, storage_reason = _storage_root_state()
    if not storage_ready:
        return _blocked_status(storage_reason)
    try:
        status_path, progress_path, _, _, _ = _paths()
    except OSError:
        return _blocked_status("storage_root_unwritable")
    supervisor = _read_json(status_path)
    progress = _read_json(progress_path)
    tables = progress.get("tables") if isinstance(progress.get("tables"), dict) else {}
    verified_tables = sum(
        1 for value in tables.values()
        if isinstance(value, dict) and value.get("verified") is True
    )
    current_table = progress.get("current_table") or next(
        (
            name for name, value in tables.items()
            if not (isinstance(value, dict) and value.get("verified") is True)
        ),
        None,
    )
    market = tables.get("market_quotes") if isinstance(tables.get("market_quotes"), dict) else {}
    destination = market.get("destination_inventory") if isinstance(market.get("destination_inventory"), dict) else {}
    cycle_history = (
        tables.get("cycle_historical_quotes")
        if isinstance(tables.get("cycle_historical_quotes"), dict)
        else {}
    )
    return {
        "state": supervisor.get("state") or progress.get("state") or "not_started",
        "supervisor_reason": supervisor.get("reason"),
        "supervisor_started_at": supervisor.get("started_at"),
        "supervisor_completed_at": supervisor.get("completed_at"),
        "child_return_code": supervisor.get("child_return_code"),
        "supervisor_attempt": supervisor.get("attempt"),
        "source_disconnect_retries": supervisor.get("source_disconnect_retries", 0),
        "stalled_progress_restarts": supervisor.get("stalled_progress_restarts", 0),
        "stalled_current_table": supervisor.get("stalled_current_table"),
        "stalled_last_progress_at": supervisor.get("stalled_last_progress_at"),
        "stalled_last_primary_key": supervisor.get("stalled_last_primary_key"),
        "stalled_snapshot_rows_copied": supervisor.get("stalled_snapshot_rows_copied"),
        "stalled_snapshot_high_water_primary_key": supervisor.get(
            "stalled_snapshot_high_water_primary_key"
        ),
        "progress_state": progress.get("state"),
        "progress_started_at": progress.get("started_at"),
        "progress_completed_at": progress.get("completed_at"),
        "progress_error_type": progress.get("error_type"),
        "progress_error": _bounded_public_error(progress.get("error")),
        "verification_scope": progress.get("verification_scope"),
        "tables_total": len(tables),
        "tables_verified": verified_tables,
        "current_table": current_table,
        "cycle_historical_quotes": {
            "verified": cycle_history.get("verified"),
            "migration_mode": cycle_history.get("migration_mode"),
            "verification_scope": cycle_history.get("verification_scope"),
            "snapshot_row_count": cycle_history.get("snapshot_row_count"),
            "snapshot_rows_copied": cycle_history.get("snapshot_rows_copied"),
            "snapshot_rows_verified": cycle_history.get("snapshot_rows_verified"),
            "snapshot_captured_at": cycle_history.get("snapshot_captured_at"),
            "last_progress_at": cycle_history.get("last_progress_at"),
            "last_primary_key": cycle_history.get("last_primary_key"),
            "source_transport_retries": cycle_history.get("source_transport_retries", 0),
            "snapshot_capture_retries": cycle_history.get("snapshot_capture_retries", 0),
        },
        "market_quotes": {
            "verified": market.get("verified"),
            "source_rows": market.get("source_rows"),
            "source_lineage_count": market.get("source_lineage_count"),
            "last_primary_key": market.get("last_primary_key"),
            "high_water_primary_key": market.get("high_water_primary_key"),
            "destination_lineage_count": destination.get("lineage_count"),
            "destination_valid": destination.get("valid"),
        },
        "storage_root": str(_configured_storage_root()),
        "storage_root_ready": True,
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


def run_local_persistence_migration_supervisor(stop_event: threading.Event) -> None:
    ready, reason = migration_preflight()
    if not ready:
        # A missing/unwritable mount cannot persist a status file by definition. The
        # read endpoint derives this blocked state directly without touching the mount.
        if reason in {"storage_root_missing", "storage_root_unwritable", "storage_root_not_durable"}:
            return
        try:
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
        except OSError:
            pass
        return

    try:
        status_path, progress_path, lock_path, stdout_path, stderr_path = _paths()
    except OSError:
        return
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
        child_env = os.environ.copy()
        child_env["CIE_MIGRATION_POSTGRES_URL"] = _migration_source_url()
        source_disconnect_retries = 0
        stalled_progress_restarts = 0
        attempt = 0

        with stdout_path.open("ab") as stdout_file, stderr_path.open("ab") as stderr_file:
            while True:
                attempt += 1
                child = subprocess.Popen(
                    MIGRATION_COMMAND,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    env=child_env,
                    start_new_session=True,
                )
                _publish_status(
                    {
                        "state": "running",
                        "reason": None,
                        "started_at": started_at,
                        "child_pid": child.pid,
                        "attempt": attempt,
                        "source_disconnect_retries": source_disconnect_retries,
                        "stalled_progress_restarts": stalled_progress_restarts,
                        "observed_at": _now(),
                        "postgresql_authoritative": True,
                        "cutover_ready": False,
                        "paper_only": True,
                        "live_execution_authority": False,
                    }
                )
                progress = _read_json(progress_path)
                last_copy_marker = _monotonic_copy_progress_marker(progress)
                marker_changed_at = time.monotonic()
                stalled_progress: dict[str, Any] | None = None

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
                                "attempt": attempt,
                                "source_disconnect_retries": source_disconnect_retries,
                                "stalled_progress_restarts": stalled_progress_restarts,
                                "postgresql_authoritative": True,
                                "cutover_ready": False,
                                "paper_only": True,
                                "live_execution_authority": False,
                            }
                        )
                        return

                    progress = _read_json(progress_path)
                    copy_marker = _monotonic_copy_progress_marker(progress)
                    if copy_marker is None:
                        last_copy_marker = None
                        marker_changed_at = time.monotonic()
                        continue
                    if copy_marker != last_copy_marker:
                        last_copy_marker = copy_marker
                        marker_changed_at = time.monotonic()
                        continue
                    if time.monotonic() - marker_changed_at < MONOTONIC_COPY_STALL_SECONDS:
                        continue

                    stalled_progress = progress
                    _terminate_child(child)
                    break

                if stalled_progress is not None:
                    stalled_progress_restarts += 1
                    checkpoint_details = _stalled_checkpoint_details(stalled_progress)
                    if stalled_progress_restarts > MAX_STALLED_PROGRESS_RESTARTS:
                        _publish_status(
                            {
                                "state": "failed",
                                "reason": "stalled_monotonic_copy_retry_exhausted",
                                "started_at": started_at,
                                "completed_at": _now(),
                                "child_return_code": child.returncode,
                                "attempt": attempt,
                                "source_disconnect_retries": source_disconnect_retries,
                                "stalled_progress_restarts": stalled_progress_restarts,
                                **checkpoint_details,
                                "postgresql_authoritative": True,
                                "cutover_ready": False,
                                "paper_only": True,
                                "live_execution_authority": False,
                            }
                        )
                        return

                    _publish_status(
                        {
                            "state": "retry_wait",
                            "reason": "stalled_monotonic_copy_progress",
                            "started_at": started_at,
                            "child_return_code": child.returncode,
                            "attempt": attempt,
                            "source_disconnect_retries": source_disconnect_retries,
                            "stalled_progress_restarts": stalled_progress_restarts,
                            "retry_after_seconds": STALLED_PROGRESS_RETRY_DELAY_SECONDS,
                            **checkpoint_details,
                            "observed_at": _now(),
                            "postgresql_authoritative": True,
                            "cutover_ready": False,
                            "paper_only": True,
                            "live_execution_authority": False,
                        }
                    )
                    if stop_event.wait(STALLED_PROGRESS_RETRY_DELAY_SECONDS):
                        _publish_status(
                            {
                                "state": "interrupted",
                                "reason": "service_shutdown",
                                "started_at": started_at,
                                "completed_at": _now(),
                                "child_return_code": child.returncode,
                                "attempt": attempt,
                                "source_disconnect_retries": source_disconnect_retries,
                                "stalled_progress_restarts": stalled_progress_restarts,
                                **checkpoint_details,
                                "postgresql_authoritative": True,
                                "cutover_ready": False,
                                "paper_only": True,
                                "live_execution_authority": False,
                            }
                        )
                        return
                    continue

                return_code = int(child.returncode or 0)
                progress = _read_json(progress_path)
                verified = return_code == 0 and progress.get("state") == "verified"
                if verified:
                    _publish_status(
                        {
                            "state": "verified",
                            "reason": "snapshot_verification_complete",
                            "started_at": started_at,
                            "completed_at": _now(),
                            "child_return_code": return_code,
                            "attempt": attempt,
                            "source_disconnect_retries": source_disconnect_retries,
                            "stalled_progress_restarts": stalled_progress_restarts,
                            "progress_state": progress.get("state"),
                            "postgresql_authoritative": True,
                            "cutover_ready": False,
                            "paper_only": True,
                            "live_execution_authority": False,
                        }
                    )
                    return

                if (
                    return_code != 0
                    and _is_transient_source_disconnect(progress)
                    and source_disconnect_retries < MAX_TRANSIENT_SOURCE_RETRIES
                ):
                    delay = TRANSIENT_SOURCE_RETRY_DELAYS_SECONDS[
                        min(source_disconnect_retries, len(TRANSIENT_SOURCE_RETRY_DELAYS_SECONDS) - 1)
                    ]
                    source_disconnect_retries += 1
                    _publish_status(
                        {
                            "state": "retry_wait",
                            "reason": "transient_source_disconnect",
                            "started_at": started_at,
                            "child_return_code": return_code,
                            "attempt": attempt,
                            "source_disconnect_retries": source_disconnect_retries,
                            "stalled_progress_restarts": stalled_progress_restarts,
                            "retry_after_seconds": delay,
                            "progress_state": progress.get("state"),
                            "observed_at": _now(),
                            "postgresql_authoritative": True,
                            "cutover_ready": False,
                            "paper_only": True,
                            "live_execution_authority": False,
                        }
                    )
                    if stop_event.wait(delay):
                        _publish_status(
                            {
                                "state": "interrupted",
                                "reason": "service_shutdown",
                                "started_at": started_at,
                                "completed_at": _now(),
                                "child_return_code": return_code,
                                "attempt": attempt,
                                "source_disconnect_retries": source_disconnect_retries,
                                "stalled_progress_restarts": stalled_progress_restarts,
                                "postgresql_authoritative": True,
                                "cutover_ready": False,
                                "paper_only": True,
                                "live_execution_authority": False,
                            }
                        )
                        return
                    continue

                _publish_status(
                    {
                        "state": "failed",
                        "reason": "migration_child_failed",
                        "started_at": started_at,
                        "completed_at": _now(),
                        "child_return_code": return_code,
                        "attempt": attempt,
                        "source_disconnect_retries": source_disconnect_retries,
                        "stalled_progress_restarts": stalled_progress_restarts,
                        "progress_state": progress.get("state"),
                        "postgresql_authoritative": True,
                        "cutover_ready": False,
                        "paper_only": True,
                        "live_execution_authority": False,
                    }
                )
                return

__all__ = [
    "AUTO_MIGRATION_ENV",
    "MIGRATION_COMMAND",
    "migration_preflight",
    "migration_status_payload",
    "run_local_persistence_migration_supervisor",
]