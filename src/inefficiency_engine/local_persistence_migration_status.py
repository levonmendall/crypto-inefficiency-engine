from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from inefficiency_engine.local_persistence_migration_supervisor import (
    _read_json,
    migration_status_payload as _base_migration_status_payload,
)
from inefficiency_engine.local_storage import DEFAULT_PRODUCTION_STORAGE_ROOT


_FUNDING_CHECKPOINT_FIELDS = (
    "verified",
    "migration_mode",
    "verification_scope",
    "snapshot_phase",
    "snapshot_high_water_primary_key",
    "snapshot_high_water_captured",
    "snapshot_rows_copied",
    "snapshot_rows_verified",
    "last_primary_key",
    "last_progress_at",
    "source_transport_retries",
)
_GUARD_STATUS_FIELDS = (
    "state",
    "reason",
    "started_at",
    "observed_at",
    "attempt",
    "error_type",
    "error",
    "release_commit",
)
_PROGRESS_FILENAME = "postgres-import-progress.json"
_GUARD_STATUS_FILENAME = "migration-guard.json"
_GUARD_FALLBACK_STATUS_PATH = Path("/tmp/cie-migration-guard-fallback.json")


def _storage_root_path() -> Path:
    configured = os.getenv("CIE_STORAGE_ROOT", "").strip()
    return Path(configured or DEFAULT_PRODUCTION_STORAGE_ROOT).expanduser().resolve()


def _migration_path() -> Path:
    return _storage_root_path() / "migration"


def _funding_checkpoint(progress: dict[str, Any]) -> dict[str, object]:
    tables = progress.get("tables")
    table = tables.get("funding_quotes") if isinstance(tables, dict) else None
    funding = table if isinstance(table, dict) else {}
    return {field: funding.get(field) for field in _FUNDING_CHECKPOINT_FIELDS}


def _guard_projection(guard: dict[str, Any]) -> dict[str, object]:
    return {
        f"migration_guard_{field}": guard.get(field)
        for field in _GUARD_STATUS_FIELDS
    }


def _guard_fallback_projection(fallback: dict[str, Any]) -> dict[str, object]:
    return {
        f"migration_guard_fallback_{field}": fallback.get(field)
        for field in _GUARD_STATUS_FIELDS
    }


def _progress_path() -> Path:
    return _migration_path() / _PROGRESS_FILENAME


def _guard_status_path() -> Path:
    return _migration_path() / _GUARD_STATUS_FILENAME


def _guard_fallback_status_path() -> Path:
    return _GUARD_FALLBACK_STATUS_PATH


def _storage_diagnostics() -> dict[str, object]:
    root = _storage_root_path()
    exists = root.exists()
    is_dir = root.is_dir()
    writable = bool(exists and is_dir and os.access(root, os.W_OK | os.X_OK))
    total_bytes: int | None = None
    used_bytes: int | None = None
    free_bytes: int | None = None
    free_ratio: float | None = None
    probe = root if exists else root.parent
    try:
        usage = shutil.disk_usage(probe)
        total_bytes = int(usage.total)
        used_bytes = int(usage.used)
        free_bytes = int(usage.free)
        free_ratio = float(usage.free / usage.total) if usage.total else None
    except OSError:
        pass
    return {
        "migration_storage_root_exists": exists,
        "migration_storage_root_is_dir": is_dir,
        "migration_storage_root_writable": writable,
        "migration_storage_total_bytes": total_bytes,
        "migration_storage_used_bytes": used_bytes,
        "migration_storage_free_bytes": free_bytes,
        "migration_storage_free_ratio": free_ratio,
        "migration_guard_durable_status_present": _guard_status_path().is_file(),
        "migration_guard_fallback_status_present": _guard_fallback_status_path().is_file(),
    }


def migration_status_payload() -> dict[str, object]:
    """Return canonical migration status plus read-only migration diagnostics."""

    payload = _base_migration_status_payload()
    progress = _read_json(_progress_path())
    guard = _read_json(_guard_status_path())
    fallback = _read_json(_guard_fallback_status_path())
    payload["funding_quotes"] = _funding_checkpoint(progress)
    payload.update(_guard_projection(guard))
    payload.update(_guard_fallback_projection(fallback))
    payload.update(_storage_diagnostics())
    return payload
