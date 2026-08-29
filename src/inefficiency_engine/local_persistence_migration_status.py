from __future__ import annotations

from typing import Any

from inefficiency_engine.local_persistence_migration_supervisor import (
    _read_json,
    migration_status_payload as _base_migration_status_payload,
)
from inefficiency_engine.local_storage import local_storage_paths


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
_PROGRESS_FILENAME = "postgres-import-progress.json"


def _funding_checkpoint(progress: dict[str, Any]) -> dict[str, object]:
    tables = progress.get("tables")
    table = tables.get("funding_quotes") if isinstance(tables, dict) else None
    funding = table if isinstance(table, dict) else {}
    return {field: funding.get(field) for field in _FUNDING_CHECKPOINT_FIELDS}


def _progress_path():
    return local_storage_paths().migration / _PROGRESS_FILENAME


def migration_status_payload() -> dict[str, object]:
    """Return canonical migration status plus the durable funding-copy checkpoint.

    Funding progress is projected from the same atomic Stage 1 progress file used by
    the supervisor. This diagnostic-only read cannot mutate migration state, retry
    policy, cutover readiness, allocation authority, or live-execution authority.
    """

    payload = _base_migration_status_payload()
    progress = _read_json(_progress_path())
    payload["funding_quotes"] = _funding_checkpoint(progress)
    return payload
