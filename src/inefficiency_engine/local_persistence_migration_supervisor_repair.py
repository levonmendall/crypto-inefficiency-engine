from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

from inefficiency_engine import local_persistence_migration_supervisor as base


MAX_OPAQUE_CHILD_RESTARTS = 3
OPAQUE_CHILD_RETRY_DELAYS_SECONDS = (1.0, 3.0, 8.0)
STDERR_TAIL_BYTES = 16_384

migration_preflight = base.migration_preflight


def _read_stderr_tail(path: Path, *, max_bytes: int = STDERR_TAIL_BYTES) -> str | None:
    """Read only a bounded tail of the child stderr log and redact credentials."""

    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max(1, int(max_bytes))))
            text = handle.read(max(1, int(max_bytes))).decode("utf-8", errors="replace")
    except OSError:
        return None
    return base._bounded_public_error(text)


def _checkpoint_details(progress: dict[str, Any]) -> dict[str, object]:
    current_table = str(progress.get("current_table") or "").strip()
    tables = progress.get("tables")
    table_report = tables.get(current_table) if isinstance(tables, dict) else None
    if not isinstance(table_report, dict):
        table_report = {}
    return {
        "checkpoint_current_table": current_table or None,
        "checkpoint_last_progress_at": table_report.get("last_progress_at"),
        "checkpoint_last_primary_key": table_report.get("last_primary_key"),
        "checkpoint_snapshot_rows_copied": table_report.get("snapshot_rows_copied"),
        "checkpoint_snapshot_high_water_primary_key": table_report.get(
            "snapshot_high_water_primary_key"
        ),
    }


def _restart_safe_opaque_child_exit(
    status: dict[str, object],
    progress: dict[str, Any],
) -> bool:
    """Retry only an unexplained process exit with a proven resumable checkpoint.

    Explicit migration failures remain fail-closed.  The only automatic recovery is
    for a nonzero child exit that did not publish semantic failure truth and whose
    current table is still in the bounded monotonic snapshot-copy phase.
    """

    if status.get("state") != "failed":
        return False
    if status.get("supervisor_reason") != "migration_child_failed":
        return False
    try:
        return_code = int(status.get("child_return_code") or 0)
    except (TypeError, ValueError):
        return False
    if return_code == 0:
        return False
    if str(progress.get("state") or "") in {"failed", "verified"}:
        return False
    if progress.get("error_type") not in (None, "") or progress.get("error") not in (None, ""):
        return False
    return base._monotonic_copy_progress_marker(progress) is not None


def _publish_repair_status(
    *,
    state: str,
    reason: str,
    started_at: object,
    child_return_code: object,
    opaque_child_restarts: int,
    retry_after_seconds: float | None,
    progress: dict[str, Any],
    stderr_tail: str | None,
) -> None:
    error = (
        f"migration child exited code={child_return_code} without durable progress error"
        + (f"; stderr_tail={stderr_tail}" if stderr_tail else "")
    )
    payload: dict[str, object] = {
        "state": state,
        "reason": reason,
        "started_at": started_at,
        "observed_at": base._now(),
        "child_return_code": child_return_code,
        "opaque_child_restarts": int(opaque_child_restarts),
        "error_type": "OpaqueMigrationChildExit",
        "error": base._bounded_public_error(error),
        "child_stderr_tail": stderr_tail,
        "progress_state": progress.get("state"),
        **_checkpoint_details(progress),
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
    if retry_after_seconds is not None:
        payload["retry_after_seconds"] = float(retry_after_seconds)
    if state in {"failed", "interrupted"}:
        payload["completed_at"] = base._now()
    base._publish_status(payload)


def migration_status_payload() -> dict[str, object]:
    """Project base migration truth plus process-level terminal diagnostics."""

    payload = base.migration_status_payload()
    try:
        status_path, _, _, _, _ = base._paths()
        supervisor = base._read_json(status_path)
    except OSError:
        supervisor = {}
    for field in (
        "opaque_child_restarts",
        "error_type",
        "error",
        "child_stderr_tail",
        "checkpoint_current_table",
        "checkpoint_last_progress_at",
        "checkpoint_last_primary_key",
        "checkpoint_snapshot_rows_copied",
        "checkpoint_snapshot_high_water_primary_key",
        "retry_after_seconds",
    ):
        value = supervisor.get(field)
        if field in {"error", "child_stderr_tail"}:
            value = base._bounded_public_error(value)
        payload[f"supervisor_{field}"] = value
    return payload


def run_local_persistence_migration_supervisor(stop_event: threading.Event) -> None:
    """Run Stage 1 with bounded recovery for opaque checkpoint-safe child exits."""

    opaque_child_restarts = 0
    while not stop_event.is_set():
        base.run_local_persistence_migration_supervisor(stop_event)
        if stop_event.is_set():
            return

        status = base.migration_status_payload()
        try:
            _, progress_path, _, _, stderr_path = base._paths()
        except OSError:
            return
        progress = base._read_json(progress_path)
        if not _restart_safe_opaque_child_exit(status, progress):
            return

        stderr_tail = _read_stderr_tail(stderr_path)
        started_at = status.get("supervisor_started_at") or base._now()
        return_code = status.get("child_return_code")

        if opaque_child_restarts >= MAX_OPAQUE_CHILD_RESTARTS:
            _publish_repair_status(
                state="failed",
                reason="opaque_checkpoint_child_retry_exhausted",
                started_at=started_at,
                child_return_code=return_code,
                opaque_child_restarts=opaque_child_restarts,
                retry_after_seconds=None,
                progress=progress,
                stderr_tail=stderr_tail,
            )
            return

        delay = OPAQUE_CHILD_RETRY_DELAYS_SECONDS[
            min(opaque_child_restarts, len(OPAQUE_CHILD_RETRY_DELAYS_SECONDS) - 1)
        ]
        opaque_child_restarts += 1
        _publish_repair_status(
            state="retry_wait",
            reason="opaque_checkpoint_child_exit",
            started_at=started_at,
            child_return_code=return_code,
            opaque_child_restarts=opaque_child_restarts,
            retry_after_seconds=delay,
            progress=progress,
            stderr_tail=stderr_tail,
        )
        if stop_event.wait(delay):
            _publish_repair_status(
                state="interrupted",
                reason="service_shutdown",
                started_at=started_at,
                child_return_code=return_code,
                opaque_child_restarts=opaque_child_restarts,
                retry_after_seconds=None,
                progress=progress,
                stderr_tail=stderr_tail,
            )
            return


__all__ = [
    "MAX_OPAQUE_CHILD_RESTARTS",
    "migration_preflight",
    "migration_status_payload",
    "run_local_persistence_migration_supervisor",
]
