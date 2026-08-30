from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from inefficiency_engine import local_persistence_migration_supervisor as base
from inefficiency_engine.market_history_inode_recovery import (
    InodeRecoveryPartitionedMarketHistory,
)


MAX_OPAQUE_CHILD_RESTARTS = 3
OPAQUE_CHILD_RETRY_DELAYS_SECONDS = (1.0, 3.0, 8.0)
STDERR_TAIL_BYTES = 16_384
MARKET_QUOTES_MIGRATION_MODE = "captured_primary_key_high_water"
INODE_RECOVERY_MIN_FREE = 131_072
INODE_RECOVERY_FREE_RATIO = 0.10
_STORAGE_EXHAUSTION_MARKERS = (
    "no space left on device",
    "errno 28",
    "disk quota exceeded",
)
_LAST_INODE_RECOVERY: dict[str, object] = {}

migration_preflight = base.migration_preflight


def _read_stderr_tail(path: Path, *, max_bytes: int = STDERR_TAIL_BYTES) -> str | None:
    """Read the newest bounded stderr tail, redact credentials, and keep the end."""

    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max(1, int(max_bytes))))
            text = handle.read(max(1, int(max_bytes))).decode("utf-8", errors="replace")
    except OSError:
        return None
    flattened = text.replace("\r", " ").replace("\n", " ").strip()
    if not flattened:
        return None
    redacted = base._URL_CREDENTIALS.sub(r"\1***@", flattened)
    return redacted[-600:]


def _is_storage_exhaustion(stderr_tail: str | None) -> bool:
    """Return true only when child stderr proves a filesystem-capacity failure."""

    if not stderr_tail:
        return False
    normalized = stderr_tail.lower()
    return any(marker in normalized for marker in _STORAGE_EXHAUSTION_MARKERS)


def _market_quotes_checkpoint_marker(progress: dict[str, Any]) -> tuple[object, ...] | None:
    """Return the restart-safe Stage 1 market_quotes checkpoint marker.

    The importer captures one finite integer primary-key high-water and then copies
    bounded batches constrained to ``id <= high_water`` while persisting
    ``last_primary_key`` after each committed Parquet append. An opaque process exit
    can therefore resume safely only when both durable boundaries are present.
    """

    if str(progress.get("current_table") or "") != "market_quotes":
        return None
    tables = progress.get("tables")
    table_report = tables.get("market_quotes") if isinstance(tables, dict) else None
    if not isinstance(table_report, dict):
        return None
    if table_report.get("verified") is True:
        return None
    if table_report.get("migration_mode") != MARKET_QUOTES_MIGRATION_MODE:
        return None
    high_water = table_report.get("high_water_primary_key")
    checkpoint = table_report.get("last_primary_key")
    if not isinstance(high_water, list) or not high_water:
        return None
    if not isinstance(checkpoint, list) or not checkpoint:
        return None
    return (
        "market_quotes",
        json.dumps(high_water, sort_keys=True, default=str),
        json.dumps(checkpoint, sort_keys=True, default=str),
        table_report.get("source_rows"),
        table_report.get("source_lineage_count"),
    )


def _checkpoint_details(progress: dict[str, Any]) -> dict[str, object]:
    current_table = str(progress.get("current_table") or "").strip()
    tables = progress.get("tables")
    table_report = tables.get(current_table) if isinstance(tables, dict) else None
    if not isinstance(table_report, dict):
        table_report = {}
    return {
        "checkpoint_current_table": current_table or None,
        "checkpoint_migration_mode": table_report.get("migration_mode"),
        "checkpoint_last_progress_at": table_report.get("last_progress_at"),
        "checkpoint_last_primary_key": table_report.get("last_primary_key"),
        "checkpoint_high_water_primary_key": table_report.get("high_water_primary_key"),
        "checkpoint_snapshot_rows_copied": table_report.get("snapshot_rows_copied"),
        "checkpoint_snapshot_high_water_primary_key": table_report.get(
            "snapshot_high_water_primary_key"
        ),
    }


def _inode_capacity() -> tuple[int | None, int | None]:
    try:
        filesystem = os.statvfs(base._configured_storage_root())
    except OSError:
        return None, None
    return int(filesystem.f_files), int(filesystem.f_ffree)


def _inode_recovery_target(inode_total: int | None) -> int:
    if inode_total is None or inode_total <= 0:
        return INODE_RECOVERY_MIN_FREE
    return max(INODE_RECOVERY_MIN_FREE, int(inode_total * INODE_RECOVERY_FREE_RATIO))


def _recover_market_history_inode_pressure(progress: dict[str, Any]) -> dict[str, object] | None:
    """Reclaim Parquet fragment inodes before relaunching a checkpointed migration."""

    global _LAST_INODE_RECOVERY
    if _market_quotes_checkpoint_marker(progress) is None:
        return None
    inode_total, inode_free = _inode_capacity()
    target = _inode_recovery_target(inode_total)
    if inode_free is None or inode_free >= target:
        return None

    _LAST_INODE_RECOVERY = {
        "state": "running",
        "started_at": base._now(),
        "inode_total_before": inode_total,
        "inode_free_before": inode_free,
        "target_free_inodes": target,
        **_checkpoint_details(progress),
    }
    try:
        history = InodeRecoveryPartitionedMarketHistory()
        result = history.compact_redundant_partitions(target_free_inodes=target)
    except Exception as exc:
        _LAST_INODE_RECOVERY.update(
            state="failed",
            completed_at=base._now(),
            error_type=type(exc).__name__,
            error=base._bounded_public_error(exc),
        )
        raise
    _LAST_INODE_RECOVERY.update(
        result,
        state="complete" if result.get("target_reached") else "insufficient",
        completed_at=base._now(),
    )
    return dict(_LAST_INODE_RECOVERY)


def _restart_safe_opaque_child_exit(
    status: dict[str, object],
    progress: dict[str, Any],
) -> bool:
    """Retry only an unexplained process exit with a proven market_quotes checkpoint.

    Explicit migration failures remain fail-closed. The only automatic recovery is
    for a nonzero child exit that did not publish semantic failure truth while the
    current table retains both its captured high-water and committed keyset checkpoint.
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
    return _market_quotes_checkpoint_marker(progress) is not None


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
    error_type: str = "OpaqueMigrationChildExit",
    error: object | None = None,
) -> None:
    public_error = error
    if public_error is None:
        public_error = (
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
        "error_type": error_type,
        "error": base._bounded_public_error(public_error),
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
        "checkpoint_migration_mode",
        "checkpoint_last_progress_at",
        "checkpoint_last_primary_key",
        "checkpoint_high_water_primary_key",
        "checkpoint_snapshot_rows_copied",
        "checkpoint_snapshot_high_water_primary_key",
        "retry_after_seconds",
    ):
        value = supervisor.get(field)
        if field in {"error", "child_stderr_tail"}:
            value = base._bounded_public_error(value)
        payload[f"supervisor_{field}"] = value
    for field, value in _LAST_INODE_RECOVERY.items():
        if field == "error":
            value = base._bounded_public_error(value)
        payload[f"supervisor_inode_compaction_{field}"] = value
    return payload


def run_local_persistence_migration_supervisor(stop_event: threading.Event) -> None:
    """Run Stage 1 with inode recovery and bounded opaque-child recovery."""

    try:
        _, progress_path, _, _, _ = base._paths()
    except OSError:
        return
    progress = base._read_json(progress_path)
    try:
        inode_recovery = _recover_market_history_inode_pressure(progress)
    except Exception as exc:
        try:
            _publish_repair_status(
                state="failed",
                reason="market_history_inode_compaction_failed",
                started_at=_LAST_INODE_RECOVERY.get("started_at") or base._now(),
                child_return_code=None,
                opaque_child_restarts=0,
                retry_after_seconds=None,
                progress=progress,
                stderr_tail=None,
                error_type=type(exc).__name__,
                error=exc,
            )
        except OSError:
            pass
        return
    if inode_recovery is not None and not inode_recovery.get("target_reached"):
        try:
            _publish_repair_status(
                state="failed",
                reason="market_history_inode_headroom_not_recovered",
                started_at=inode_recovery.get("started_at") or base._now(),
                child_return_code=None,
                opaque_child_restarts=0,
                retry_after_seconds=None,
                progress=progress,
                stderr_tail=None,
                error_type="InodeHeadroomUnavailable",
                error=(
                    "market history compaction could not restore required inode headroom: "
                    f"free={inode_recovery.get('inode_free_after')} "
                    f"target={inode_recovery.get('target_free_inodes')}"
                ),
            )
        except OSError:
            pass
        return

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

        if _is_storage_exhaustion(stderr_tail):
            _publish_repair_status(
                state="failed",
                reason="migration_storage_exhausted",
                started_at=started_at,
                child_return_code=return_code,
                opaque_child_restarts=opaque_child_restarts,
                retry_after_seconds=None,
                progress=progress,
                stderr_tail=stderr_tail,
                error_type="NoSpaceLeftOnDevice",
            )
            return

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
    "INODE_RECOVERY_FREE_RATIO",
    "INODE_RECOVERY_MIN_FREE",
    "MARKET_QUOTES_MIGRATION_MODE",
    "MAX_OPAQUE_CHILD_RESTARTS",
    "migration_preflight",
    "migration_status_payload",
    "run_local_persistence_migration_supervisor",
]
