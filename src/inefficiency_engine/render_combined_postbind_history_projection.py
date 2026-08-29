from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

from inefficiency_engine import render_combined_postbind_lane_repair as base
from inefficiency_engine.durable_lane_history_projection_supervisor import (
    run_durable_lane_history_projection_supervisor,
)
from inefficiency_engine.local_persistence_migration_supervisor import (
    migration_preflight,
    migration_status_payload,
    run_local_persistence_migration_supervisor,
)
from inefficiency_engine.local_storage import local_storage_paths
from inefficiency_engine.startup_database_recovery import (
    install_startup_database_recovery,
)


MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS = 2.0
MIGRATION_GUARD_EXCEPTION_RETRY_SECONDS = 2.0
MIGRATION_GUARD_STATUS_FILENAME = "migration-guard.json"
MIGRATION_GUARD_FALLBACK_STATUS_PATH = Path("/tmp/cie-migration-guard-fallback.json")
_TERMINAL_MIGRATION_STATES = {"failed", "interrupted", "verified"}
_URL_CREDENTIALS = re.compile(
    r"(?i)\b(postgres(?:ql)?(?:\+psycopg)?://)([^@\s]+)@"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _guard_status_path():
    return local_storage_paths().migration / MIGRATION_GUARD_STATUS_FILENAME


def _bounded_guard_error(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return None
    text = _URL_CREDENTIALS.sub(r"\1***@", text)
    return text[:600]


def _guard_status_payload(
    *,
    state: str,
    reason: str | None,
    started_at: str,
    attempt: int,
    error_type: str | None = None,
    error: object | None = None,
) -> dict[str, object]:
    return {
        "state": state,
        "reason": reason,
        "started_at": started_at,
        "observed_at": _now(),
        "attempt": int(attempt),
        "error_type": error_type,
        "error": _bounded_guard_error(error),
        "release_commit": os.getenv("RENDER_GIT_COMMIT", "").strip() or None,
        "postgresql_authoritative": True,
        "cutover_ready": False,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, path)


def _publish_migration_guard_status(
    *,
    state: str,
    reason: str | None,
    started_at: str,
    attempt: int,
    error_type: str | None = None,
    error: object | None = None,
) -> None:
    """Persist outer migration-guard truth independently of supervisor ownership."""

    _atomic_write_json(
        _guard_status_path(),
        _guard_status_payload(
            state=state,
            reason=reason,
            started_at=started_at,
            attempt=attempt,
            error_type=error_type,
            error=error,
        ),
    )


def _publish_migration_guard_fallback_status(
    *,
    state: str,
    reason: str,
    started_at: str,
    attempt: int,
    error_type: str | None = None,
    error: object | None = None,
) -> None:
    """Persist current-process guard failures outside the durable migration disk.

    This fallback is diagnostic only. It cannot authorize migration, cutover, allocation,
    or live execution. Its purpose is to distinguish "guard never ran" from "guard ran but
    could not write /var/data/cie", including a full or unwritable persistent disk.
    """

    payload = _guard_status_payload(
        state=state,
        reason=reason,
        started_at=started_at,
        attempt=attempt,
        error_type=error_type,
        error=error,
    )
    payload["diagnostic_only"] = True
    _atomic_write_json(MIGRATION_GUARD_FALLBACK_STATUS_PATH, payload)


def _clear_migration_guard_fallback_status() -> None:
    try:
        MIGRATION_GUARD_FALLBACK_STATUS_PATH.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def _record_guard_publish_failure(
    *,
    started_at: str,
    attempt: int,
    exc: BaseException,
    reason: str,
) -> None:
    try:
        _publish_migration_guard_fallback_status(
            state="durable_status_write_failed",
            reason=reason,
            started_at=started_at,
            attempt=attempt,
            error_type=type(exc).__name__,
            error=exc,
        )
    except Exception:
        # The status endpoint also probes the durable root directly, so even a failure
        # of both files remains distinguishable from healthy storage.
        return


def _publish_synchronous_migration_guard_startup() -> str:
    """Publish guard startup from the main thread before the daemon thread can start."""

    started_at = _now()
    try:
        _publish_migration_guard_status(
            state="main_startup",
            reason="before_guard_thread_start",
            started_at=started_at,
            attempt=0,
        )
    except Exception as exc:
        _record_guard_publish_failure(
            started_at=started_at,
            attempt=0,
            exc=exc,
            reason="main_startup_durable_status_write_failed",
        )
    else:
        _clear_migration_guard_fallback_status()
    return started_at


def _safe_publish_migration_guard_status(**kwargs: object) -> None:
    """Publish guard telemetry without allowing telemetry I/O to kill supervision."""

    try:
        _publish_migration_guard_status(**kwargs)
    except Exception as exc:
        _record_guard_publish_failure(
            started_at=str(kwargs.get("started_at") or _now()),
            attempt=int(kwargs.get("attempt") or 0),
            exc=exc,
            reason="background_durable_status_write_failed",
        )


def _migration_is_terminal(status: dict[str, object]) -> bool:
    state = str(status.get("state") or "")
    progress_state = str(status.get("progress_state") or "")
    return state in _TERMINAL_MIGRATION_STATES or progress_state in {"failed", "verified"}


def _run_local_persistence_migration_with_deploy_handoff(
    stop_event: threading.Event,
) -> None:
    """Keep a fresh deploy eligible to take over an in-flight Stage 1 migration."""

    guard_started_at = _now()
    guard_attempt = 0
    _safe_publish_migration_guard_status(
        state="started",
        reason=None,
        started_at=guard_started_at,
        attempt=guard_attempt,
    )

    while not stop_event.is_set():
        guard_attempt += 1
        _safe_publish_migration_guard_status(
            state="running",
            reason="supervisor_entry",
            started_at=guard_started_at,
            attempt=guard_attempt,
        )
        try:
            ready, preflight_reason = migration_preflight()
            run_local_persistence_migration_supervisor(stop_event)
            if stop_event.is_set():
                _safe_publish_migration_guard_status(
                    state="interrupted",
                    reason="service_shutdown",
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return
            if not ready:
                _safe_publish_migration_guard_status(
                    state="blocked",
                    reason=preflight_reason,
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return

            status = migration_status_payload()
            state = str(status.get("state") or "")
            progress_state = str(status.get("progress_state") or "")
            supervisor_reason = str(status.get("supervisor_reason") or "")

            if _migration_is_terminal(status):
                _safe_publish_migration_guard_status(
                    state="terminal",
                    reason=state or progress_state or "migration_terminal",
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return
            if state == "blocked" and supervisor_reason != "another_importer_holds_lock":
                _safe_publish_migration_guard_status(
                    state="blocked",
                    reason=supervisor_reason or "migration_blocked",
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return

            _safe_publish_migration_guard_status(
                state="retry_wait",
                reason="deploy_handoff_incomplete",
                started_at=guard_started_at,
                attempt=guard_attempt,
            )
            if stop_event.wait(MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS):
                _safe_publish_migration_guard_status(
                    state="interrupted",
                    reason="service_shutdown",
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return
        except Exception as exc:
            status: dict[str, object] = {}
            try:
                status = migration_status_payload()
            except Exception:
                pass
            terminal = _migration_is_terminal(status)
            postgresql_authoritative = status.get("postgresql_authoritative")
            can_retry = not terminal and postgresql_authoritative is not False
            _safe_publish_migration_guard_status(
                state="error_retry_wait" if can_retry else "failed",
                reason="migration_guard_exception",
                started_at=guard_started_at,
                attempt=guard_attempt,
                error_type=type(exc).__name__,
                error=exc,
            )
            if not can_retry:
                return
            if stop_event.wait(MIGRATION_GUARD_EXCEPTION_RETRY_SECONDS):
                _safe_publish_migration_guard_status(
                    state="interrupted",
                    reason="service_shutdown",
                    started_at=guard_started_at,
                    attempt=guard_attempt,
                )
                return


def main() -> int:
    """Run the canonical combined service plus independent durable background guards."""

    install_startup_database_recovery(base.base)

    stop_event = threading.Event()
    history_projection_guard = threading.Thread(
        target=run_durable_lane_history_projection_supervisor,
        args=(stop_event,),
        name="durable-lane-history-projection-supervisor",
        daemon=True,
    )
    migration_guard = threading.Thread(
        target=_run_local_persistence_migration_with_deploy_handoff,
        args=(stop_event,),
        name="local-persistence-migration-supervisor",
        daemon=True,
    )

    # This synchronous main-thread marker is intentionally before either background
    # guard starts. A fresh deployment must therefore expose one of two truths:
    # durable guard startup evidence, or a current-process fallback write failure.
    _publish_synchronous_migration_guard_startup()

    history_projection_guard.start()
    migration_guard.start()
    try:
        return base.main()
    finally:
        stop_event.set()
        history_projection_guard.join(timeout=10.0)
        migration_guard.join(timeout=10.0)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
