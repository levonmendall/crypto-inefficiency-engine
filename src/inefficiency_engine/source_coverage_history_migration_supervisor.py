from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.request import urlopen

from inefficiency_engine.source_coverage_history_migration_child import (
    MIGRATION_INCOMPLETE_EXIT_CODE,
    MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE,
)


MIGRATION_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.source_coverage_history_migration_child",
]
DIAGNOSTIC_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.source_history_supervisor_diagnostic_child",
]
MIGRATION_EXECUTOR_DEADLINE_SECONDS = 30.0
MIGRATION_PROGRESS_INTERVAL_SECONDS = 1.0
MIGRATION_FAILURE_RETRY_SECONDS = 10.0
DIAGNOSTIC_EXECUTOR_DEADLINE_SECONDS = 5.0
API_BIND_POLL_SECONDS = 2.0
API_BIND_TIMEOUT_SECONDS = 2.0


def _api_is_bound(port: str | int) -> bool:
    try:
        with urlopen(
            f"http://127.0.0.1:{port}/health",
            timeout=API_BIND_TIMEOUT_SECONDS,
        ) as response:
            return int(getattr(response, "status", 200)) == 200
    except Exception:
        return False


def _terminate(child: subprocess.Popen[bytes], *, grace_seconds: float = 3.0) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    deadline = time.monotonic() + max(0.1, grace_seconds)
    while time.monotonic() < deadline:
        if child.poll() is not None:
            return
        time.sleep(0.2)
    if child.poll() is None:
        child.kill()


def _run_bounded_child(
    stop_event: threading.Event,
    *,
    deadline_seconds: float = MIGRATION_EXECUTOR_DEADLINE_SECONDS,
) -> tuple[int | None, bool]:
    child = subprocess.Popen(MIGRATION_COMMAND)
    started = time.monotonic()
    timed_out = False
    while not stop_event.is_set() and child.poll() is None:
        if time.monotonic() - started >= deadline_seconds:
            timed_out = True
            _terminate(child)
            break
        stop_event.wait(0.25)
    if stop_event.is_set():
        _terminate(child)
    return child.poll(), timed_out


def _run_diagnostic_child(payload: dict[str, object]) -> bool:
    """Publish supervisor truth without ever letting PostgreSQL block this parent."""

    try:
        completed = subprocess.run(
            [*DIAGNOSTIC_COMMAND, json.dumps(payload, separators=(",", ":"))],
            capture_output=True,
            text=True,
            check=False,
            timeout=DIAGNOSTIC_EXECUTOR_DEADLINE_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(
            "source history diagnostic child exceeded its bounded deadline; continuing",
            flush=True,
        )
        return False
    if completed.returncode != 0:
        print(
            "source history diagnostic child exited "
            f"code={completed.returncode}; continuing",
            flush=True,
        )
        return False
    return True


def _record_supervisor_failure(
    _store=None,
    *,
    attempt_number: int,
    started_at: datetime,
    stage: str,
    error_type: str,
    return_code: int | None,
    timed_out: bool,
    message: str | None = None,
    preserve_fresh_child_failure: bool = False,
) -> None:
    """Publish terminal process truth through a bounded disposable diagnostic child."""

    _run_diagnostic_child(
        {
            "attempt_number": int(attempt_number),
            "attempt_started_at": started_at.astimezone(timezone.utc).isoformat(),
            "stage": stage,
            "error_type": error_type,
            "child_return_code": return_code,
            "child_timed_out": bool(timed_out),
            "message": message,
            "preserve_fresh_child_failure": bool(preserve_fresh_child_failure),
            "executor_deadline_seconds": MIGRATION_EXECUTOR_DEADLINE_SECONDS,
            "retry_seconds": MIGRATION_FAILURE_RETRY_SECONDS,
        }
    )


def run_source_coverage_history_migration_supervisor(stop_event: threading.Event) -> None:
    """Drain the canonical source snapshot archive outside live-source deadlines.

    Migration remains finite, database-only and checkpointed in disposable children.
    The long-lived parent itself performs no durable-store open/read/write. Even failure
    telemetry is delegated to a separate five-second diagnostic process, so PostgreSQL
    connection pressure can never freeze migration retries or leave this supervisor
    permanently stuck behind its own observability path.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    attempt_number = 0
    while not stop_event.is_set():
        attempt_number += 1
        started_at = datetime.now(timezone.utc)
        try:
            return_code, timed_out = _run_bounded_child(stop_event)
        except Exception as exc:
            _record_supervisor_failure(
                attempt_number=attempt_number,
                started_at=started_at,
                stage="canonical_history_archive_migration_supervisor_exception",
                error_type=type(exc).__name__,
                return_code=None,
                timed_out=False,
                message=str(exc)[:1000],
            )
            print(
                "source coverage history migration supervisor failed to run child; "
                f"retrying: {type(exc).__name__}: {exc}",
                flush=True,
            )
            stop_event.wait(MIGRATION_FAILURE_RETRY_SECONDS)
            continue

        if stop_event.is_set():
            return
        if timed_out:
            _record_supervisor_failure(
                attempt_number=attempt_number,
                started_at=started_at,
                stage="canonical_history_archive_migration_child_timed_out",
                error_type="SourceCoverageHistoryMigrationChildDeadlineExceeded",
                return_code=return_code,
                timed_out=True,
                message=(
                    "source coverage history migration child exceeded its bounded "
                    f"{MIGRATION_EXECUTOR_DEADLINE_SECONDS:.1f}s deadline"
                ),
            )
            print(
                "source coverage history migration child timed out; retrying from checkpoint",
                flush=True,
            )
            stop_event.wait(MIGRATION_FAILURE_RETRY_SECONDS)
            continue
        if return_code == 0:
            print("canonical source coverage history archive migration complete", flush=True)
            return
        if return_code == MIGRATION_INCOMPLETE_EXIT_CODE:
            stop_event.wait(MIGRATION_PROGRESS_INTERVAL_SECONDS)
            continue
        if return_code == MIGRATION_PREREQUISITE_NOT_READY_EXIT_CODE:
            print(
                "source coverage history migration waiting for planner-usable "
                "worker-heartbeat read index",
                flush=True,
            )
            stop_event.wait(MIGRATION_FAILURE_RETRY_SECONDS)
            continue

        terminal_error_type = (
            "SourceCoverageHistoryMigrationChildTerminatedBySignal"
            if return_code is not None and return_code < 0
            else "SourceCoverageHistoryMigrationChildExitedNonZero"
        )
        _record_supervisor_failure(
            attempt_number=attempt_number,
            started_at=started_at,
            stage="canonical_history_archive_migration_child_failed",
            error_type=terminal_error_type,
            return_code=return_code,
            timed_out=False,
            message=f"source coverage history migration child exited code={return_code}",
            preserve_fresh_child_failure=True,
        )
        print(
            f"source coverage history migration child exited code={return_code}; retrying",
            flush=True,
        )
        stop_event.wait(MIGRATION_FAILURE_RETRY_SECONDS)


__all__ = [
    "DIAGNOSTIC_COMMAND",
    "DIAGNOSTIC_EXECUTOR_DEADLINE_SECONDS",
    "MIGRATION_COMMAND",
    "MIGRATION_EXECUTOR_DEADLINE_SECONDS",
    "_record_supervisor_failure",
    "_run_diagnostic_child",
    "run_source_coverage_history_migration_supervisor",
]
