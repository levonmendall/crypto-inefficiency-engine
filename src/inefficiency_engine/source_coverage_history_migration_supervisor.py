from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.request import urlopen

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.source_coverage_history_migration_child import (
    MIGRATION_INCOMPLETE_EXIT_CODE,
    SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID,
)


MIGRATION_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.source_coverage_history_migration_child",
]
MIGRATION_EXECUTOR_DEADLINE_SECONDS = 30.0
MIGRATION_PROGRESS_INTERVAL_SECONDS = 1.0
MIGRATION_FAILURE_RETRY_SECONDS = 10.0
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


def _supervisor_store() -> Any | None:
    """Open durable persistence fail-soft after API bind for supervisor diagnostics."""

    try:
        settings = Settings.from_env()
        return build_evidence_store(settings.evidence_db_path)
    except Exception:
        return None


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


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _fresh_concrete_child_failure(store: Any, *, started_at: datetime) -> bool:
    """Preserve a richer child-published exception from the current attempt."""

    try:
        heartbeat = store.latest_worker_heartbeat(
            SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID
        )
    except Exception:
        return False
    if heartbeat is None or str(heartbeat.state or "") not in {"degraded", "error"}:
        return False
    try:
        observed_at = _utc(heartbeat.observed_at)
    except Exception:
        return False
    return observed_at >= _utc(started_at)


def _termination_signal(return_code: int | None) -> tuple[int | None, str | None]:
    if return_code is None or int(return_code) >= 0:
        return None, None
    number = abs(int(return_code))
    try:
        name = signal.Signals(number).name
    except (ValueError, OSError):
        name = None
    return number, name


def _record_supervisor_failure(
    store: Any | None,
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
    """Publish current terminal process truth without erasing richer child errors."""

    if store is None:
        return
    if preserve_fresh_child_failure and _fresh_concrete_child_failure(
        store,
        started_at=started_at,
    ):
        return

    signal_number, signal_name = _termination_signal(return_code)
    try:
        store.record_worker_heartbeat(
            worker_id=SOURCE_COVERAGE_HISTORY_MIGRATION_WORKER_ID,
            state="degraded",
            error_type=error_type,
            detail={
                "stage": stage,
                "message": message,
                "supervisor_observation": True,
                "supervisor_executes_migration": False,
                "attempt_number": int(attempt_number),
                "attempt_started_at": _utc(started_at).isoformat(),
                "executor_deadline_seconds": MIGRATION_EXECUTOR_DEADLINE_SECONDS,
                "child_return_code": return_code,
                "child_timed_out": bool(timed_out),
                "process_termination_observed_by_supervisor": return_code is not None,
                "termination_signal_number": signal_number,
                "termination_signal": signal_name,
                "possible_oom_or_external_kill": signal_number == int(signal.SIGKILL),
                "oom_kill_proven": False,
                "retrying": True,
                "retry_seconds": MIGRATION_FAILURE_RETRY_SECONDS,
                "migration_owner": "independent-bounded-history-child",
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "candidate_level_history_synthesized": False,
                "historical_counts_as_forward": False,
                "qualification_thresholds_unchanged": True,
                "qualification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        # Diagnostics are fail-soft and can never become migration/certification authority.
        pass


def run_source_coverage_history_migration_supervisor(stop_event: threading.Event) -> None:
    """Drain the canonical source snapshot archive outside live-source deadlines.

    The migration is finite, database-only and checkpointed. Each disposable child owns
    only a small batch and exits completely. This prevents the live 45-second source
    snapshot executor from repeatedly consuming its budget before archive migration can
    advance, while keeping migration off the API request path. Parent-observed child
    timeouts/terminations are durably published so an old child exception cannot remain
    the apparent current production failure while the supervisor is actively retrying.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    store = _supervisor_store()
    attempt_number = 0
    while not stop_event.is_set():
        attempt_number += 1
        started_at = datetime.now(timezone.utc)
        try:
            return_code, timed_out = _run_bounded_child(stop_event)
        except Exception as exc:
            _record_supervisor_failure(
                store,
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
                store,
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

        signal_number, _ = _termination_signal(return_code)
        terminal_error_type = (
            "SourceCoverageHistoryMigrationChildTerminatedBySignal"
            if signal_number is not None
            else "SourceCoverageHistoryMigrationChildExitedNonZero"
        )
        _record_supervisor_failure(
            store,
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
    "MIGRATION_COMMAND",
    "MIGRATION_EXECUTOR_DEADLINE_SECONDS",
    "_record_supervisor_failure",
    "run_source_coverage_history_migration_supervisor",
]
