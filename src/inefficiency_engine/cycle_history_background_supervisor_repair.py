from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time

from inefficiency_engine import cycle_history_background_supervisor as base
from inefficiency_engine.cycle_history_background_backfill_repair import (
    INDEX_NOT_READY_EXIT_CODE,
)
from inefficiency_engine.cycle_history_index_maintenance_child import (
    WORKER_ID as INDEX_WORKER_ID,
)
from inefficiency_engine.instance_memory import instance_memory_snapshot


INDEX_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_index_maintenance_child",
]
INDEX_DIAGNOSTIC_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_index_supervisor_probe",
]
BACKFILL_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_background_backfill_repair",
]
# Production pg_stat_progress_create_index proved the exact concurrent index was healthy
# and still scanning the large market_quotes table well past the pace compatible with a
# ten-minute deadline. Give the sole DDL owner one bounded hour plus a one-minute process
# margin for post-DDL catalog verification and clean shutdown. No history/backfill
# deadline, shared runtime-index timeout, or certification rule is changed here.
INDEX_EXECUTOR_DEADLINE_SECONDS = 3660.0
INDEX_RETRY_SECONDS = 30.0
INDEX_PROGRESS_HEARTBEAT_SECONDS = 15.0
INDEX_DIAGNOSTIC_DEADLINE_SECONDS = 8.0
INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD = 3
INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS = 120.0


def _run_bounded_child(
    command: list[str],
    *,
    deadline_seconds: float,
    stop_event: threading.Event,
) -> tuple[int | None, bool]:
    print(f"starting disposable child: {' '.join(command)}", flush=True)
    child = subprocess.Popen(command)
    started = time.monotonic()
    timed_out = False
    while not stop_event.is_set() and child.poll() is None:
        if time.monotonic() - started >= deadline_seconds:
            timed_out = True
            base._terminate(child)
            break
        stop_event.wait(0.5)

    if stop_event.is_set():
        base._terminate(child)
        return child.poll(), timed_out
    return child.poll(), timed_out


def _run_index_diagnostic(payload: dict[str, object]) -> dict[str, object] | None:
    """Run every supervisor DB observation outside the long-lived parent process."""

    try:
        completed = subprocess.run(
            [*INDEX_DIAGNOSTIC_COMMAND, json.dumps(payload, separators=(",", ":"))],
            capture_output=True,
            text=True,
            check=False,
            timeout=INDEX_DIAGNOSTIC_DEADLINE_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(
            "cycle-history index diagnostic child exceeded its bounded deadline; "
            "continuing to supervise DDL",
            flush=True,
        )
        return None
    if completed.returncode != 0:
        print(
            "cycle-history index diagnostic child exited "
            f"code={completed.returncode}; continuing to supervise DDL",
            flush=True,
        )
        return None
    try:
        value = json.loads(completed.stdout)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _safe_index_status() -> dict[str, object] | None:
    result = _run_index_diagnostic({"action": "status"})
    if not isinstance(result, dict):
        return None
    status = result.get("index_status")
    return dict(status) if isinstance(status, dict) else None


def _record_index_supervisor_heartbeat(
    _store=None,
    *,
    state: str,
    stage: str,
    error_type: str | None = None,
    detail: dict[str, object] | None = None,
    include_status_progress: bool = False,
) -> None:
    """Publish supervisor truth through an eight-second disposable DB process."""

    _run_index_diagnostic(
        {
            "action": "record",
            "state": state,
            "stage": stage,
            "error_type": error_type,
            "detail": dict(detail or {}),
            "include_status_progress": bool(include_status_progress),
        }
    )


def _index_child_exit_diagnostics(
    return_code: int | None,
    *,
    timed_out: bool,
) -> tuple[str | None, dict[str, object]]:
    """Classify what the parent can prove about a vanished disposable child."""

    detail: dict[str, object] = {
        "child_return_code": return_code,
        "child_timed_out": bool(timed_out),
        "process_termination_observed_by_supervisor": True,
    }
    if timed_out:
        return "IndexChildDeadlineExceeded", detail
    if return_code is None:
        return "IndexChildTerminationUnknown", detail
    if return_code < 0:
        signal_number = -int(return_code)
        try:
            signal_name = signal.Signals(signal_number).name
        except ValueError:
            signal_name = f"SIGNAL_{signal_number}"
        detail.update(
            {
                "termination_signal_number": signal_number,
                "termination_signal": signal_name,
                "possible_oom_or_external_kill": signal_name == "SIGKILL",
                "oom_kill_proven": False,
            }
        )
        return "IndexChildTerminatedBySignal", detail
    if return_code != 0:
        return "IndexChildExitedNonZero", detail
    return None, detail


def _run_index_child(
    *,
    stop_event: threading.Event,
) -> tuple[int | None, bool]:
    """Run the sole DDL owner while disposable probes publish bounded liveness."""

    print(f"starting disposable child: {' '.join(INDEX_COMMAND)}", flush=True)
    child = subprocess.Popen(INDEX_COMMAND)
    started = time.monotonic()
    next_progress = started + INDEX_PROGRESS_HEARTBEAT_SECONDS
    timed_out = False

    while not stop_event.is_set() and child.poll() is None:
        now = time.monotonic()
        runtime_seconds = max(0.0, now - started)
        if runtime_seconds >= INDEX_EXECUTOR_DEADLINE_SECONDS:
            timed_out = True
            base._terminate(child)
            break
        if now >= next_progress:
            _record_index_supervisor_heartbeat(
                state="running",
                stage="cycle_history_index_supervisor_observing",
                detail={
                    "child_pid": child.pid,
                    "child_runtime_seconds": runtime_seconds,
                    "executor_deadline_seconds": INDEX_EXECUTOR_DEADLINE_SECONDS,
                    "diagnostic_process_deadline_seconds": (
                        INDEX_DIAGNOSTIC_DEADLINE_SECONDS
                    ),
                },
                include_status_progress=True,
            )
            next_progress = time.monotonic() + INDEX_PROGRESS_HEARTBEAT_SECONDS
        stop_event.wait(0.5)

    if stop_event.is_set():
        base._terminate(child)
    return child.poll(), timed_out


def run_cycle_history_background_supervisor(stop_event: threading.Event) -> None:
    """Verify the exact query index before any raw cycle-history reconstruction.

    The exact-index child remains the sole DDL owner. All parent-side catalog/progress
    observation and heartbeat persistence now occur in eight-second disposable probe
    processes, so PostgreSQL connection pressure cannot freeze this long-lived supervisor
    or consume the one-hour DDL child's lifetime. The second child owns only the existing
    checkpointed 180-day history slice.
    """

    port = base.os.getenv("PORT", "10000")
    while not stop_event.is_set() and not base._api_is_bound(port):
        stop_event.wait(base.API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    exact_index_ready = False
    consecutive_terminal_failures = 0
    while not stop_event.is_set():
        if not exact_index_ready:
            # Verify catalog truth before every retry in a killable diagnostic process.
            before = _safe_index_status()
            if before is not None and bool(before.get("ready")):
                exact_index_ready = True
                consecutive_terminal_failures = 0
                _record_index_supervisor_heartbeat(
                    state="success",
                    stage="cycle_history_index_ready_observed_before_retry",
                    detail={
                        "index_status": before,
                        "ddl_retry_skipped": True,
                        "planner_usable_verified": before.get("planner_usable_verified"),
                    },
                )
            else:
                return_code, timed_out = _run_index_child(stop_event=stop_event)
                if stop_event.is_set():
                    return

                # Catalog truth wins over child lifecycle, but this verification is also
                # bounded outside the parent so it can never strand future retries.
                after = _safe_index_status()
                if after is not None and bool(after.get("ready")):
                    error_type, exit_detail = _index_child_exit_diagnostics(
                        return_code,
                        timed_out=timed_out,
                    )
                    exact_index_ready = True
                    consecutive_terminal_failures = 0
                    _record_index_supervisor_heartbeat(
                        state="success",
                        stage="cycle_history_index_ready_observed_after_child_exit",
                        detail={
                            **exit_detail,
                            "child_exit_error_type": error_type,
                            "index_status": after,
                            "planner_usable_verified": after.get(
                                "planner_usable_verified"
                            ),
                            "ddl_retry_skipped": True,
                        },
                    )
                elif return_code == INDEX_NOT_READY_EXIT_CODE and not timed_out:
                    # The child completed normally and already published concrete SQL
                    # failure/retry detail. Preserve that terminal heartbeat verbatim.
                    stop_event.wait(INDEX_RETRY_SECONDS)
                    continue
                elif timed_out or return_code not in (0, None):
                    error_type, exit_detail = _index_child_exit_diagnostics(
                        return_code,
                        timed_out=timed_out,
                    )
                    consecutive_terminal_failures += 1
                    retry_seconds = (
                        INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS
                        if consecutive_terminal_failures
                        >= INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD
                        else INDEX_RETRY_SECONDS
                    )
                    _record_index_supervisor_heartbeat(
                        state="degraded",
                        stage="cycle_history_index_child_terminated",
                        error_type=error_type,
                        detail={
                            **exit_detail,
                            "index_status": after,
                            "consecutive_terminal_failures": consecutive_terminal_failures,
                            "retry_seconds": retry_seconds,
                            "retry_backoff_escalated": retry_seconds
                            > INDEX_RETRY_SECONDS,
                        },
                    )
                    print(
                        "cycle-history exact-index child terminated "
                        f"code={return_code} timed_out={timed_out}; "
                        f"retrying in {retry_seconds:.0f}s",
                        flush=True,
                    )
                    stop_event.wait(retry_seconds)
                    continue
                elif after is not None:
                    consecutive_terminal_failures += 1
                    _record_index_supervisor_heartbeat(
                        state="degraded",
                        stage="cycle_history_index_child_returned_without_ready_index",
                        error_type="IndexChildReturnedWithoutPlannerUsableIndex",
                        detail={
                            "child_return_code": return_code,
                            "index_status": after,
                            "consecutive_terminal_failures": consecutive_terminal_failures,
                            "retry_seconds": INDEX_RETRY_SECONDS,
                        },
                    )
                    stop_event.wait(INDEX_RETRY_SECONDS)
                    continue
                else:
                    # If bounded verification itself is unavailable, preserve legacy
                    # behavior only for a clean child exit; nonzero exits are handled above.
                    exact_index_ready = return_code in (0, None) and not timed_out

            if exact_index_ready:
                print(
                    "cycle-history exact index verified; raw-history backfill may resume",
                    flush=True,
                )

        memory = instance_memory_snapshot()
        if bool(getattr(memory, "terminate_required", False)):
            print(
                "cycle-history background backfill deferred by emergency memory admission",
                flush=True,
            )
            stop_event.wait(base.BACKFILL_MEMORY_RETRY_SECONDS)
            continue

        return_code, timed_out = _run_bounded_child(
            BACKFILL_COMMAND,
            deadline_seconds=base.BACKFILL_EXECUTOR_DEADLINE_SECONDS,
            stop_event=stop_event,
        )
        if stop_event.is_set():
            return
        if timed_out:
            print(
                "cycle-history background backfill exceeded its 90s executor deadline; retrying",
                flush=True,
            )
            stop_event.wait(base.BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        if return_code == INDEX_NOT_READY_EXIT_CODE:
            exact_index_ready = False
            stop_event.wait(INDEX_RETRY_SECONDS)
            continue
        if return_code == base.TEMPORARY_ADMISSION_EXIT_CODE:
            stop_event.wait(base.BACKFILL_MEMORY_RETRY_SECONDS)
            continue
        if return_code == base.INCOMPLETE_PROGRESS_EXIT_CODE:
            stop_event.wait(base.BACKFILL_BOOTSTRAP_INTERVAL_SECONDS)
            continue
        if return_code not in (0, None):
            print(
                f"cycle-history background backfill child exited code={return_code}; retrying",
                flush=True,
            )
            stop_event.wait(base.BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        stop_event.wait(base.BACKFILL_SUCCESS_INTERVAL_SECONDS)


__all__ = [
    "BACKFILL_COMMAND",
    "INDEX_COMMAND",
    "INDEX_DIAGNOSTIC_COMMAND",
    "INDEX_DIAGNOSTIC_DEADLINE_SECONDS",
    "INDEX_EXECUTOR_DEADLINE_SECONDS",
    "INDEX_PROGRESS_HEARTBEAT_SECONDS",
    "INDEX_RETRY_SECONDS",
    "INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS",
    "INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD",
    "_index_child_exit_diagnostics",
    "_record_index_supervisor_heartbeat",
    "_run_index_diagnostic",
    "_safe_index_status",
    "run_cycle_history_background_supervisor",
]
