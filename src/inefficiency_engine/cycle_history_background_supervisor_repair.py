from __future__ import annotations

import subprocess
import sys
import threading
import time

from inefficiency_engine import cycle_history_background_supervisor as base
from inefficiency_engine.cycle_history_background_backfill_repair import (
    INDEX_NOT_READY_EXIT_CODE,
)
from inefficiency_engine.instance_memory import instance_memory_snapshot


INDEX_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_index_maintenance_child",
]
BACKFILL_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_background_backfill_repair",
]
# Production proved the former 120-second PostgreSQL statement timeout could restart
# CREATE INDEX CONCURRENTLY indefinitely before the exact four-column index finished.
# The dedicated child now gives that one build a ten-minute SQL deadline. Keep a small
# bounded process margin for verification and clean shutdown without changing any
# history/backfill deadline.
INDEX_EXECUTOR_DEADLINE_SECONDS = 630.0
INDEX_RETRY_SECONDS = 30.0


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


def run_cycle_history_background_supervisor(stop_event: threading.Event) -> None:
    """Verify the exact query index before any raw cycle-history reconstruction.

    The first child owns only bounded PostgreSQL index verification/maintenance. The
    second child owns only the existing checkpointed 180-day history slice. A backfill
    child is never launched until the index child has certified a planner-usable exact
    access path. If a later backfill detects that the index is no longer usable, the
    latch is cleared and maintenance is re-run before another bucket query starts.
    """

    port = base.os.getenv("PORT", "10000")
    while not stop_event.is_set() and not base._api_is_bound(port):
        stop_event.wait(base.API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    exact_index_ready = False
    while not stop_event.is_set():
        if not exact_index_ready:
            return_code, timed_out = _run_bounded_child(
                INDEX_COMMAND,
                deadline_seconds=INDEX_EXECUTOR_DEADLINE_SECONDS,
                stop_event=stop_event,
            )
            if stop_event.is_set():
                return
            if timed_out:
                print(
                    "cycle-history exact-index maintenance exceeded its bounded deadline; retrying",
                    flush=True,
                )
                stop_event.wait(INDEX_RETRY_SECONDS)
                continue
            if return_code not in (0, None):
                print(
                    f"cycle-history exact-index maintenance exited code={return_code}; retrying",
                    flush=True,
                )
                stop_event.wait(INDEX_RETRY_SECONDS)
                continue
            exact_index_ready = True
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
            # Never continue raw reconstruction against a vanished/invalid access path.
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
    "INDEX_EXECUTOR_DEADLINE_SECONDS",
    "INDEX_RETRY_SECONDS",
    "run_cycle_history_background_supervisor",
]
