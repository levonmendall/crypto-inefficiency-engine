from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from urllib.request import urlopen

from inefficiency_engine.instance_memory import instance_memory_snapshot


BACKFILL_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.cycle_history_background_backfill",
]
BACKFILL_EXECUTOR_DEADLINE_SECONDS = 90.0
# The backfill child now advances a bounded batch rather than one bucket. Leave an
# explicit window after each successful batch so scheduled research/alpha work can win
# the shared heavy-work lease instead of being repeatedly raced by a five-second loop.
BACKFILL_SUCCESS_INTERVAL_SECONDS = 30.0
BACKFILL_FAILURE_RETRY_SECONDS = 15.0
BACKFILL_MEMORY_RETRY_SECONDS = 15.0
API_BIND_POLL_SECONDS = 2.0
API_BIND_TIMEOUT_SECONDS = 2.0
TEMPORARY_ADMISSION_EXIT_CODE = 75


def _api_is_bound(port: str | int) -> bool:
    try:
        with urlopen(
            f"http://127.0.0.1:{port}/health",
            timeout=API_BIND_TIMEOUT_SECONDS,
        ) as response:
            return int(getattr(response, "status", 200)) == 200
    except Exception:
        return False


def _terminate(child: subprocess.Popen[bytes], *, grace_seconds: float = 5.0) -> None:
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


def run_cycle_history_background_supervisor(stop_event: threading.Event) -> None:
    """Run exact history maintenance in short-lived, memory-reclaiming children.

    The coordinator stays lightweight. Each child acquires the existing heavy-work
    lease, advances a bounded durable history batch, checkpoints each completed bucket,
    and exits completely. The post-success yield gives disposable research a fair lease
    window while still advancing the 180-day frozen target much faster than the former
    one-bucket-per-process design.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    while not stop_event.is_set():
        memory = instance_memory_snapshot()
        if bool(getattr(memory, "start_blocked", False)):
            print(
                "cycle-history background backfill deferred by aggregate memory admission",
                flush=True,
            )
            stop_event.wait(BACKFILL_MEMORY_RETRY_SECONDS)
            continue

        print(
            f"starting disposable cycle-history backfill child: {' '.join(BACKFILL_COMMAND)}",
            flush=True,
        )
        child = subprocess.Popen(BACKFILL_COMMAND)
        started = time.monotonic()
        timed_out = False
        while not stop_event.is_set() and child.poll() is None:
            if time.monotonic() - started >= BACKFILL_EXECUTOR_DEADLINE_SECONDS:
                timed_out = True
                _terminate(child)
                break
            stop_event.wait(0.5)

        if stop_event.is_set():
            _terminate(child)
            return

        return_code = child.poll()
        if timed_out:
            print(
                "cycle-history background backfill exceeded its 90s executor deadline; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        if return_code == TEMPORARY_ADMISSION_EXIT_CODE:
            stop_event.wait(BACKFILL_MEMORY_RETRY_SECONDS)
            continue
        if return_code not in (0, None):
            print(
                f"cycle-history background backfill child exited code={return_code}; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        stop_event.wait(BACKFILL_SUCCESS_INTERVAL_SECONDS)


__all__ = [
    "BACKFILL_COMMAND",
    "BACKFILL_EXECUTOR_DEADLINE_SECONDS",
    "BACKFILL_SUCCESS_INTERVAL_SECONDS",
    "run_cycle_history_background_supervisor",
]
