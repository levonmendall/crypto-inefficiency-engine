from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from urllib.request import urlopen

from inefficiency_engine.candidate_observatory_historical_replay import (
    REPLAY_COMPLETE_EXIT_CODE,
)
from inefficiency_engine.instance_memory import instance_memory_snapshot


BACKFILL_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.disposable_heavy_job",
    "observatory_backfill",
]
BACKFILL_EXECUTOR_DEADLINE_SECONDS = 60.0
BACKFILL_SUCCESS_INTERVAL_SECONDS = 5.0
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


def run_candidate_observatory_backfill_supervisor(stop_event: threading.Event) -> None:
    """Backfill missing observatory truth in short-lived, leased children.

    Each child advances only bounded durable-ledger rows and then exits, so historical
    observability cannot monopolize memory. The heavy-work lease serializes this job
    against research/history work. Once a genuine live observatory boundary exists and
    all pre-boundary records are indexed, the supervisor terminates permanently for
    the lifetime of the service process.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    while not stop_event.is_set():
        memory = instance_memory_snapshot()
        if bool(getattr(memory, "start_blocked", False)):
            stop_event.wait(BACKFILL_MEMORY_RETRY_SECONDS)
            continue

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
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue
        if return_code == REPLAY_COMPLETE_EXIT_CODE:
            print("candidate observatory historical replay is complete", flush=True)
            return
        if return_code == TEMPORARY_ADMISSION_EXIT_CODE:
            stop_event.wait(BACKFILL_MEMORY_RETRY_SECONDS)
            continue
        if return_code not in (0, None):
            print(
                f"candidate observatory historical replay child exited code={return_code}; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue
        stop_event.wait(BACKFILL_SUCCESS_INTERVAL_SECONDS)


__all__ = [
    "BACKFILL_COMMAND",
    "BACKFILL_EXECUTOR_DEADLINE_SECONDS",
    "run_candidate_observatory_backfill_supervisor",
]
