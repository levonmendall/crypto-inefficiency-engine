from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from urllib.request import urlopen


PROJECTION_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.durable_lane_history_projection",
]
PROJECTION_DEADLINE_SECONDS = 45.0
PROJECTION_INTERVAL_SECONDS = 30.0
PROJECTION_FAILURE_RETRY_SECONDS = 10.0
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
    deadline_seconds: float = PROJECTION_DEADLINE_SECONDS,
) -> tuple[int | None, bool]:
    child = subprocess.Popen(PROJECTION_COMMAND)
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


def run_durable_lane_history_projection_supervisor(stop_event: threading.Event) -> None:
    """Continuously materialize history outside the browser/API request path."""

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    while not stop_event.is_set():
        return_code, timed_out = _run_bounded_child(stop_event)
        if stop_event.is_set():
            return
        if timed_out:
            print(
                "durable lane history projection timed out; retaining prior projection",
                flush=True,
            )
            stop_event.wait(PROJECTION_FAILURE_RETRY_SECONDS)
            continue
        if return_code == 0:
            stop_event.wait(PROJECTION_INTERVAL_SECONDS)
            continue
        print(
            f"durable lane history projection exited code={return_code}; retaining prior projection",
            flush=True,
        )
        stop_event.wait(PROJECTION_FAILURE_RETRY_SECONDS)


__all__ = [
    "PROJECTION_COMMAND",
    "PROJECTION_DEADLINE_SECONDS",
    "PROJECTION_INTERVAL_SECONDS",
    "run_durable_lane_history_projection_supervisor",
]
