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
BACKFILL_COVERAGE_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.candidate_observatory_lane_coverage_preflight",
]
BACKFILL_EXECUTOR_DEADLINE_SECONDS = 60.0
# Lane certification must aggregate the full replay window across several durable
# ledgers. Keep the child strictly bounded, but give those database aggregates enough
# time to publish the authoritative 13-lane coverage heartbeat before termination.
BACKFILL_COVERAGE_DEADLINE_SECONDS = 90.0
BACKFILL_SUCCESS_INTERVAL_SECONDS = 5.0
BACKFILL_FAILURE_RETRY_SECONDS = 15.0
BACKFILL_MEMORY_RETRY_SECONDS = 15.0
API_BIND_POLL_SECONDS = 2.0
API_BIND_TIMEOUT_SECONDS = 2.0
TEMPORARY_ADMISSION_EXIT_CODE = 75
COVERAGE_NOT_READY_EXIT_CODE = 2
COVERAGE_COMPLETE_EXIT_CODE = 3
COVERAGE_INCOMPLETE_EXIT_CODE = 4


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


def _run_bounded_child(
    command: list[str],
    stop_event: threading.Event,
    *,
    deadline_seconds: float,
) -> tuple[int | None, bool]:
    child = subprocess.Popen(command)
    started = time.monotonic()
    timed_out = False
    while not stop_event.is_set() and child.poll() is None:
        if time.monotonic() - started >= deadline_seconds:
            timed_out = True
            _terminate(child)
            break
        stop_event.wait(0.5)
    if stop_event.is_set():
        _terminate(child)
    return child.poll(), timed_out


def _coverage_result(stop_event: threading.Event) -> tuple[int | None, bool]:
    return _run_bounded_child(
        BACKFILL_COVERAGE_COMMAND,
        stop_event,
        deadline_seconds=BACKFILL_COVERAGE_DEADLINE_SECONDS,
    )


def _coverage_terminal(code: int | None) -> bool:
    return code in {COVERAGE_COMPLETE_EXIT_CODE, COVERAGE_INCOMPLETE_EXIT_CODE}


def _log_terminal_coverage(code: int | None) -> None:
    if code == COVERAGE_COMPLETE_EXIT_CODE:
        print(
            "candidate observatory historical replay is complete with all required lanes certified",
            flush=True,
        )
    elif code == COVERAGE_INCOMPLETE_EXIT_CODE:
        print(
            "candidate observatory historical replay indexed but required lane coverage is incomplete",
            flush=True,
        )


def run_candidate_observatory_backfill_supervisor(stop_event: threading.Event) -> None:
    """Backfill missing observatory truth in short-lived, leased children.

    A short-lived coverage preflight runs before heavy-job memory admission. If durable
    replay is already drained, this guarantees the authoritative 13-lane heartbeat can
    be refreshed even when aggregate service memory is too high to admit another heavy
    replay child. If replay is not ready, the preflight exits without certification and
    the existing fail-closed, memory-gated replay path remains unchanged.
    """

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    while not stop_event.is_set():
        # Coverage certification is a finite database aggregation, not a heavy provider
        # replay. Run it first so a previously drained replay cannot remain stuck behind
        # the aggregate-memory start gate with an obsolete two-lane heartbeat.
        coverage_code, coverage_timed_out = _coverage_result(stop_event)
        if stop_event.is_set():
            return
        if coverage_timed_out:
            print(
                "candidate observatory lane coverage preflight timed out; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue
        if _coverage_terminal(coverage_code):
            _log_terminal_coverage(coverage_code)
            return
        if coverage_code not in (COVERAGE_NOT_READY_EXIT_CODE, None):
            print(
                f"candidate observatory lane coverage preflight exited code={coverage_code}; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        memory = instance_memory_snapshot()
        if bool(getattr(memory, "start_blocked", False)):
            stop_event.wait(BACKFILL_MEMORY_RETRY_SECONDS)
            continue

        return_code, timed_out = _run_bounded_child(
            BACKFILL_COMMAND,
            stop_event,
            deadline_seconds=BACKFILL_EXECUTOR_DEADLINE_SECONDS,
        )
        if stop_event.is_set():
            return
        if timed_out:
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue

        if return_code == REPLAY_COMPLETE_EXIT_CODE:
            coverage_code, coverage_timed_out = _coverage_result(stop_event)
            if stop_event.is_set():
                return
            if coverage_timed_out:
                print(
                    "candidate observatory lane coverage certification timed out; retrying",
                    flush=True,
                )
                stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
                continue
            if _coverage_terminal(coverage_code):
                _log_terminal_coverage(coverage_code)
                return
            print(
                f"candidate observatory lane coverage child exited code={coverage_code}; retrying",
                flush=True,
            )
            stop_event.wait(BACKFILL_FAILURE_RETRY_SECONDS)
            continue

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
    "BACKFILL_COVERAGE_COMMAND",
    "BACKFILL_COVERAGE_DEADLINE_SECONDS",
    "BACKFILL_EXECUTOR_DEADLINE_SECONDS",
    "COVERAGE_NOT_READY_EXIT_CODE",
    "run_candidate_observatory_backfill_supervisor",
]
