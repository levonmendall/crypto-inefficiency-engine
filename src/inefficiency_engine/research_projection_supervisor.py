from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from urllib.request import urlopen

from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_projection import DASHBOARD_RESEARCH_PROJECTION_WORKER_ID
from inefficiency_engine.evidence import build_evidence_store
from inefficiency_engine.instance_memory import instance_memory_snapshot


REFRESH_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.research_projection_refresh_child",
]
REFRESH_INTERVAL_SECONDS = 60.0
REFRESH_FAILURE_RETRY_SECONDS = 15.0
REFRESH_MEMORY_RETRY_SECONDS = 15.0
REFRESH_EXECUTOR_DEADLINE_SECONDS = 45.0
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


def _record_timeout(store) -> None:
    if store is None:
        return
    try:
        store.record_worker_heartbeat(
            worker_id=DASHBOARD_RESEARCH_PROJECTION_WORKER_ID,
            state="degraded",
            error_type="ResearchProjectionRefreshDeadlineExceeded",
            detail={
                "publication_stage": "disposable_persisted_refresh",
                "publication_owner": "research-projection-refresh-supervisor",
                "deadline_seconds": REFRESH_EXECUTOR_DEADLINE_SECONDS,
                "executor_terminated": True,
                "retrying": True,
                "research_computation": False,
                "provider_calls": False,
                "presentation_only": True,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
            },
        )
    except Exception:
        pass


def run_research_projection_supervisor(stop_event: threading.Event) -> None:
    """Keep the persisted research projection fresh with a killable process boundary."""

    port = os.getenv("PORT", "10000")
    while not stop_event.is_set() and not _api_is_bound(port):
        stop_event.wait(API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    try:
        settings = Settings.from_env()
        store = build_evidence_store(settings.evidence_db_path)
    except Exception:
        store = None

    while not stop_event.is_set():
        memory = instance_memory_snapshot()
        if bool(getattr(memory, "start_blocked", False)):
            stop_event.wait(REFRESH_MEMORY_RETRY_SECONDS)
            continue

        child = subprocess.Popen(REFRESH_COMMAND)
        started = time.monotonic()
        timed_out = False
        while not stop_event.is_set() and child.poll() is None:
            if time.monotonic() - started >= REFRESH_EXECUTOR_DEADLINE_SECONDS:
                timed_out = True
                _terminate(child)
                _record_timeout(store)
                break
            stop_event.wait(0.5)

        if stop_event.is_set():
            _terminate(child)
            return

        if timed_out:
            stop_event.wait(REFRESH_FAILURE_RETRY_SECONDS)
            continue

        return_code = child.poll()
        stop_event.wait(
            REFRESH_INTERVAL_SECONDS if return_code in (0, None) else REFRESH_FAILURE_RETRY_SECONDS
        )


__all__ = [
    "REFRESH_COMMAND",
    "REFRESH_EXECUTOR_DEADLINE_SECONDS",
    "REFRESH_INTERVAL_SECONDS",
    "run_research_projection_supervisor",
]
