from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from urllib.request import urlopen

from inefficiency_engine.instance_memory import instance_memory_snapshot


API_APP = "inefficiency_engine.read_api_active_volume_deploy:app"
PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"
DEFAULT_RESEARCH_INTERVAL_SECONDS = 30.0
DEFAULT_HISTORY_INTERVAL_SECONDS = 300.0
DEFAULT_HEAVY_STARTUP_GRACE_SECONDS = 90.0
DEFAULT_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS = 180.0
DEFAULT_PORTFOLIO_RUNNING_STALE_SECONDS = 210.0
DEFAULT_PORTFOLIO_HEARTBEAT_STALE_SECONDS = 600.0
PORTFOLIO_WATCHDOG_CHECK_SECONDS = 15.0
LOCAL_HEALTH_TIMEOUT_SECONDS = 2.0
SUPERVISOR_POLL_SECONDS = 1.0
HEAVY_TERMINATE_GRACE_SECONDS = 15.0
PERMANENT_RESTART_GRACE_SECONDS = 15.0


def _env_seconds(name: str, default: float, *, minimum: float = 1.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value)


def child_commands(port: str | int) -> dict[str, list[str]]:
    """Return only permanent lightweight children for the Render service."""

    port_text = str(port)
    return {
        "portfolio": [
            sys.executable,
            "-m",
            "inefficiency_engine.lightweight_portfolio_worker",
        ],
        "api": [
            sys.executable,
            "-m",
            "uvicorn",
            API_APP,
            "--host",
            "0.0.0.0",
            "--port",
            port_text,
        ],
    }


def heavy_commands() -> dict[str, list[str]]:
    """Return mutually-exclusive disposable heavyweight jobs."""

    base = [sys.executable, "-m", "inefficiency_engine.disposable_heavy_job"]
    return {
        "research": [*base, "research"],
        "history": [*base, "history"],
    }


def _terminate_children(
    children: Sequence[subprocess.Popen[bytes]],
    *,
    timeout_seconds: float = 20.0,
) -> None:
    for child in children:
        if child.poll() is None:
            child.terminate()

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if all(child.poll() is not None for child in children):
            return
        time.sleep(0.2)

    for child in children:
        if child.poll() is None:
            child.kill()


def _local_health(port: str | int) -> dict[str, object]:
    """Read the already-running API's persisted runtime diagnostics with a hard timeout."""

    with urlopen(
        f"http://127.0.0.1:{port}/health",
        timeout=LOCAL_HEALTH_TIMEOUT_SECONDS,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("local health payload is not an object")
    return payload


def portfolio_watchdog_reason(
    payload: dict[str, object],
    *,
    process_started_at: datetime,
    process_age_seconds: float,
    startup_grace_seconds: float = DEFAULT_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS,
    running_stale_seconds: float = DEFAULT_PORTFOLIO_RUNNING_STALE_SECONDS,
    heartbeat_stale_seconds: float = DEFAULT_PORTFOLIO_HEARTBEAT_STALE_SECONDS,
) -> str | None:
    """Return a restart reason only when the permanent portfolio worker is genuinely stuck.

    The canonical loop normally writes ``running`` immediately before a cycle and a
    terminal success/degraded/error heartbeat immediately afterward. A normal cycle
    is bounded to 120 seconds and the next cycle is normally five minutes later, so
    those two states need different watchdog windows. This deliberately does not use
    the generic 180-second dashboard staleness threshold, which is shorter than the
    normal portfolio sleep interval.
    """

    if process_age_seconds < max(0.0, startup_grace_seconds):
        return None

    runtime = payload.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    row = workers.get("portfolio") if isinstance(workers, dict) else None
    if not isinstance(row, dict) or not row.get("available"):
        return "current portfolio child has not published a durable heartbeat"

    observed_raw = row.get("observed_at")
    try:
        observed_at = datetime.fromisoformat(str(observed_raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return "portfolio heartbeat has no valid observed_at timestamp"
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    started = process_started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    if observed_at < started:
        return "current portfolio child has not replaced the previous process heartbeat"

    try:
        age_seconds = max(0.0, float(row.get("age_seconds") or 0.0))
    except (TypeError, ValueError):
        return "portfolio heartbeat age is invalid"
    state = str(row.get("state") or "unknown")

    if state == "running" and age_seconds > max(1.0, running_stale_seconds):
        return (
            f"portfolio cycle remained running for {age_seconds:.1f}s "
            f"(limit {running_stale_seconds:.1f}s)"
        )
    if age_seconds > max(1.0, heartbeat_stale_seconds):
        return (
            f"portfolio heartbeat is {age_seconds:.1f}s old "
            f"(limit {heartbeat_stale_seconds:.1f}s; state={state})"
        )
    if state in {"stopped", "completed"}:
        return f"portfolio subprocess is alive but heartbeat state={state}"
    return None


def main() -> int:
    """Run a memory-bounded Render topology with disposable heavy processes.

    Only the read API and canonical portfolio stay resident. Research and historical
    maintenance are mutually exclusive subprocesses: one bounded cycle/batch runs,
    persists durable state, and exits so the OS reclaims its entire Python heap.
    Aggregate cgroup memory is checked before every heavy start and while a heavy job
    is alive. The supervisor also watches the portfolio worker's durable heartbeat so
    an alive-but-wedged child cannot leave the dashboard/account state frozen while
    the API process continues serving stale data.
    """

    port = os.getenv("PORT", "10000")
    permanent_commands = child_commands(port)
    disposable_commands = heavy_commands()
    permanent: dict[str, subprocess.Popen[bytes]] = {}
    permanent_started_at: dict[str, datetime] = {}
    permanent_started_monotonic: dict[str, float] = {}
    heavy: subprocess.Popen[bytes] | None = None
    heavy_name: str | None = None
    heavy_termination_requested_at: float | None = None
    stopping = False

    research_interval = _env_seconds(
        "CIE_DISPOSABLE_RESEARCH_INTERVAL_SECONDS",
        DEFAULT_RESEARCH_INTERVAL_SECONDS,
    )
    history_interval = _env_seconds(
        "CIE_DISPOSABLE_HISTORY_INTERVAL_SECONDS",
        DEFAULT_HISTORY_INTERVAL_SECONDS,
        minimum=30.0,
    )
    startup_grace = _env_seconds(
        "CIE_HEAVY_STARTUP_GRACE_SECONDS",
        DEFAULT_HEAVY_STARTUP_GRACE_SECONDS,
        minimum=0.0,
    )
    portfolio_watchdog_startup_grace = _env_seconds(
        "CIE_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS",
        DEFAULT_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS,
        minimum=30.0,
    )
    portfolio_running_stale = _env_seconds(
        "CIE_PORTFOLIO_RUNNING_STALE_SECONDS",
        DEFAULT_PORTFOLIO_RUNNING_STALE_SECONDS,
        minimum=150.0,
    )
    portfolio_heartbeat_stale = _env_seconds(
        "CIE_PORTFOLIO_HEARTBEAT_STALE_SECONDS",
        DEFAULT_PORTFOLIO_HEARTBEAT_STALE_SECONDS,
        minimum=480.0,
    )

    def _request_stop(signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        print(f"combined runtime received signal {signum}; shutting down", flush=True)

    def _start_permanent(name: str) -> None:
        command = permanent_commands[name]
        print(f"starting permanent child {name}: {' '.join(command)}", flush=True)
        permanent[name] = subprocess.Popen(command)
        permanent_started_at[name] = datetime.now(timezone.utc)
        permanent_started_monotonic[name] = time.monotonic()

    def _restart_portfolio(reason: str) -> None:
        current = permanent.get("portfolio")
        if current is not None:
            print(
                f"portfolio watchdog restarting alive-but-stale child pid={current.pid}: {reason}",
                flush=True,
            )
            _terminate_children(
                [current],
                timeout_seconds=PERMANENT_RESTART_GRACE_SECONDS,
            )
        _start_permanent("portfolio")

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    started_at = time.monotonic()
    next_due = {
        "research": started_at + startup_grace,
        "history": started_at + startup_grace + min(60.0, history_interval / 2.0),
    }
    intervals = {"research": research_interval, "history": history_interval}
    next_portfolio_watchdog = started_at + min(
        PORTFOLIO_WATCHDOG_CHECK_SECONDS,
        portfolio_watchdog_startup_grace,
    )

    try:
        # Canonical accounting starts first, then the read API. Heavy research waits
        # through startup_grace so the canonical account proves liveness first.
        for name in ("portfolio", "api"):
            _start_permanent(name)

        while not stopping:
            for name, child in permanent.items():
                return_code = child.poll()
                if return_code is not None:
                    print(
                        f"critical permanent child {name} exited with code {return_code}; restarting Render service",
                        flush=True,
                    )
                    return return_code if return_code != 0 else 1

            now = time.monotonic()
            memory = instance_memory_snapshot()

            if now >= next_portfolio_watchdog:
                next_portfolio_watchdog = now + PORTFOLIO_WATCHDOG_CHECK_SECONDS
                try:
                    health = _local_health(port)
                    reason = portfolio_watchdog_reason(
                        health,
                        process_started_at=permanent_started_at["portfolio"],
                        process_age_seconds=max(
                            0.0,
                            now - permanent_started_monotonic["portfolio"],
                        ),
                        startup_grace_seconds=portfolio_watchdog_startup_grace,
                        running_stale_seconds=portfolio_running_stale,
                        heartbeat_stale_seconds=portfolio_heartbeat_stale,
                    )
                except Exception as exc:
                    # Render's own /health probe remains the API-process authority.
                    # Do not kill a healthy portfolio child merely because this local
                    # diagnostic read was temporarily unavailable.
                    print(
                        f"portfolio watchdog health read unavailable: {type(exc).__name__}",
                        flush=True,
                    )
                    reason = None
                if reason is not None:
                    _restart_portfolio(reason)
                    now = time.monotonic()
                    next_portfolio_watchdog = now + PORTFOLIO_WATCHDOG_CHECK_SECONDS

            if heavy is not None:
                return_code = heavy.poll()
                if return_code is not None:
                    completed_name = heavy_name or "unknown"
                    print(
                        f"disposable heavy child {completed_name} exited code={return_code}; "
                        f"aggregate_memory={memory.as_dict()}",
                        flush=True,
                    )
                    next_due[completed_name] = now + intervals.get(completed_name, 30.0)
                    heavy = None
                    heavy_name = None
                    heavy_termination_requested_at = None
                elif memory.terminate_required:
                    if heavy_termination_requested_at is None:
                        heavy_termination_requested_at = now
                        print(
                            f"aggregate memory reached terminate budget; stopping disposable {heavy_name}: "
                            f"{memory.as_dict()}",
                            flush=True,
                        )
                        heavy.terminate()
                    elif now - heavy_termination_requested_at >= HEAVY_TERMINATE_GRACE_SECONDS:
                        print(
                            f"disposable {heavy_name} did not exit after memory SIGTERM; killing process",
                            flush=True,
                        )
                        heavy.kill()
                elif memory.soft_exceeded:
                    print(
                        f"aggregate memory above soft budget while {heavy_name} runs: {memory.as_dict()}",
                        flush=True,
                    )
            else:
                due_history = now >= next_due["history"]
                due_research = now >= next_due["research"]
                next_name = "history" if due_history else "research" if due_research else None
                if next_name is not None:
                    if memory.start_blocked:
                        # Do not start another Python heap while the permanent service
                        # is already close to Render's aggregate memory boundary.
                        next_due[next_name] = now + min(30.0, intervals[next_name])
                        print(
                            f"deferring disposable {next_name}; aggregate memory start-blocked: "
                            f"{memory.as_dict()}",
                            flush=True,
                        )
                    else:
                        command = disposable_commands[next_name]
                        print(
                            f"starting disposable heavy child {next_name}: {' '.join(command)}; "
                            f"aggregate_memory={memory.as_dict()}",
                            flush=True,
                        )
                        heavy = subprocess.Popen(command)
                        heavy_name = next_name
                        heavy_termination_requested_at = None

            time.sleep(SUPERVISOR_POLL_SECONDS)
        return 0
    finally:
        children = list(permanent.values())
        if heavy is not None:
            children.append(heavy)
        _terminate_children(children)


if __name__ == "__main__":
    raise SystemExit(main())
