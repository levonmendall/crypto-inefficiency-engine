from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence

from inefficiency_engine.instance_memory import instance_memory_snapshot


API_APP = "inefficiency_engine.read_api_active_volume_deploy:app"
DEFAULT_RESEARCH_INTERVAL_SECONDS = 30.0
DEFAULT_HISTORY_INTERVAL_SECONDS = 300.0
DEFAULT_HEAVY_STARTUP_GRACE_SECONDS = 90.0
SUPERVISOR_POLL_SECONDS = 1.0
HEAVY_TERMINATE_GRACE_SECONDS = 15.0


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


def main() -> int:
    """Run a memory-bounded Render topology with disposable heavy processes.

    Only the read API and canonical portfolio stay resident. Research and historical
    maintenance are mutually exclusive subprocesses: one bounded cycle/batch runs,
    persists durable state, and exits so the OS reclaims its entire Python heap.
    Aggregate cgroup memory is checked before every heavy start and while a heavy job
    is alive. This makes peak memory depend on batch/concurrency limits rather than
    on the 40-asset universe or the number of accumulated research features.
    """

    port = os.getenv("PORT", "10000")
    permanent_commands = child_commands(port)
    disposable_commands = heavy_commands()
    permanent: dict[str, subprocess.Popen[bytes]] = {}
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

    def _request_stop(signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        print(f"combined runtime received signal {signum}; shutting down", flush=True)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    started_at = time.monotonic()
    next_due = {
        "research": started_at + startup_grace,
        "history": started_at + startup_grace + min(60.0, history_interval / 2.0),
    }
    intervals = {"research": research_interval, "history": history_interval}

    try:
        # Canonical accounting starts first, then the read API. Heavy research waits
        # through startup_grace so the canonical account proves liveness first.
        for name in ("portfolio", "api"):
            command = permanent_commands[name]
            print(f"starting permanent child {name}: {' '.join(command)}", flush=True)
            permanent[name] = subprocess.Popen(command)

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
