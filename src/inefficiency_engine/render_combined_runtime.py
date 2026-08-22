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
RESEARCH_WORKER_ID = "shadow-research-auxiliary"
TEMPORARY_ADMISSION_EXIT_CODE = 75

DEFAULT_RESEARCH_INTERVAL_SECONDS = 30.0
DEFAULT_HISTORY_INTERVAL_SECONDS = 300.0
DEFAULT_HEAVY_STARTUP_GRACE_SECONDS = 90.0
DEFAULT_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS = 180.0
DEFAULT_PORTFOLIO_RUNNING_STALE_SECONDS = 210.0
DEFAULT_PORTFOLIO_HEARTBEAT_STALE_SECONDS = 600.0
DEFAULT_RESEARCH_WATCHDOG_STARTUP_GRACE_SECONDS = 180.0
DEFAULT_RESEARCH_HEARTBEAT_STALE_SECONDS = 600.0
DEFAULT_RESEARCH_JOB_TIMEOUT_SECONDS = 300.0
DEFAULT_RESEARCH_RECOVERY_FAILURE_SECONDS = 600.0
PORTFOLIO_WATCHDOG_CHECK_SECONDS = 15.0
LOCAL_READ_TIMEOUT_SECONDS = 2.5
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
    port_text = str(port)
    return {
        "portfolio": [sys.executable, "-m", "inefficiency_engine.lightweight_portfolio_worker"],
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
    base = [sys.executable, "-m", "inefficiency_engine.disposable_heavy_job"]
    return {"research": [*base, "research"], "history": [*base, "history"]}


def choose_heavy_job(*, due_research: bool, due_history: bool, research_overdue: bool) -> str | None:
    if research_overdue or due_research:
        return "research"
    if due_history:
        return "history"
    return None


def recovery_failure_exceeded(
    *,
    research_overdue: bool,
    failed_since: float | None,
    now: float,
    limit_seconds: float,
) -> bool:
    return bool(
        research_overdue
        and failed_since is not None
        and now - failed_since >= max(1.0, float(limit_seconds))
    )


def research_job_timed_out(
    *,
    heavy_name: str | None,
    heavy_started_at: float | None,
    now: float,
    timeout_seconds: float,
) -> bool:
    """Legacy total-runtime predicate retained for compatibility tests/callers.

    Production supervision no longer uses this predicate because a valid integrated
    research cycle can legitimately exceed the old 300-second wall-clock budget.
    """

    return bool(
        heavy_name == "research"
        and heavy_started_at is not None
        and now - heavy_started_at > max(1.0, float(timeout_seconds))
    )


def research_job_stalled(
    *,
    heavy_name: str | None,
    last_progress_at: float | None,
    now: float,
    timeout_seconds: float,
) -> bool:
    """Kill only a research child that made no durable progress for the timeout."""

    return bool(
        heavy_name == "research"
        and last_progress_at is not None
        and now - last_progress_at > max(1.0, float(timeout_seconds))
    )


def research_heartbeat_marker(health: dict[str, object]) -> str | None:
    """Return the durable research heartbeat timestamp used as a progress marker."""

    runtime = health.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    row = workers.get("research") if isinstance(workers, dict) else None
    if not isinstance(row, dict) or not row.get("available"):
        return None
    observed_at = row.get("observed_at")
    if observed_at in (None, ""):
        return None
    return str(observed_at)


def _terminate_children(children: Sequence[subprocess.Popen[bytes]], *, timeout_seconds: float = 20.0) -> None:
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


def _local_json(port: str | int, path: str) -> dict[str, object]:
    with urlopen(
        f"http://127.0.0.1:{port}{path}",
        timeout=LOCAL_READ_TIMEOUT_SECONDS,
    ) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"local {path} payload is not an object")
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
    if process_age_seconds < max(0.0, startup_grace_seconds):
        return None
    runtime = payload.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    row = workers.get("portfolio") if isinstance(workers, dict) else None
    if not isinstance(row, dict) or not row.get("available"):
        return "current portfolio child has not published a durable heartbeat"
    try:
        observed_at = datetime.fromisoformat(str(row.get("observed_at")).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return "portfolio heartbeat has no valid observed_at timestamp"
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    started = process_started_at if process_started_at.tzinfo is not None else process_started_at.replace(tzinfo=timezone.utc)
    if observed_at < started:
        return "current portfolio child has not replaced the previous process heartbeat"
    try:
        age_seconds = max(0.0, float(row.get("age_seconds") or 0.0))
    except (TypeError, ValueError):
        return "portfolio heartbeat age is invalid"
    state = str(row.get("state") or "unknown")
    if state == "running" and age_seconds > max(1.0, running_stale_seconds):
        return f"portfolio cycle remained running for {age_seconds:.1f}s (limit {running_stale_seconds:.1f}s)"
    if age_seconds > max(1.0, heartbeat_stale_seconds):
        return f"portfolio heartbeat is {age_seconds:.1f}s old (limit {heartbeat_stale_seconds:.1f}s; state={state})"
    if state in {"stopped", "completed"}:
        return f"portfolio subprocess is alive but heartbeat state={state}"
    return None


def research_watchdog_reason(
    health: dict[str, object],
    dashboard: dict[str, object] | None,
    *,
    runtime_age_seconds: float,
    startup_grace_seconds: float = DEFAULT_RESEARCH_WATCHDOG_STARTUP_GRACE_SECONDS,
    heartbeat_stale_seconds: float = DEFAULT_RESEARCH_HEARTBEAT_STALE_SECONDS,
) -> str | None:
    """Require both current research execution and current dashboard publication."""

    if runtime_age_seconds < max(0.0, startup_grace_seconds):
        return None
    runtime = health.get("runtime_heartbeats")
    workers = runtime.get("workers") if isinstance(runtime, dict) else None
    row = workers.get("research") if isinstance(workers, dict) else None
    if not isinstance(row, dict) or not row.get("available"):
        return "research worker has not published a durable heartbeat"
    try:
        age_seconds = max(0.0, float(row.get("age_seconds") or 0.0))
    except (TypeError, ValueError):
        return "research heartbeat age is invalid"
    state = str(row.get("state") or "unknown")
    if state in {"error", "stopped"}:
        return f"research heartbeat state={state}; a new disposable research cycle is required"
    if age_seconds > max(1.0, heartbeat_stale_seconds):
        return f"research heartbeat is {age_seconds:.1f}s old (limit {heartbeat_stale_seconds:.1f}s; state={state})"

    if dashboard is None:
        return "research dashboard projection could not be read"
    freshness = dashboard.get("research_projection_freshness")
    if not isinstance(freshness, dict):
        return "research dashboard projection has no freshness contract"
    if not freshness.get("available"):
        return "research dashboard projection has not been published"
    if bool(freshness.get("stale")) or bool(dashboard.get("research_projection_stale")):
        age = freshness.get("age_seconds")
        return (
            f"research dashboard projection is stale ({float(age):.1f}s old)"
            if isinstance(age, (int, float))
            else "research dashboard projection is stale"
        )

    observed_raw = dashboard.get("research_projection_observed_at") or freshness.get("observed_at")
    try:
        observed_at = datetime.fromisoformat(str(observed_raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return "research dashboard projection has no valid observed_at timestamp"
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    projection_age = max(0.0, (datetime.now(timezone.utc) - observed_at).total_seconds())
    if projection_age > max(1.0, heartbeat_stale_seconds):
        return (
            f"research dashboard projection is {projection_age:.1f}s old "
            f"(publication SLA {heartbeat_stale_seconds:.1f}s)"
        )
    return None


def main() -> int:
    port = os.getenv("PORT", "10000")
    permanent_commands = child_commands(port)
    disposable_commands = heavy_commands()
    permanent: dict[str, subprocess.Popen[bytes]] = {}
    permanent_started_at: dict[str, datetime] = {}
    permanent_started_monotonic: dict[str, float] = {}
    heavy: subprocess.Popen[bytes] | None = None
    heavy_name: str | None = None
    heavy_started_at: float | None = None
    heavy_last_progress_at: float | None = None
    heavy_research_heartbeat_marker: str | None = None
    latest_research_heartbeat_marker: str | None = None
    heavy_termination_requested_at: float | None = None
    stopping = False

    research_interval = _env_seconds("CIE_DISPOSABLE_RESEARCH_INTERVAL_SECONDS", DEFAULT_RESEARCH_INTERVAL_SECONDS)
    history_interval = _env_seconds("CIE_DISPOSABLE_HISTORY_INTERVAL_SECONDS", DEFAULT_HISTORY_INTERVAL_SECONDS, minimum=30.0)
    startup_grace = _env_seconds("CIE_HEAVY_STARTUP_GRACE_SECONDS", DEFAULT_HEAVY_STARTUP_GRACE_SECONDS, minimum=0.0)
    portfolio_watchdog_startup_grace = _env_seconds(
        "CIE_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS",
        DEFAULT_PORTFOLIO_WATCHDOG_STARTUP_GRACE_SECONDS,
        minimum=30.0,
    )
    portfolio_running_stale = _env_seconds(
        "CIE_PORTFOLIO_RUNNING_STALE_SECONDS", DEFAULT_PORTFOLIO_RUNNING_STALE_SECONDS, minimum=150.0
    )
    portfolio_heartbeat_stale = _env_seconds(
        "CIE_PORTFOLIO_HEARTBEAT_STALE_SECONDS", DEFAULT_PORTFOLIO_HEARTBEAT_STALE_SECONDS, minimum=480.0
    )
    research_watchdog_startup_grace = _env_seconds(
        "CIE_RESEARCH_WATCHDOG_STARTUP_GRACE_SECONDS",
        DEFAULT_RESEARCH_WATCHDOG_STARTUP_GRACE_SECONDS,
        minimum=60.0,
    )
    research_heartbeat_stale = _env_seconds(
        "CIE_RESEARCH_HEARTBEAT_STALE_SECONDS", DEFAULT_RESEARCH_HEARTBEAT_STALE_SECONDS, minimum=300.0
    )
    research_job_timeout = _env_seconds(
        "CIE_RESEARCH_JOB_TIMEOUT_SECONDS", DEFAULT_RESEARCH_JOB_TIMEOUT_SECONDS, minimum=180.0
    )
    recovery_failure_limit = _env_seconds(
        "CIE_RESEARCH_RECOVERY_FAILURE_SECONDS", DEFAULT_RESEARCH_RECOVERY_FAILURE_SECONDS, minimum=300.0
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
            print(f"portfolio watchdog restarting child pid={current.pid}: {reason}", flush=True)
            _terminate_children([current], timeout_seconds=PERMANENT_RESTART_GRACE_SECONDS)
        _start_permanent("portfolio")

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)

    started_at = time.monotonic()
    next_due = {
        "research": started_at + startup_grace,
        "history": started_at + startup_grace + min(60.0, history_interval / 2.0),
    }
    intervals = {"research": research_interval, "history": history_interval}
    next_runtime_watchdog = started_at + 15.0
    research_overdue = False
    research_overdue_reason: str | None = None
    research_recovery_failed_since: float | None = None

    try:
        for name in ("portfolio", "api"):
            _start_permanent(name)

        while not stopping:
            for name, child in permanent.items():
                return_code = child.poll()
                if return_code is not None:
                    print(f"critical permanent child {name} exited code={return_code}; restarting Render service", flush=True)
                    return return_code if return_code != 0 else 1

            now = time.monotonic()
            memory = instance_memory_snapshot()

            if now >= next_runtime_watchdog:
                next_runtime_watchdog = now + PORTFOLIO_WATCHDOG_CHECK_SECONDS
                try:
                    health = _local_json(port, "/health")
                    marker = research_heartbeat_marker(health)
                    if marker is not None:
                        if (
                            heavy is not None
                            and heavy_name == "research"
                            and marker != heavy_research_heartbeat_marker
                        ):
                            heavy_research_heartbeat_marker = marker
                            heavy_last_progress_at = now
                        latest_research_heartbeat_marker = marker
                    portfolio_reason = portfolio_watchdog_reason(
                        health,
                        process_started_at=permanent_started_at["portfolio"],
                        process_age_seconds=max(0.0, now - permanent_started_monotonic["portfolio"]),
                        startup_grace_seconds=portfolio_watchdog_startup_grace,
                        running_stale_seconds=portfolio_running_stale,
                        heartbeat_stale_seconds=portfolio_heartbeat_stale,
                    )
                    try:
                        dashboard = _local_json(port, "/v3/dashboard/snapshot")
                    except Exception:
                        dashboard = None
                    research_reason = research_watchdog_reason(
                        health,
                        dashboard,
                        runtime_age_seconds=max(0.0, now - started_at),
                        startup_grace_seconds=research_watchdog_startup_grace,
                        heartbeat_stale_seconds=research_heartbeat_stale,
                    )
                    research_overdue = research_reason is not None
                    research_overdue_reason = research_reason
                except Exception as exc:
                    print(f"runtime watchdog read unavailable: {type(exc).__name__}", flush=True)
                    portfolio_reason = None
                if portfolio_reason is not None:
                    _restart_portfolio(portfolio_reason)
                    now = time.monotonic()
                    next_runtime_watchdog = now + PORTFOLIO_WATCHDOG_CHECK_SECONDS
                if research_overdue:
                    next_due["research"] = min(next_due["research"], now)
                    next_due["history"] = max(next_due["history"], now + min(history_interval, 300.0))
                    if heavy is not None and heavy_name == "history" and heavy.poll() is None and heavy_termination_requested_at is None:
                        print(
                            "research publication overdue; preempting history: "
                            f"{research_overdue_reason}",
                            flush=True,
                        )
                        heavy.terminate()
                        heavy_termination_requested_at = now

            if heavy is not None:
                return_code = heavy.poll()
                if return_code is not None:
                    completed_name = heavy_name or "unknown"
                    print(
                        f"disposable heavy child {completed_name} exited code={return_code}; "
                        f"aggregate_memory={memory.as_dict()}",
                        flush=True,
                    )
                    if completed_name == "research":
                        if return_code == 0:
                            research_recovery_failed_since = None
                            next_due["research"] = now + research_interval
                        else:
                            if research_overdue and research_recovery_failed_since is None:
                                research_recovery_failed_since = now
                            next_due["research"] = now + min(10.0, research_interval)
                    else:
                        next_due[completed_name] = now + intervals.get(completed_name, 30.0)
                    heavy = None
                    heavy_name = None
                    heavy_started_at = None
                    heavy_last_progress_at = None
                    heavy_research_heartbeat_marker = None
                    heavy_termination_requested_at = None
                elif heavy_termination_requested_at is not None:
                    if now - heavy_termination_requested_at >= HEAVY_TERMINATE_GRACE_SECONDS:
                        print(f"disposable {heavy_name} ignored SIGTERM; killing process", flush=True)
                        heavy.kill()
                elif research_job_stalled(
                    heavy_name=heavy_name,
                    last_progress_at=heavy_last_progress_at,
                    now=now,
                    timeout_seconds=research_job_timeout,
                ):
                    if research_recovery_failed_since is None:
                        research_recovery_failed_since = now
                    runtime_seconds = (
                        now - heavy_started_at if heavy_started_at is not None else research_job_timeout
                    )
                    print(
                        f"research disposable made no durable progress for {research_job_timeout:.1f}s; "
                        f"terminating stuck job after {runtime_seconds:.1f}s total runtime",
                        flush=True,
                    )
                    heavy_termination_requested_at = now
                    heavy.terminate()
                elif memory.terminate_required:
                    if heavy_name == "research" and research_overdue and research_recovery_failed_since is None:
                        research_recovery_failed_since = now
                    heavy_termination_requested_at = now
                    print(
                        f"aggregate memory reached terminate budget; stopping disposable {heavy_name}: {memory.as_dict()}",
                        flush=True,
                    )
                    heavy.terminate()
                elif memory.soft_exceeded:
                    print(f"aggregate memory above soft budget while {heavy_name} runs: {memory.as_dict()}", flush=True)
            else:
                if recovery_failure_exceeded(
                    research_overdue=research_overdue,
                    failed_since=research_recovery_failed_since,
                    now=now,
                    limit_seconds=recovery_failure_limit,
                ):
                    failed_for = now - float(research_recovery_failed_since or now)
                    print(
                        "research publication remains overdue through isolated recovery attempts "
                        f"for {failed_for:.1f}s; continuing fail-closed research recovery without "
                        f"restarting healthy permanent children; reason={research_overdue_reason}; "
                        f"memory={memory.as_dict()}",
                        flush=True,
                    )
                    # Rate-limit this durability warning while allowing another
                    # disposable research attempt. A stale isolated research plane is
                    # not a reason to take down a healthy API or portfolio child.
                    research_recovery_failed_since = now

                due_history = now >= next_due["history"]
                due_research = now >= next_due["research"]
                next_name = choose_heavy_job(
                    due_research=due_research,
                    due_history=due_history,
                    research_overdue=research_overdue,
                )
                if next_name is not None:
                    if memory.start_blocked:
                        if next_name == "research" and research_overdue:
                            if research_recovery_failed_since is None:
                                research_recovery_failed_since = now
                            next_due["research"] = now + min(10.0, research_interval)
                        else:
                            next_due[next_name] = now + min(30.0, intervals[next_name])
                        print(
                            f"deferring disposable {next_name}; aggregate memory start-blocked: {memory.as_dict()}",
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
                        heavy_started_at = now
                        heavy_last_progress_at = now if next_name == "research" else None
                        heavy_research_heartbeat_marker = (
                            latest_research_heartbeat_marker if next_name == "research" else None
                        )
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
