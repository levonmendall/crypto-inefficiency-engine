from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Mapping, Sequence

from inefficiency_engine.process_tree import (
    process_tree_alive,
    signal_process_tree,
    subprocess_group_kwargs,
)


class ControlCycleDeadlineExceeded(TimeoutError):
    """Raised when one synchronous canonical-control cycle exceeds its wall-clock budget."""


@dataclass(frozen=True)
class ControlExecutorResult:
    """Terminal result from one externally supervised canonical-control cycle."""

    ok: bool
    parent_generation: str
    parent_sequence: int
    executor_pid: int
    executor_cycle_id: str
    return_code: int | None
    error_type: str | None
    executor_runtime_seconds: float
    executor_last_stage: str
    executor_stage_observed_at: str | None
    executor_deadline_seconds: float
    executor_terminated: bool
    executor_killed: bool
    retry_count: int
    payload: dict[str, object] = field(default_factory=dict)
    historical_cache_progress: dict[str, object] = field(default_factory=dict)
    historical_cache_complete: bool = False

    def telemetry(self) -> dict[str, object]:
        result = "success" if self.ok else "timeout" if self.error_type == "ControlExecutorDeadlineExceeded" else "error"
        return {
            "parent_process_identity": f"{os.getpid()}:{self.parent_generation}",
            "parent_pid": os.getpid(),
            "parent_generation": self.parent_generation,
            "parent_sequence": self.parent_sequence,
            "parent_heartbeat_current": True,
            "executor_pid": self.executor_pid,
            "executor_cycle_id": self.executor_cycle_id,
            "executor_current_stage": self.executor_last_stage,
            "executor_stage_observed_at": self.executor_stage_observed_at,
            "executor_age_seconds": self.executor_runtime_seconds,
            "executor_deadline_seconds": self.executor_deadline_seconds,
            "last_executor_result": result,
            "last_executor_error_type": self.error_type,
            "last_executor_runtime_seconds": self.executor_runtime_seconds,
            "executor_last_stage_before_failure": (
                self.executor_last_stage if not self.ok else None
            ),
            "executor_terminated": self.executor_terminated,
            "executor_killed": self.executor_killed,
            "retry_count": self.retry_count,
            "historical_cache_progress": self.historical_cache_progress,
            "historical_cache_complete": self.historical_cache_complete,
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        }


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _terminate_executor(
    process: subprocess.Popen[bytes],
    *,
    grace_seconds: float,
) -> tuple[bool, bool]:
    """Terminate the executor process tree and escalate without touching its parent."""

    if not process_tree_alive(process):
        return False, False
    terminated = signal_process_tree(process, signal.SIGTERM)
    killed = False
    deadline = time.monotonic() + max(0.0, float(grace_seconds))
    while process_tree_alive(process) and time.monotonic() < deadline:
        time.sleep(0.01)
    if process_tree_alive(process):
        killed = signal_process_tree(process, signal.SIGKILL)
    try:
        process.wait(timeout=max(0.1, float(grace_seconds) + 0.1))
    except subprocess.TimeoutExpired:
        if process_tree_alive(process):
            killed = signal_process_tree(process, signal.SIGKILL) or killed
        process.wait(timeout=1.0)
    return bool(terminated), bool(killed)


def _linux_parent_death_signal() -> None:
    """Ask Linux to SIGKILL the executor if its supervising parent disappears."""

    if not sys.platform.startswith("linux"):
        return
    import ctypes

    pr_set_pdeathsig = 1
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(pr_set_pdeathsig, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    # Close the fork/prctl race: if the parent exited before prctl completed, the
    # kernel could not deliver the configured signal retroactively.
    if os.getppid() == 1:
        os.kill(os.getpid(), signal.SIGKILL)


class ControlExecutorSupervisor:
    """Keep the control parent alive while one disposable cycle runs in a child.

    The executor is an operating-system process, not a thread or in-process signal
    boundary. A blocked Python/DBAPI/native call can therefore be terminated without
    resetting the parent generation or its monotonically increasing sequence.
    """

    def __init__(
        self,
        *,
        deadline_seconds: float,
        heartbeat_interval_seconds: float = 2.0,
        terminate_grace_seconds: float = 2.0,
        workspace: Path | None = None,
    ) -> None:
        self.deadline_seconds = max(0.01, float(deadline_seconds))
        self.heartbeat_interval_seconds = max(
            0.01,
            min(float(heartbeat_interval_seconds), self.deadline_seconds),
        )
        self.terminate_grace_seconds = max(0.0, float(terminate_grace_seconds))
        self.workspace = workspace
        self.parent_generation = uuid.uuid4().hex
        self.retry_count = 0
        self.last_result: ControlExecutorResult | None = None

    def _running_telemetry(
        self,
        *,
        sequence: int,
        cycle_id: str,
        pid: int,
        started: float,
        status: Mapping[str, object],
    ) -> dict[str, object]:
        previous = self.last_result.telemetry() if self.last_result is not None else {}
        return {
            "parent_process_identity": f"{os.getpid()}:{self.parent_generation}",
            "parent_pid": os.getpid(),
            "parent_generation": self.parent_generation,
            "parent_sequence": int(sequence),
            "parent_heartbeat_current": True,
            "executor_pid": int(pid),
            "executor_cycle_id": cycle_id,
            "executor_current_stage": str(
                status.get("stage") or "control_executor_starting"
            ),
            "executor_stage_observed_at": status.get("observed_at"),
            "executor_age_seconds": max(0.0, time.monotonic() - started),
            "executor_deadline_seconds": self.deadline_seconds,
            "last_executor_result": previous.get("last_executor_result"),
            "last_executor_error_type": previous.get("last_executor_error_type"),
            "last_executor_runtime_seconds": previous.get(
                "last_executor_runtime_seconds"
            ),
            "executor_last_stage_before_failure": previous.get(
                "executor_last_stage_before_failure"
            ),
            "executor_terminated": False,
            "executor_killed": False,
            "retry_count": self.retry_count,
            "historical_cache_progress": status.get(
                "historical_cache_progress",
                previous.get("historical_cache_progress", {}),
            ),
            "historical_cache_complete": bool(
                status.get(
                    "historical_cache_complete",
                    previous.get("historical_cache_complete", False),
                )
            ),
            "provider_requests_allowed": False,
            "provider_requests_used": 0,
            "paper_only": True,
        }

    def run_cycle(
        self,
        *,
        sequence: int,
        command: Sequence[str] | None = None,
        heartbeat: Callable[[dict[str, object]], None] | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> ControlExecutorResult:
        cycle_id = uuid.uuid4().hex
        base_dir = str(self.workspace) if self.workspace is not None else None
        with tempfile.TemporaryDirectory(
            prefix="cie-control-executor-",
            dir=base_dir,
        ) as directory:
            status_path = Path(directory) / "status.json"
            result_path = Path(directory) / "result.json"
            child_env = os.environ.copy()
            if environment is not None:
                child_env.update({str(key): str(value) for key, value in environment.items()})
            child_env.update(
                {
                    "CIE_CONTROL_EXECUTOR_CYCLE_ID": cycle_id,
                    "CIE_CONTROL_EXECUTOR_SEQUENCE": str(int(sequence)),
                    "CIE_CONTROL_EXECUTOR_STATUS_PATH": str(status_path),
                    "CIE_CONTROL_EXECUTOR_RESULT_PATH": str(result_path),
                }
            )
            child_command = list(
                command
                or [sys.executable, "-m", "inefficiency_engine.control_cycle_executor"]
            )
            started = time.monotonic()
            process = subprocess.Popen(
                child_command,
                env=child_env,
                preexec_fn=(
                    _linux_parent_death_signal
                    if sys.platform.startswith("linux")
                    else None
                ),
                **subprocess_group_kwargs(),
            )
            last_status: dict[str, object] = {}
            terminated = False
            killed = False
            timed_out = False

            while True:
                status = _read_json(status_path)
                if status:
                    last_status = status
                return_code = process.poll()
                if return_code is not None:
                    break
                elapsed = time.monotonic() - started
                if elapsed >= self.deadline_seconds:
                    timed_out = True
                    terminated, killed = _terminate_executor(
                        process,
                        grace_seconds=self.terminate_grace_seconds,
                    )
                    return_code = process.poll()
                    break
                if heartbeat is not None:
                    heartbeat(
                        self._running_telemetry(
                            sequence=sequence,
                            cycle_id=cycle_id,
                            pid=process.pid,
                            started=started,
                            status=last_status,
                        )
                    )
                remaining = max(0.01, self.deadline_seconds - elapsed)
                time.sleep(min(self.heartbeat_interval_seconds, remaining))

            runtime = max(0.0, time.monotonic() - started)
            payload = _read_json(result_path)
            last_stage = str(
                last_status.get("stage")
                or payload.get("stage")
                or "control_executor_starting"
            )
            if timed_out:
                ok = False
                error_type = "ControlExecutorDeadlineExceeded"
            elif int(return_code or 0) != 0:
                ok = False
                error_type = str(
                    payload.get("error_type") or "ControlExecutorExitedNonzero"
                )
            elif not payload:
                ok = False
                error_type = "ControlExecutorResultMissing"
            else:
                ok = bool(payload.get("ok"))
                error_type = None if ok else str(
                    payload.get("error_type") or "ControlExecutorFailed"
                )

            if ok:
                self.retry_count = 0
            else:
                self.retry_count += 1
            progress = payload.get("historical_cache_progress")
            if not isinstance(progress, dict):
                progress = last_status.get("historical_cache_progress")
            if not isinstance(progress, dict):
                progress = {}
            cache_complete = bool(
                payload.get(
                    "historical_cache_complete",
                    last_status.get("historical_cache_complete", False),
                )
            )
            result = ControlExecutorResult(
                ok=ok,
                parent_generation=self.parent_generation,
                parent_sequence=int(sequence),
                executor_pid=process.pid,
                executor_cycle_id=cycle_id,
                return_code=return_code,
                error_type=error_type,
                executor_runtime_seconds=runtime,
                executor_last_stage=last_stage,
                executor_stage_observed_at=(
                    str(last_status.get("observed_at"))
                    if last_status.get("observed_at") is not None
                    else None
                ),
                executor_deadline_seconds=self.deadline_seconds,
                executor_terminated=terminated,
                executor_killed=killed,
                retry_count=self.retry_count,
                payload=payload,
                historical_cache_progress=progress,
                historical_cache_complete=cache_complete,
            )
            self.last_result = result
            return result


def hard_control_deadline_supported() -> bool:
    """Return whether this process can enforce a wall-clock deadline on the main thread."""

    return bool(
        threading.current_thread() is threading.main_thread()
        and hasattr(signal, "SIGALRM")
        and hasattr(signal, "ITIMER_REAL")
        and hasattr(signal, "setitimer")
    )


@contextmanager
def hard_control_cycle_deadline(seconds: float) -> Iterator[bool]:
    """Interrupt synchronous control work without leaving an executor thread behind.

    Canonical control is a dedicated Linux process in production. Keeping durable
    reconciliation on that process's main thread lets a process-local real-time alarm
    interrupt Python work, while PostgreSQL ``statement_timeout`` remains the primary
    bound for SQL already executing in the database. Unlike ``asyncio.to_thread``
    cancellation, no reconciliation thread survives after this context exits.
    """

    budget = max(0.001, float(seconds))
    if not hard_control_deadline_supported():
        yield False
        return

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)

    def _raise_deadline(_signum, _frame) -> None:
        raise ControlCycleDeadlineExceeded(
            f"canonical control cycle exceeded {budget:.3f}s wall-clock deadline"
        )

    signal.signal(signal.SIGALRM, _raise_deadline)
    signal.setitimer(signal.ITIMER_REAL, budget)
    try:
        yield True
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_delay > 0.0:
            signal.setitimer(signal.ITIMER_REAL, previous_delay, previous_interval)


def install_control_pool_checkout_timeout(store, *, timeout_seconds: float) -> bool:
    """Bound PostgreSQL QueuePool waits below the canonical-control cycle deadline.

    PostgreSQL statement/lock timeouts begin only after a connection is checked out.
    A saturated SQLAlchemy QueuePool can otherwise wait longer than the whole control
    budget. This process-local adjustment does not alter other workers or database
    semantics; it only bounds how long canonical control waits for a connection.
    """

    engine = store.engine
    if str(getattr(engine.dialect, "name", "")) != "postgresql":
        return False
    pool = getattr(engine, "pool", None)
    if pool is None or not hasattr(pool, "_timeout"):
        return False
    pool._timeout = max(0.25, float(timeout_seconds))
    return True
