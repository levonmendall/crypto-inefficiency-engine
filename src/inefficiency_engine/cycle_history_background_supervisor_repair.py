from __future__ import annotations

import signal
import subprocess
import sys
import threading
import time
from typing import Any

from sqlalchemy import text

from inefficiency_engine import cycle_history_background_supervisor as base
from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_history_background_backfill_repair import (
    INDEX_NOT_READY_EXIT_CODE,
)
from inefficiency_engine.cycle_history_index_gate import cycle_history_exact_index_status
from inefficiency_engine.cycle_history_index_maintenance_child import (
    WORKER_ID as INDEX_WORKER_ID,
)
from inefficiency_engine.evidence import build_evidence_store
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
# Production pg_stat_progress_create_index proved the exact concurrent index was healthy
# and still scanning the large market_quotes table well past the pace compatible with a
# ten-minute deadline. Give the sole DDL owner one bounded hour plus a one-minute process
# margin for post-DDL catalog verification and clean shutdown. No history/backfill
# deadline, shared runtime-index timeout, or certification rule is changed here.
INDEX_EXECUTOR_DEADLINE_SECONDS = 3660.0
INDEX_RETRY_SECONDS = 30.0
INDEX_PROGRESS_HEARTBEAT_SECONDS = 15.0
INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD = 3
INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS = 120.0


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


def _index_store() -> Any | None:
    """Open the lightweight durable store used only for index supervision truth."""

    try:
        settings = Settings.from_env()
        return build_evidence_store(settings.evidence_db_path)
    except Exception as exc:
        print(
            f"cycle-history index supervisor durable store unavailable: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return None


def _safe_index_status(store: Any | None) -> dict[str, object] | None:
    if store is None:
        return None
    try:
        return dict(cycle_history_exact_index_status(store))
    except Exception:
        return None


def _latest_index_detail(store: Any | None) -> dict[str, object]:
    if store is None:
        return {}
    try:
        heartbeat = store.latest_worker_heartbeat(INDEX_WORKER_ID)
    except Exception:
        return {}
    if heartbeat is None:
        return {}
    detail = getattr(heartbeat, "detail", None)
    return dict(detail) if isinstance(detail, dict) else {}


def _postgres_index_progress(store: Any | None) -> dict[str, object]:
    """Best-effort read-only progress for the active market_quotes index build."""

    if store is None or getattr(store.engine.dialect, "name", "") != "postgresql":
        return {}
    query = text(
        """
        SELECT
            p.pid,
            p.command,
            p.phase,
            p.blocks_total,
            p.blocks_done,
            p.tuples_total,
            p.tuples_done,
            p.partitions_total,
            p.partitions_done,
            idx.relname AS index_name
        FROM pg_stat_progress_create_index AS p
        JOIN pg_class AS tbl ON tbl.oid = p.relid
        LEFT JOIN pg_class AS idx ON idx.oid = p.index_relid
        WHERE tbl.relname = 'market_quotes'
        ORDER BY p.pid
        LIMIT 1
        """
    )
    try:
        with store.engine.connect() as db:
            row = db.execute(query).mappings().first()
    except Exception:
        return {}
    if row is None:
        return {}
    return {str(key): value for key, value in row.items()}


def _record_index_supervisor_heartbeat(
    store: Any | None,
    *,
    state: str,
    stage: str,
    error_type: str | None = None,
    detail: dict[str, object] | None = None,
) -> None:
    """Publish parent-observed liveness without granting the parent DDL authority."""

    if store is None:
        return
    previous = _latest_index_detail(store)
    carried = {
        key: previous[key]
        for key in (
            "attempt_number",
            "previous_attempt_number",
            "previous_stage",
            "previous_error_type",
            "previous_message",
            "previous_effective_index_name",
            "statement_timeout_ms",
            "index_status",
            "current_index",
            "current_table",
            "current_index_runtime_seconds",
            "current_index_ok",
            "current_index_concurrent",
            "message",
            "effective_index_name",
        )
        if previous.get(key) is not None
    }
    try:
        store.record_worker_heartbeat(
            worker_id=INDEX_WORKER_ID,
            state=state,
            error_type=error_type,
            detail={
                "stage": stage,
                **carried,
                "supervisor_observation": True,
                "supervisor_executes_ddl": False,
                "dedicated_cycle_history_index_owner": True,
                "create_index_concurrently_required_in_postgres": True,
                "provider_requests_allowed": False,
                "provider_requests_used": 0,
                "qualification_thresholds_unchanged": True,
                "certification_authority": False,
                "allocation_authority": False,
                "live_execution_authority": False,
                "paper_only": True,
                **(detail or {}),
            },
        )
    except Exception:
        pass


def _index_child_exit_diagnostics(
    return_code: int | None,
    *,
    timed_out: bool,
) -> tuple[str | None, dict[str, object]]:
    """Classify what the parent can prove about a vanished disposable child."""

    detail: dict[str, object] = {
        "child_return_code": return_code,
        "child_timed_out": bool(timed_out),
        "process_termination_observed_by_supervisor": True,
    }
    if timed_out:
        return "IndexChildDeadlineExceeded", detail
    if return_code is None:
        return "IndexChildTerminationUnknown", detail
    if return_code < 0:
        signal_number = -int(return_code)
        try:
            signal_name = signal.Signals(signal_number).name
        except ValueError:
            signal_name = f"SIGNAL_{signal_number}"
        detail.update(
            {
                "termination_signal_number": signal_number,
                "termination_signal": signal_name,
                # SIGKILL is consistent with an OOM kill or external hard kill, but the
                # process return code alone cannot distinguish those causes.
                "possible_oom_or_external_kill": signal_name == "SIGKILL",
                "oom_kill_proven": False,
            }
        )
        return "IndexChildTerminatedBySignal", detail
    if return_code != 0:
        return "IndexChildExitedNonZero", detail
    return None, detail


def _run_index_child(
    store: Any | None,
    *,
    stop_event: threading.Event,
) -> tuple[int | None, bool]:
    """Run the sole DDL owner while the parent publishes independent liveness."""

    print(f"starting disposable child: {' '.join(INDEX_COMMAND)}", flush=True)
    child = subprocess.Popen(INDEX_COMMAND)
    started = time.monotonic()
    next_progress = started + INDEX_PROGRESS_HEARTBEAT_SECONDS
    timed_out = False

    while not stop_event.is_set() and child.poll() is None:
        now = time.monotonic()
        runtime_seconds = max(0.0, now - started)
        if runtime_seconds >= INDEX_EXECUTOR_DEADLINE_SECONDS:
            timed_out = True
            base._terminate(child)
            break
        if now >= next_progress:
            progress = _postgres_index_progress(store)
            index_status = _safe_index_status(store)
            _record_index_supervisor_heartbeat(
                store,
                state="running",
                stage="cycle_history_index_supervisor_observing",
                detail={
                    "child_pid": child.pid,
                    "child_runtime_seconds": runtime_seconds,
                    "executor_deadline_seconds": INDEX_EXECUTOR_DEADLINE_SECONDS,
                    "index_status": index_status,
                    "planner_usable_verified": (
                        index_status.get("planner_usable_verified")
                        if isinstance(index_status, dict)
                        else None
                    ),
                    "postgres_progress_available": bool(progress),
                    "postgres_index_progress": progress,
                },
            )
            next_progress = now + INDEX_PROGRESS_HEARTBEAT_SECONDS
        stop_event.wait(0.5)

    if stop_event.is_set():
        base._terminate(child)
    return child.poll(), timed_out


def run_cycle_history_background_supervisor(stop_event: threading.Event) -> None:
    """Verify the exact query index before any raw cycle-history reconstruction.

    The first child remains the sole owner of bounded PostgreSQL index maintenance. The
    lightweight supervisor independently verifies catalog readiness before every retry,
    publishes progress while the DDL call is blocking, and records exit-code/signal truth
    if the child disappears before it can publish its own terminal heartbeat. The second
    child owns only the existing checkpointed 180-day history slice.
    """

    port = base.os.getenv("PORT", "10000")
    while not stop_event.is_set() and not base._api_is_bound(port):
        stop_event.wait(base.API_BIND_POLL_SECONDS)
    if stop_event.is_set():
        return

    store = _index_store()
    exact_index_ready = False
    consecutive_terminal_failures = 0
    while not stop_event.is_set():
        if not exact_index_ready:
            # A prior child can be terminated after PostgreSQL completed the concurrent
            # build but before the child recorded success. Verify catalog truth first so
            # a valid replacement index is never rebuilt just because the process died.
            before = _safe_index_status(store)
            if before is not None and bool(before.get("ready")):
                exact_index_ready = True
                consecutive_terminal_failures = 0
                _record_index_supervisor_heartbeat(
                    store,
                    state="success",
                    stage="cycle_history_index_ready_observed_before_retry",
                    detail={
                        "index_status": before,
                        "ddl_retry_skipped": True,
                        "planner_usable_verified": before.get("planner_usable_verified"),
                    },
                )
            else:
                return_code, timed_out = _run_index_child(
                    store,
                    stop_event=stop_event,
                )
                if stop_event.is_set():
                    return

                # Always re-read planner/catalog truth after the child exits. A valid
                # index wins over an abnormal process exit because the gate is about the
                # access path, not the lifecycle of the disposable interpreter.
                after = _safe_index_status(store)
                if after is not None and bool(after.get("ready")):
                    error_type, exit_detail = _index_child_exit_diagnostics(
                        return_code,
                        timed_out=timed_out,
                    )
                    exact_index_ready = True
                    consecutive_terminal_failures = 0
                    _record_index_supervisor_heartbeat(
                        store,
                        state="success",
                        stage="cycle_history_index_ready_observed_after_child_exit",
                        detail={
                            **exit_detail,
                            "child_exit_error_type": error_type,
                            "index_status": after,
                            "planner_usable_verified": after.get(
                                "planner_usable_verified"
                            ),
                            "ddl_retry_skipped": True,
                        },
                    )
                elif return_code == INDEX_NOT_READY_EXIT_CODE and not timed_out:
                    # The child completed normally and already published the concrete SQL
                    # failure/retry detail. Preserve that terminal heartbeat verbatim.
                    stop_event.wait(INDEX_RETRY_SECONDS)
                    continue
                elif timed_out or return_code not in (0, None):
                    error_type, exit_detail = _index_child_exit_diagnostics(
                        return_code,
                        timed_out=timed_out,
                    )
                    consecutive_terminal_failures += 1
                    retry_seconds = (
                        INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS
                        if consecutive_terminal_failures
                        >= INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD
                        else INDEX_RETRY_SECONDS
                    )
                    _record_index_supervisor_heartbeat(
                        store,
                        state="degraded",
                        stage="cycle_history_index_child_terminated",
                        error_type=error_type,
                        detail={
                            **exit_detail,
                            "index_status": after,
                            "consecutive_terminal_failures": consecutive_terminal_failures,
                            "retry_seconds": retry_seconds,
                            "retry_backoff_escalated": retry_seconds
                            > INDEX_RETRY_SECONDS,
                        },
                    )
                    print(
                        "cycle-history exact-index child terminated "
                        f"code={return_code} timed_out={timed_out}; "
                        f"retrying in {retry_seconds:.0f}s",
                        flush=True,
                    )
                    stop_event.wait(retry_seconds)
                    continue
                elif after is not None:
                    consecutive_terminal_failures += 1
                    _record_index_supervisor_heartbeat(
                        store,
                        state="degraded",
                        stage="cycle_history_index_child_returned_without_ready_index",
                        error_type="IndexChildReturnedWithoutPlannerUsableIndex",
                        detail={
                            "child_return_code": return_code,
                            "index_status": after,
                            "consecutive_terminal_failures": consecutive_terminal_failures,
                            "retry_seconds": INDEX_RETRY_SECONDS,
                        },
                    )
                    stop_event.wait(INDEX_RETRY_SECONDS)
                    continue
                else:
                    # If durable verification itself is unavailable, preserve legacy
                    # behavior only for a clean child exit; any nonzero exit is handled
                    # above and never silently promoted.
                    exact_index_ready = return_code in (0, None) and not timed_out

            if exact_index_ready:
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
    "INDEX_PROGRESS_HEARTBEAT_SECONDS",
    "INDEX_RETRY_SECONDS",
    "INDEX_TERMINAL_FAILURE_BACKOFF_SECONDS",
    "INDEX_TERMINAL_FAILURE_BACKOFF_THRESHOLD",
    "run_cycle_history_background_supervisor",
]
