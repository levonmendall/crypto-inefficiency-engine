from __future__ import annotations

import json
import subprocess
import sys
import threading

from inefficiency_engine import render_combined_postbind as base
from inefficiency_engine.candidate_observatory_backfill_supervisor import (
    run_candidate_observatory_backfill_supervisor,
)
from inefficiency_engine.cycle_history_background_supervisor_repair import (
    run_cycle_history_background_supervisor,
)
from inefficiency_engine.research_projection_supervisor import (
    run_research_projection_supervisor,
)
from inefficiency_engine.runtime_watchdog_readiness_repair import (
    install_runtime_watchdog_readiness_repair,
)
from inefficiency_engine.source_coverage_history_migration_supervisor import (
    run_source_coverage_history_migration_supervisor,
)


SOURCE_REPAIR_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.permanent_source_worker_lane_repair",
]
# The outer combined runtime considers its portfolio child critical. Keep that permanent
# slot occupied by a tiny supervisor so a canonical portfolio/database failure recycles
# only the actual portfolio worker instead of returning from the combined parent and
# terminating the API plus every long-running background supervisor.
PORTFOLIO_BOUNDED_HEARTBEAT_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.portfolio_process_supervisor",
]
RESEARCH_OBSERVABILITY_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.disposable_heavy_job_research_observability",
]
CONTROL_TRUTH_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.permanent_control_worker_truth_repair",
]
RUNTIME_PARENT_HEARTBEAT_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.combined_runtime_parent_heartbeat",
]
RUNTIME_PARENT_TERMINAL_DEADLINE_SECONDS = 5.0
WORKER_HEARTBEAT_READ_INDEX_SPEC = {
    "worker_heartbeats": ("worker_id", "id"),
}
# Keep the canonical database-independent liveness app as the production ASGI target.
# It intercepts bounded explicit diagnostics after path selection, while /health remains
# a zero-database process-liveness branch.
BOUNDED_HEARTBEAT_API_APP = "inefficiency_engine.read_api_liveness_deploy:app"


def install_source_repair_child_command() -> None:
    """Install source-lane repair plus isolated portfolio and bounded diagnostics."""

    if getattr(base.base, "_remaining_source_lane_repair_installed", False):
        return
    original = base.base._BASE_RUNTIME_CHILD_COMMANDS

    def repaired_commands(port: str | int) -> dict[str, list[str]]:
        commands = dict(original(port))
        commands["source"] = list(SOURCE_REPAIR_COMMAND)
        commands["portfolio"] = list(PORTFOLIO_BOUNDED_HEARTBEAT_COMMAND)
        commands["api"] = [
            sys.executable,
            "-m",
            "uvicorn",
            BOUNDED_HEARTBEAT_API_APP,
            "--host",
            "0.0.0.0",
            "--port",
            str(port),
        ]
        return commands

    base.base._BASE_RUNTIME_CHILD_COMMANDS = repaired_commands
    base.base._remaining_source_lane_repair_installed = True


def install_control_truth_command() -> None:
    """Route only canonical control through truthful disposable diagnostics."""

    base.base.CONTROL_COMMAND = list(CONTROL_TRUTH_COMMAND)


def install_worker_heartbeat_read_index() -> None:
    """Keep the latest-worker index in the dedicated priority post-bind scope.

    The index is no longer appended to the generic background queue. It remains
    non-authoritative and post-bind, but is attempted before unrelated BRIN/source/
    strategy DDL so certification can use targeted newest-row seeks promptly.
    """

    base.PRIORITY_READ_INDEX_SPECS.update(WORKER_HEARTBEAT_READ_INDEX_SPEC)


def install_research_observability_heavy_command() -> None:
    """Route only disposable research through the observability/closure repair."""

    runtime = base.base._runtime
    if getattr(runtime, "_research_observability_repair_installed", False):
        return
    original = runtime.heavy_commands

    def repaired_heavy_commands() -> dict[str, list[str]]:
        commands = dict(original())
        commands["research"] = list(RESEARCH_OBSERVABILITY_COMMAND)
        return commands

    runtime.heavy_commands = repaired_heavy_commands
    runtime._research_observability_repair_installed = True


def _stop_diagnostic_child(child: subprocess.Popen[bytes] | None) -> None:
    if child is None or child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5.0)


def _record_parent_terminal(payload: dict[str, object]) -> None:
    """Best-effort terminal truth in a disposable process; never delay shutdown long."""

    try:
        subprocess.run(
            [
                *RUNTIME_PARENT_HEARTBEAT_COMMAND,
                "--terminal",
                json.dumps(payload, separators=(",", ":")),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=RUNTIME_PARENT_TERMINAL_DEADLINE_SECONDS,
        )
    except Exception:
        pass


def main() -> int:
    install_source_repair_child_command()
    install_control_truth_command()
    install_worker_heartbeat_read_index()
    install_research_observability_heavy_command()
    install_runtime_watchdog_readiness_repair()
    stop_event = threading.Event()
    cycle_history_guard = threading.Thread(
        target=run_cycle_history_background_supervisor,
        args=(stop_event,),
        name="cycle-history-background-supervisor",
        daemon=True,
    )
    observatory_backfill_guard = threading.Thread(
        target=run_candidate_observatory_backfill_supervisor,
        args=(stop_event,),
        name="candidate-observatory-backfill-supervisor",
        daemon=True,
    )
    source_history_guard = threading.Thread(
        target=run_source_coverage_history_migration_supervisor,
        args=(stop_event,),
        name="source-coverage-history-migration-supervisor",
        daemon=True,
    )
    research_projection_guard = threading.Thread(
        target=run_research_projection_supervisor,
        args=(stop_event,),
        name="research-projection-refresh-supervisor",
        daemon=True,
    )
    runtime_parent_heartbeat = subprocess.Popen(RUNTIME_PARENT_HEARTBEAT_COMMAND)
    cycle_history_guard.start()
    observatory_backfill_guard.start()
    source_history_guard.start()
    research_projection_guard.start()
    try:
        try:
            return_code = base.main()
        except BaseException as exc:
            _record_parent_terminal(
                {
                    "exit_reason": "combined_runtime_base_raised",
                    "return_code": None,
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:500],
                }
            )
            raise
        _record_parent_terminal(
            {
                "exit_reason": "combined_runtime_base_returned",
                "return_code": return_code,
                "error_type": None,
                "message": None,
            }
        )
        return return_code
    finally:
        stop_event.set()
        _stop_diagnostic_child(runtime_parent_heartbeat)
        cycle_history_guard.join(timeout=10.0)
        observatory_backfill_guard.join(timeout=10.0)
        source_history_guard.join(timeout=10.0)
        research_projection_guard.join(timeout=10.0)


if __name__ == "__main__":
    raise SystemExit(main())
