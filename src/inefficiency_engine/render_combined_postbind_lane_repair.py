from __future__ import annotations

import sys
import threading

from inefficiency_engine import render_combined_postbind as base
from inefficiency_engine.candidate_observatory_backfill_supervisor import (
    run_candidate_observatory_backfill_supervisor,
)
from inefficiency_engine.cycle_history_background_supervisor import (
    run_cycle_history_background_supervisor,
)
from inefficiency_engine.research_projection_supervisor import (
    run_research_projection_supervisor,
)


SOURCE_REPAIR_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.permanent_source_worker_lane_repair",
]
PORTFOLIO_BOUNDED_HEARTBEAT_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.lightweight_portfolio_worker_bounded_heartbeat",
]
RESEARCH_OBSERVABILITY_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.disposable_heavy_job_research_observability",
]
# The outer ASGI liveness boundary guarantees Render's /health probe never enters
# PostgreSQL-backed runtime diagnostics. /ready and every diagnostic route remain on
# the full mobile-truth application behind that wrapper.
BOUNDED_HEARTBEAT_API_APP = "inefficiency_engine.read_api_liveness_deploy:app"


def install_source_repair_child_command() -> None:
    """Install source-lane repair plus bounded diagnostic heartbeat reads."""

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


def main() -> int:
    install_source_repair_child_command()
    install_research_observability_heavy_command()
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
    research_projection_guard = threading.Thread(
        target=run_research_projection_supervisor,
        args=(stop_event,),
        name="research-projection-refresh-supervisor",
        daemon=True,
    )
    cycle_history_guard.start()
    observatory_backfill_guard.start()
    research_projection_guard.start()
    try:
        return base.main()
    finally:
        stop_event.set()
        cycle_history_guard.join(timeout=10.0)
        observatory_backfill_guard.join(timeout=10.0)
        research_projection_guard.join(timeout=10.0)


if __name__ == "__main__":
    raise SystemExit(main())
