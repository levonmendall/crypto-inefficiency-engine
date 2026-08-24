from __future__ import annotations

import sys

from inefficiency_engine import render_combined_postbind as base


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
BOUNDED_HEARTBEAT_API_APP = "inefficiency_engine.read_api_bounded_heartbeat_deploy:app"


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


def main() -> int:
    install_source_repair_child_command()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
