from __future__ import annotations

import sys

from inefficiency_engine import render_combined_postbind as base


SOURCE_REPAIR_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.permanent_source_worker_lane_repair",
]


def install_source_repair_child_command() -> None:
    """Route only the source child through the lane-repair bootstrap wrapper."""

    if getattr(base.base, "_remaining_source_lane_repair_installed", False):
        return
    original = base.base._BASE_RUNTIME_CHILD_COMMANDS

    def repaired_commands(port: str | int) -> dict[str, list[str]]:
        commands = dict(original(port))
        commands["source"] = list(SOURCE_REPAIR_COMMAND)
        return commands

    base.base._BASE_RUNTIME_CHILD_COMMANDS = repaired_commands
    base.base._remaining_source_lane_repair_installed = True


def main() -> int:
    install_source_repair_child_command()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
