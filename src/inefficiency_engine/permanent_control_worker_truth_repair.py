from __future__ import annotations

import sys
from typing import Any

from inefficiency_engine import control_cycle_runtime
from inefficiency_engine import permanent_control_worker as base


CONTROL_EXECUTOR_COMMAND = [
    sys.executable,
    "-m",
    "inefficiency_engine.control_cycle_executor_truth_repair",
]
_ORIGINAL_RUN_CYCLE = control_cycle_runtime.ControlExecutorSupervisor.run_cycle


def _run_cycle_with_truthful_executor(
    self: Any,
    *,
    sequence: int,
    command=None,
    heartbeat=None,
    environment=None,
):
    return _ORIGINAL_RUN_CYCLE(
        self,
        sequence=sequence,
        command=(list(CONTROL_EXECUTOR_COMMAND) if command is None else command),
        heartbeat=heartbeat,
        environment=environment,
    )


def main() -> int:
    """Keep canonical control unchanged except for its disposable executor module."""

    control_cycle_runtime.ControlExecutorSupervisor.run_cycle = (
        _run_cycle_with_truthful_executor
    )
    return int(base.main())


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CONTROL_EXECUTOR_COMMAND", "main"]
