from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Callable, Iterator

from inefficiency_engine.instance_memory import InstanceMemorySnapshot, instance_memory_snapshot


MEMORY_PRESSURE_EXIT_CODE = 75
DEFAULT_MEMORY_POLL_SECONDS = 0.5


class DisposableMemoryAdmissionDeferred(RuntimeError):
    """A disposable executor was not admitted because aggregate memory is too high."""

    def __init__(self, label: str, snapshot: InstanceMemorySnapshot):
        self.label = label
        self.snapshot = snapshot
        usage = "unknown" if snapshot.usage_mb is None else f"{snapshot.usage_mb:.1f} MiB"
        threshold = (
            "unknown"
            if snapshot.start_block_mb is None
            else f"{snapshot.start_block_mb:.1f} MiB"
        )
        super().__init__(
            f"{label} deferred before heavy imports: aggregate memory {usage}; "
            f"start-block threshold {threshold}"
        )


def require_disposable_memory_admission(
    label: str,
    *,
    snapshot_reader: Callable[[], InstanceMemorySnapshot] | None = None,
) -> InstanceMemorySnapshot:
    """Apply the existing aggregate start-block threshold before heavy imports."""

    reader = snapshot_reader or instance_memory_snapshot
    snapshot = reader()
    if snapshot.start_blocked:
        raise DisposableMemoryAdmissionDeferred(label, snapshot)
    return snapshot


def _watch_disposable_memory(
    label: str,
    stop: threading.Event,
    *,
    snapshot_reader: Callable[[], InstanceMemorySnapshot],
    exit_func: Callable[[int], object],
    poll_seconds: float,
) -> None:
    while not stop.wait(max(0.05, float(poll_seconds))):
        snapshot = snapshot_reader()
        if not snapshot.terminate_required:
            continue
        usage = "unknown" if snapshot.usage_mb is None else f"{snapshot.usage_mb:.1f} MiB"
        threshold = (
            "unknown"
            if snapshot.terminate_mb is None
            else f"{snapshot.terminate_mb:.1f} MiB"
        )
        print(
            f"{label} terminating disposable executor under aggregate memory pressure: "
            f"usage={usage} terminate_threshold={threshold}",
            flush=True,
        )
        exit_func(MEMORY_PRESSURE_EXIT_CODE)
        return


@contextmanager
def disposable_executor_memory_guard(
    label: str,
    *,
    snapshot_reader: Callable[[], InstanceMemorySnapshot] | None = None,
    exit_func: Callable[[int], object] | None = None,
    poll_seconds: float = DEFAULT_MEMORY_POLL_SECONDS,
) -> Iterator[InstanceMemorySnapshot]:
    """Protect a disposable executor without changing any configured memory threshold.

    Admission is checked before the caller imports its heavyweight application graph.
    Once admitted, a daemon watcher samples aggregate cgroup memory and hard-exits only
    this disposable interpreter with code 75 if the existing terminate threshold is
    crossed. That gives the API and permanent paper-control processes first claim on
    the Render instance instead of allowing an auxiliary executor to trigger an OOM
    kill of an arbitrary permanent process.
    """

    reader = snapshot_reader or instance_memory_snapshot
    terminate = exit_func or os._exit
    initial = require_disposable_memory_admission(label, snapshot_reader=reader)
    stop = threading.Event()
    watcher = threading.Thread(
        target=_watch_disposable_memory,
        args=(label, stop),
        kwargs={
            "snapshot_reader": reader,
            "exit_func": terminate,
            "poll_seconds": poll_seconds,
        },
        name=f"{label}-memory-guard",
        daemon=True,
    )
    watcher.start()
    try:
        yield initial
    finally:
        stop.set()
        watcher.join(timeout=max(0.1, min(1.0, float(poll_seconds) * 2.0)))
