from __future__ import annotations

import gc
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa

from inefficiency_engine.instance_memory import InstanceMemorySnapshot, instance_memory_snapshot
from inefficiency_engine.local_storage import local_storage_paths


DEFAULT_MARKET_BATCH_ROWS = 512
MAX_MARKET_BATCH_ROWS = 1_000
DEFAULT_COMPACTION_BATCH_ROWS = 8_192
MEMORY_PRESSURE_EXIT_CODE = 75
ALLOCATOR_TRIM_INTERVAL_SECONDS = 1.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def market_batch_rows() -> int:
    raw = os.getenv("CIE_STAGE_ONE_MARKET_BATCH_ROWS", str(DEFAULT_MARKET_BATCH_ROWS))
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_MARKET_BATCH_ROWS
    return max(64, min(MAX_MARKET_BATCH_ROWS, value))


def compaction_batch_rows() -> int:
    raw = os.getenv(
        "CIE_STAGE_ONE_COMPACTION_BATCH_ROWS",
        str(DEFAULT_COMPACTION_BATCH_ROWS),
    )
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_COMPACTION_BATCH_ROWS
    return max(1_024, min(32_768, value))


def release_unused_memory() -> None:
    """Return one-shot Stage-1 allocations to the service before Render must reclaim them."""

    gc.collect()
    try:
        pool = pa.default_memory_pool()
        release = getattr(pool, "release_unused", None)
        if callable(release):
            release()
    except Exception:
        # Memory trimming is a best-effort safety valve. Correctness continues to be
        # enforced by the durable migration checkpoint and fail-closed verification.
        pass


def _memory_status_path() -> Path:
    return local_storage_paths().migration / "market-memory-guard.json"


def _write_memory_status(
    snapshot: InstanceMemorySnapshot,
    *,
    state: str,
    checkpoint: object | None,
    high_water: object | None,
) -> None:
    path = _memory_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "state": state,
        "observed_at": _now(),
        "checkpoint": checkpoint,
        "high_water_primary_key": high_water,
        **snapshot.as_dict(),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str))
    os.replace(temporary, path)


def _market_checkpoint(payload: dict[str, Any]) -> tuple[object, object] | None:
    if str(payload.get("current_table") or "") != "market_quotes":
        return None
    tables = payload.get("tables")
    table = tables.get("market_quotes") if isinstance(tables, dict) else None
    if not isinstance(table, dict) or table.get("verified") is True:
        return None
    checkpoint = table.get("last_primary_key")
    high_water = table.get("high_water_primary_key")
    if not isinstance(checkpoint, list) or not checkpoint:
        return None
    if not isinstance(high_water, list) or not high_water:
        return None
    return checkpoint, high_water


def install_market_copy_guard(migration_module: Any) -> None:
    """Bound Stage-1 market copy allocations and shed the child before a 2GB OOM.

    The market copy checkpoint is published only after the Parquet/manifest append is
    durable.  The guarded publisher therefore checks aggregate cgroup memory *after*
    that checkpoint reaches disk. If memory remains above the service termination
    threshold after Python/Arrow trimming, the child exits with EX_TEMPFAIL (75).
    The production migration supervisor already treats an opaque market child exit at
    a durable monotonic checkpoint as restart-safe and resumes from that checkpoint.
    """

    if getattr(migration_module, "_stage_one_market_memory_guard_installed", False):
        return

    original_migrate = migration_module.migrate
    original_publish = migration_module._publish

    def bounded_migrate(source_url: str, *, batch_size: int = migration_module.BATCH_SIZE):
        return original_migrate(
            source_url,
            batch_size=min(max(1, int(batch_size)), market_batch_rows()),
        )

    def guarded_publish(payload: dict[str, Any], path: Path | None = None) -> None:
        original_publish(payload, path)
        marker = _market_checkpoint(payload)
        if marker is None:
            return
        checkpoint, high_water = marker
        release_unused_memory()
        snapshot = instance_memory_snapshot()
        state = "memory_pressure" if snapshot.terminate_required else "copying"
        try:
            _write_memory_status(
                snapshot,
                state=state,
                checkpoint=checkpoint,
                high_water=high_water,
            )
        except OSError:
            pass
        if snapshot.terminate_required:
            raise SystemExit(MEMORY_PRESSURE_EXIT_CODE)

    migration_module.migrate = bounded_migrate
    migration_module._publish = guarded_publish
    migration_module._stage_one_market_memory_guard_installed = True


@contextmanager
def allocator_trim_loop() -> Iterator[None]:
    """Continuously trim Arrow allocations during long copy and verification phases."""

    stop = threading.Event()

    def trim() -> None:
        while not stop.wait(ALLOCATOR_TRIM_INTERVAL_SECONDS):
            release_unused_memory()

    worker = threading.Thread(target=trim, name="stage-one-arrow-memory-trim", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=2.0)
        release_unused_memory()


__all__ = [
    "ALLOCATOR_TRIM_INTERVAL_SECONDS",
    "DEFAULT_COMPACTION_BATCH_ROWS",
    "DEFAULT_MARKET_BATCH_ROWS",
    "MEMORY_PRESSURE_EXIT_CODE",
    "allocator_trim_loop",
    "compaction_batch_rows",
    "install_market_copy_guard",
    "market_batch_rows",
    "release_unused_memory",
]
