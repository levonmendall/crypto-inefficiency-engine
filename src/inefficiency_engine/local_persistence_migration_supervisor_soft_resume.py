from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair
from inefficiency_engine.instance_memory import (
    InstanceMemorySnapshot,
    instance_memory_snapshot as _instance_memory_snapshot,
)


MEMORY_PRESSURE_RESUME_GATE = "existing_soft_threshold"
_ORIGINAL_MEMORY_STATUS_FIELDS = repair._memory_status_fields
_ORIGINAL_PUBLISH_REPAIR_STATUS = repair._publish_repair_status


@dataclass(frozen=True)
class _SoftResumeSnapshot:
    """Expose the existing soft threshold only as the proven-resume admission gate.

    Normal instance memory semantics remain unchanged. The wrapped supervisor still
    reports the real start-block and terminate thresholds; only its `start_blocked`
    predicate is narrowed for a proven checkpointed code-75 memory-pressure restart.
    """

    usage_mb: float | None
    limit_mb: float | None
    soft_mb: float | None
    start_block_mb: float | None
    terminate_mb: float | None
    source: str

    @classmethod
    def from_snapshot(cls, snapshot: InstanceMemorySnapshot) -> "_SoftResumeSnapshot":
        return cls(
            usage_mb=snapshot.usage_mb,
            limit_mb=snapshot.limit_mb,
            soft_mb=snapshot.soft_mb,
            start_block_mb=snapshot.start_block_mb,
            terminate_mb=snapshot.terminate_mb,
            source=snapshot.source,
        )

    @property
    def start_blocked(self) -> bool:
        return bool(
            self.usage_mb is not None
            and self.soft_mb is not None
            and self.usage_mb >= self.soft_mb
        )


def _soft_resume_snapshot() -> _SoftResumeSnapshot:
    return _SoftResumeSnapshot.from_snapshot(_instance_memory_snapshot())


def _memory_status_fields(marker: dict[str, Any], snapshot: Any) -> dict[str, object]:
    fields = _ORIGINAL_MEMORY_STATUS_FIELDS(marker, snapshot)
    fields.update(
        memory_pressure_resume_gate=MEMORY_PRESSURE_RESUME_GATE,
        memory_pressure_resume_gate_mb=snapshot.soft_mb,
        memory_pressure_current_soft_mb=snapshot.soft_mb,
    )
    return fields


def _publish_repair_status(**kwargs: Any) -> None:
    reason = str(kwargs.get("reason") or "")
    if reason == "market_memory_pressure_wait":
        kwargs["error"] = (
            "waiting for aggregate instance memory to fall below the existing soft "
            "threshold before restarting checkpointed Stage 1 market work"
        )
    elif reason == "market_memory_pressure_headroom_recovered":
        kwargs["error"] = (
            "aggregate instance memory is below the existing soft threshold; resuming "
            "from the durable market checkpoint"
        )
    _ORIGINAL_PUBLISH_REPAIR_STATUS(**kwargs)


def run_local_persistence_migration_supervisor(stop_event: threading.Event) -> None:
    """Run the existing fail-closed supervisor with a lower proven-resume admission gate.

    Production proved a fresh Stage 1 child needs more than the 102.4 MiB gap between
    the existing start-block and terminate thresholds before it can reach the next
    durable market checkpoint. For a *proven* checkpointed PR #330 code-75 exit only,
    wait until aggregate usage is below the already-existing soft threshold. The
    terminate threshold, ordinary start-block threshold, retry ceiling, retry delays,
    checkpoint/high-water, copy shape, verification rules, and authority gates are not
    changed.
    """

    original_snapshot = repair.instance_memory_snapshot
    original_fields = repair._memory_status_fields
    original_publish = repair._publish_repair_status
    repair.instance_memory_snapshot = _soft_resume_snapshot
    repair._memory_status_fields = _memory_status_fields
    repair._publish_repair_status = _publish_repair_status
    try:
        repair.run_local_persistence_migration_supervisor(stop_event)
    finally:
        repair.instance_memory_snapshot = original_snapshot
        repair._memory_status_fields = original_fields
        repair._publish_repair_status = original_publish


def migration_preflight():
    return repair.migration_preflight()


def migration_status_payload() -> dict[str, object]:
    payload = repair.migration_status_payload()
    try:
        status_path, _, _, _, _ = repair.base._paths()
        supervisor = repair.base._read_json(status_path)
    except OSError:
        supervisor = {}
    for field in (
        "memory_pressure_resume_gate",
        "memory_pressure_resume_gate_mb",
        "memory_pressure_current_soft_mb",
    ):
        payload[f"supervisor_{field}"] = supervisor.get(field)
    return payload


__all__ = [
    "MEMORY_PRESSURE_RESUME_GATE",
    "migration_preflight",
    "migration_status_payload",
    "run_local_persistence_migration_supervisor",
]
