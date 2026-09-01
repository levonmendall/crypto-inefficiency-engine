from __future__ import annotations

import threading

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair
from inefficiency_engine import local_persistence_migration_supervisor_soft_resume as soft_resume
from inefficiency_engine.instance_memory import InstanceMemorySnapshot


def _snapshot(usage: float) -> InstanceMemorySnapshot:
    return InstanceMemorySnapshot(
        usage_mb=usage,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="cgroup_v2",
    )


def test_soft_resume_gate_requires_existing_soft_headroom() -> None:
    normal = _snapshot(1500.0)
    assert normal.start_blocked is False

    resume = soft_resume._SoftResumeSnapshot.from_snapshot(normal)
    assert resume.start_block_mb == 1587.2
    assert resume.soft_mb == 1433.6
    assert resume.start_blocked is True

    recovered = soft_resume._SoftResumeSnapshot.from_snapshot(_snapshot(1400.0))
    assert recovered.start_blocked is False


def test_soft_resume_telemetry_preserves_normal_thresholds() -> None:
    snapshot = soft_resume._SoftResumeSnapshot.from_snapshot(_snapshot(1500.0))
    fields = soft_resume._memory_status_fields(
        {
            "observed_at": "2026-09-01T16:23:06+00:00",
            "checkpoint": [2996655],
            "high_water_primary_key": [3094848],
            "usage_mb": 1997.7,
            "terminate_mb": 1689.6,
        },
        snapshot,
    )

    assert fields["memory_pressure_current_start_block_mb"] == 1587.2
    assert fields["memory_pressure_current_terminate_mb"] == 1689.6
    assert fields["memory_pressure_resume_gate"] == "existing_soft_threshold"
    assert fields["memory_pressure_resume_gate_mb"] == 1433.6
    assert fields["memory_pressure_current_soft_mb"] == 1433.6


def test_supervisor_wrapper_scopes_and_restores_soft_resume_shims(monkeypatch) -> None:
    original_snapshot = repair.instance_memory_snapshot
    original_fields = repair._memory_status_fields
    original_publish = repair._publish_repair_status
    observed: dict[str, object] = {}

    monkeypatch.setattr(soft_resume, "_instance_memory_snapshot", lambda: _snapshot(1500.0))

    def fake_run(_stop_event: threading.Event) -> None:
        observed["snapshot"] = repair.instance_memory_snapshot()
        observed["fields_is_shim"] = repair._memory_status_fields is soft_resume._memory_status_fields
        observed["publish_is_shim"] = repair._publish_repair_status is soft_resume._publish_repair_status

    monkeypatch.setattr(repair, "run_local_persistence_migration_supervisor", fake_run)

    soft_resume.run_local_persistence_migration_supervisor(threading.Event())

    resume_snapshot = observed["snapshot"]
    assert isinstance(resume_snapshot, soft_resume._SoftResumeSnapshot)
    assert resume_snapshot.start_blocked is True
    assert observed["fields_is_shim"] is True
    assert observed["publish_is_shim"] is True
    assert repair.instance_memory_snapshot is original_snapshot
    assert repair._memory_status_fields is original_fields
    assert repair._publish_repair_status is original_publish
