from __future__ import annotations

import json

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair
from inefficiency_engine.instance_memory import InstanceMemorySnapshot


def _progress() -> dict[str, object]:
    return {
        "state": "running",
        "current_table": "market_quotes",
        "tables": {
            "market_quotes": {
                "migration_mode": "captured_primary_key_high_water",
                "verified": False,
                "last_primary_key": [2996655],
                "high_water_primary_key": [3094848],
                "source_rows": 3064204,
                "source_lineage_count": 3056585,
            }
        },
    }


def _status(*, state: str = "failed") -> dict[str, object]:
    return {
        "state": state,
        "supervisor_reason": "migration_child_failed" if state == "failed" else "done",
        "child_return_code": 75 if state == "failed" else 0,
        "supervisor_started_at": "2026-09-01T04:39:42+00:00",
    }


def _marker(*, observed_at: str = "2026-09-01T04:40:00+00:00", checkpoint=None):
    return {
        "state": "memory_pressure",
        "observed_at": observed_at,
        "checkpoint": checkpoint or [2996655],
        "high_water_primary_key": [3094848],
        "usage_mb": 1720.0,
        "terminate_mb": 1689.6,
    }


def _snapshot(usage: float) -> InstanceMemorySnapshot:
    return InstanceMemorySnapshot(
        usage_mb=usage,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="cgroup_v2",
    )


def test_memory_pressure_evidence_requires_fresh_exact_checkpoint(monkeypatch, tmp_path) -> None:
    marker_path = tmp_path / "market-memory-guard.json"
    marker_path.write_text(json.dumps(_marker()))
    monkeypatch.setattr(repair, "_memory_pressure_status_path", lambda: marker_path)

    proven = repair._proven_market_memory_pressure(_status(), _progress())
    assert proven is not None
    assert proven["checkpoint"] == [2996655]

    marker_path.write_text(json.dumps(_marker(checkpoint=[2996000])))
    assert repair._proven_market_memory_pressure(_status(), _progress()) is None

    marker_path.write_text(
        json.dumps(_marker(observed_at="2026-09-01T04:39:00+00:00"))
    )
    assert repair._proven_market_memory_pressure(_status(), _progress()) is None


def test_proven_memory_exit_waits_for_headroom_without_expanding_restart_ceiling(
    monkeypatch, tmp_path
) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    stderr_path = tmp_path / "stderr.log"
    marker_path = tmp_path / "market-memory-guard.json"
    progress_path.write_text(json.dumps(_progress()))
    marker_path.write_text(json.dumps(_marker()))
    stderr_path.write_text(
        "old traceback: FATAL: the database system is in recovery mode\n"
    )

    monkeypatch.setattr(
        repair.base,
        "_paths",
        lambda: (
            tmp_path / "status.json",
            progress_path,
            tmp_path / "lock",
            tmp_path / "stdout.log",
            stderr_path,
        ),
    )
    monkeypatch.setattr(repair, "_memory_pressure_status_path", lambda: marker_path)
    monkeypatch.setattr(repair, "_prepare_market_history_layout", lambda path: None)
    monkeypatch.setattr(repair, "_recover_market_history_inode_pressure", lambda progress: None)
    monkeypatch.setattr(repair, "_run_base_supervisor_with_coarse_command", lambda stop: None)

    statuses = iter([_status(), _status(state="verified")])
    monkeypatch.setattr(repair.base, "migration_status_payload", lambda: next(statuses))

    snapshots = iter([_snapshot(1700.0), _snapshot(1500.0)])
    monkeypatch.setattr(repair, "instance_memory_snapshot", lambda: next(snapshots))

    published: list[dict[str, object]] = []
    monkeypatch.setattr(
        repair,
        "_publish_repair_status",
        lambda **kwargs: published.append(kwargs),
    )

    class Stop:
        def __init__(self) -> None:
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> bool:
            self.waits.append(seconds)
            return False

    stop = Stop()
    repair.run_local_persistence_migration_supervisor(stop)  # type: ignore[arg-type]

    reasons = [str(item["reason"]) for item in published]
    assert reasons == [
        "market_memory_pressure_wait",
        "market_memory_pressure_headroom_recovered",
    ]
    assert stop.waits == [repair.MEMORY_HEADROOM_POLL_SECONDS, 1.0]
    assert all(item["opaque_child_restarts"] == 1 for item in published)
    assert all(item["stderr_tail"] is None for item in published)
    assert all(item["progress"]["tables"]["market_quotes"]["last_primary_key"] == [2996655] for item in published)
    assert all(item["progress"]["tables"]["market_quotes"]["high_water_primary_key"] == [3094848] for item in published)


def test_unproven_code_75_keeps_existing_opaque_path(monkeypatch, tmp_path) -> None:
    progress_path = tmp_path / "postgres-import-progress.json"
    stderr_path = tmp_path / "stderr.log"
    marker_path = tmp_path / "market-memory-guard.json"
    progress_path.write_text(json.dumps(_progress()))
    marker_path.write_text(json.dumps(_marker(checkpoint=[2996000])))
    stderr_path.write_text("old postgres recovery traceback")

    monkeypatch.setattr(
        repair.base,
        "_paths",
        lambda: (
            tmp_path / "status.json",
            progress_path,
            tmp_path / "lock",
            tmp_path / "stdout.log",
            stderr_path,
        ),
    )
    monkeypatch.setattr(repair, "_memory_pressure_status_path", lambda: marker_path)
    monkeypatch.setattr(repair, "_prepare_market_history_layout", lambda path: None)
    monkeypatch.setattr(repair, "_recover_market_history_inode_pressure", lambda progress: None)
    monkeypatch.setattr(repair, "_run_base_supervisor_with_coarse_command", lambda stop: None)
    statuses = iter([_status(), _status(state="verified")])
    monkeypatch.setattr(repair.base, "migration_status_payload", lambda: next(statuses))

    published: list[dict[str, object]] = []
    monkeypatch.setattr(
        repair,
        "_publish_repair_status",
        lambda **kwargs: published.append(kwargs),
    )

    class Stop:
        def __init__(self) -> None:
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> bool:
            self.waits.append(seconds)
            return False

    stop = Stop()
    repair.run_local_persistence_migration_supervisor(stop)  # type: ignore[arg-type]

    assert [item["reason"] for item in published] == ["opaque_checkpoint_child_exit"]
    assert stop.waits == [1.0]
    assert published[0]["opaque_child_restarts"] == 1
