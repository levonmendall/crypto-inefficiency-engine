from __future__ import annotations

import threading
from pathlib import Path

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair


def _copying_progress(*, error_type=None, error=None, checkpoint=True, high_water=True):
    market = {
        "verified": None,
        "migration_mode": "captured_primary_key_high_water",
        "source_rows": 2_794_738,
        "source_lineage_count": 2_787_792,
    }
    if checkpoint:
        market["last_primary_key"] = [1_748_641]
    if high_water:
        market["high_water_primary_key"] = [2_812_933]
    return {
        "state": "running",
        "current_table": "market_quotes",
        "error_type": error_type,
        "error": error,
        "tables": {"market_quotes": market},
    }


def _opaque_failure_status():
    return {
        "state": "failed",
        "supervisor_reason": "migration_child_failed",
        "supervisor_started_at": "2026-08-29T22:06:31+00:00",
        "child_return_code": 1,
        "progress_state": "running",
    }


def test_market_quotes_marker_matches_real_stage_one_checkpoint_shape():
    marker = repair._market_quotes_checkpoint_marker(_copying_progress())

    assert marker is not None
    assert marker[0] == "market_quotes"
    assert "2812933" in marker[1]
    assert "1748641" in marker[2]


def test_opaque_child_exit_is_retryable_only_with_restart_safe_market_checkpoint():
    assert repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        _copying_progress(),
    )

    assert not repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        _copying_progress(checkpoint=False),
    )
    assert not repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        _copying_progress(high_water=False),
    )

    explicit_failure = _copying_progress(
        error_type="IntegrityError",
        error="destination equivalence failed",
    )
    assert not repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        explicit_failure,
    )


def test_funding_snapshot_mode_is_not_mistaken_for_market_quotes_resume():
    progress = _copying_progress()
    progress["tables"]["market_quotes"]["migration_mode"] = (
        "captured_monotonic_integer_high_water"
    )
    progress["tables"]["market_quotes"]["snapshot_phase"] = "copying_snapshot"

    assert repair._market_quotes_checkpoint_marker(progress) is None
    assert not repair._restart_safe_opaque_child_exit(_opaque_failure_status(), progress)


def test_inode_recovery_compacts_only_checkpointed_market_history(monkeypatch):
    observed = []

    class FakeHistory:
        def compact_redundant_partitions(self, *, target_free_inodes):
            observed.append(target_free_inodes)
            return {
                "compacted_groups": 12,
                "files_collapsed": 200_000,
                "rows_rewritten": 300_000,
                "garbage_reaped": 0,
                "inode_total_before": 1_638_400,
                "inode_free_before": 4,
                "inode_total_after": 1_638_400,
                "inode_free_after": target_free_inodes + 1,
                "target_free_inodes": target_free_inodes,
                "target_reached": True,
            }

    monkeypatch.setattr(repair, "PartitionedMarketHistory", FakeHistory)
    monkeypatch.setattr(repair, "_inode_capacity", lambda: (1_638_400, 4))
    repair._LAST_INODE_RECOVERY = {}

    result = repair._recover_market_history_inode_pressure(_copying_progress())

    assert observed == [163_840]
    assert result is not None
    assert result["state"] == "complete"
    assert result["files_collapsed"] == 200_000
    assert result["checkpoint_last_primary_key"] == [1_748_641]
    assert repair._recover_market_history_inode_pressure(
        _copying_progress(checkpoint=False)
    ) is None


def test_stderr_tail_is_bounded_redacted_and_keeps_terminal_end(tmp_path: Path):
    stderr_path = tmp_path / "stderr.log"
    stderr_path.write_text(
        "prefix\n"
        + ("x" * 900)
        + "\npostgresql://user:secret@example.test/db\n"
        + "terminal database failure at the very end\n"
    )

    tail = repair._read_stderr_tail(stderr_path)

    assert tail is not None
    assert len(tail) <= 600
    assert "secret" not in tail
    assert "postgresql://***@example.test/db" in tail
    assert tail.endswith("terminal database failure at the very end")


def test_storage_exhaustion_requires_proven_errno_or_quota_message():
    assert repair._is_storage_exhaustion(
        "OSError: [Errno 28] No space left on device: /var/data/cie/migration/progress.tmp"
    )
    assert repair._is_storage_exhaustion("write failed: Disk quota exceeded")
    assert not repair._is_storage_exhaustion("opaque child failure")
    assert not repair._is_storage_exhaustion(None)


def test_wrapper_relaunches_after_opaque_market_checkpoint_exit(monkeypatch, tmp_path: Path):
    progress_path = tmp_path / "progress.json"
    stderr_path = tmp_path / "stderr.log"
    stderr_path.write_text("opaque child failure")
    statuses = iter(
        [
            _opaque_failure_status(),
            {
                "state": "verified",
                "supervisor_reason": "snapshot_verification_complete",
                "child_return_code": 0,
                "progress_state": "verified",
            },
        ]
    )
    run_calls = []
    published = []

    monkeypatch.setattr(
        repair.base,
        "run_local_persistence_migration_supervisor",
        lambda stop_event: run_calls.append(True),
    )
    monkeypatch.setattr(repair.base, "migration_status_payload", lambda: next(statuses))
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
    monkeypatch.setattr(repair.base, "_read_json", lambda path: _copying_progress())
    monkeypatch.setattr(repair.base, "_publish_status", lambda payload: published.append(payload))
    monkeypatch.setattr(repair, "_inode_capacity", lambda: (1_638_400, 1_000_000))
    monkeypatch.setattr(repair, "OPAQUE_CHILD_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))

    repair.run_local_persistence_migration_supervisor(threading.Event())

    assert len(run_calls) == 2
    assert published[0]["state"] == "retry_wait"
    assert published[0]["reason"] == "opaque_checkpoint_child_exit"
    assert published[0]["checkpoint_migration_mode"] == "captured_primary_key_high_water"
    assert published[0]["checkpoint_last_primary_key"] == [1_748_641]
    assert published[0]["checkpoint_high_water_primary_key"] == [2_812_933]
    assert published[0]["error_type"] == "OpaqueMigrationChildExit"


def test_wrapper_fails_immediately_when_storage_is_exhausted(monkeypatch, tmp_path: Path):
    progress_path = tmp_path / "progress.json"
    stderr_path = tmp_path / "stderr.log"
    stderr_path.write_text(
        "OSError: [Errno 28] No space left on device: "
        "'/var/data/cie/migration/postgres-import-progress.tmp'"
    )
    run_calls = []
    published = []

    monkeypatch.setattr(
        repair.base,
        "run_local_persistence_migration_supervisor",
        lambda stop_event: run_calls.append(True),
    )
    monkeypatch.setattr(repair.base, "migration_status_payload", _opaque_failure_status)
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
    monkeypatch.setattr(repair.base, "_read_json", lambda path: _copying_progress())
    monkeypatch.setattr(repair.base, "_publish_status", lambda payload: published.append(payload))
    monkeypatch.setattr(repair, "_inode_capacity", lambda: (1_638_400, 1_000_000))

    repair.run_local_persistence_migration_supervisor(threading.Event())

    assert len(run_calls) == 1
    assert published[-1]["state"] == "failed"
    assert published[-1]["reason"] == "migration_storage_exhausted"
    assert published[-1]["error_type"] == "NoSpaceLeftOnDevice"
    assert published[-1]["opaque_child_restarts"] == 0
    assert published[-1]["checkpoint_last_primary_key"] == [1_748_641]
    assert published[-1]["checkpoint_high_water_primary_key"] == [2_812_933]


def test_wrapper_fails_closed_when_opaque_retry_budget_is_exhausted(monkeypatch, tmp_path: Path):
    progress_path = tmp_path / "progress.json"
    stderr_path = tmp_path / "stderr.log"
    stderr_path.write_text("still opaque")
    run_calls = []
    published = []

    monkeypatch.setattr(
        repair.base,
        "run_local_persistence_migration_supervisor",
        lambda stop_event: run_calls.append(True),
    )
    monkeypatch.setattr(repair.base, "migration_status_payload", _opaque_failure_status)
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
    monkeypatch.setattr(repair.base, "_read_json", lambda path: _copying_progress())
    monkeypatch.setattr(repair.base, "_publish_status", lambda payload: published.append(payload))
    monkeypatch.setattr(repair, "_inode_capacity", lambda: (1_638_400, 1_000_000))
    monkeypatch.setattr(repair, "MAX_OPAQUE_CHILD_RESTARTS", 1)
    monkeypatch.setattr(repair, "OPAQUE_CHILD_RETRY_DELAYS_SECONDS", (0.0,))

    repair.run_local_persistence_migration_supervisor(threading.Event())

    assert len(run_calls) == 2
    assert published[-1]["state"] == "failed"
    assert published[-1]["reason"] == "opaque_checkpoint_child_retry_exhausted"
    assert published[-1]["opaque_child_restarts"] == 1
