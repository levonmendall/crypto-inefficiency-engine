from __future__ import annotations

import threading
from pathlib import Path

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair


def _copying_progress(*, error_type=None, error=None):
    return {
        "state": "running",
        "current_table": "market_quotes",
        "error_type": error_type,
        "error": error,
        "tables": {
            "market_quotes": {
                "verified": None,
                "migration_mode": "captured_monotonic_integer_high_water",
                "snapshot_phase": "copying_snapshot",
                "last_progress_at": "2026-08-29T23:30:00+00:00",
                "last_primary_key": [1748641],
                "snapshot_rows_copied": 1700000,
                "snapshot_high_water_primary_key": [2812933],
            }
        },
    }


def _opaque_failure_status():
    return {
        "state": "failed",
        "supervisor_reason": "migration_child_failed",
        "supervisor_started_at": "2026-08-29T22:06:31+00:00",
        "child_return_code": 1,
        "progress_state": "running",
    }


def test_opaque_child_exit_is_retryable_only_with_restart_safe_checkpoint():
    assert repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        _copying_progress(),
    )

    explicit_failure = _copying_progress(
        error_type="IntegrityError",
        error="destination equivalence failed",
    )
    assert not repair._restart_safe_opaque_child_exit(
        _opaque_failure_status(),
        explicit_failure,
    )


def test_stderr_tail_is_bounded_and_redacts_postgres_credentials(tmp_path: Path):
    stderr_path = tmp_path / "stderr.log"
    stderr_path.write_text(
        "prefix\npostgresql://user:secret@example.test/db\nterminal database failure\n"
    )

    tail = repair._read_stderr_tail(stderr_path)

    assert tail is not None
    assert "secret" not in tail
    assert "postgresql://***@example.test/db" in tail
    assert "terminal database failure" in tail


def test_wrapper_relaunches_after_opaque_checkpoint_exit(monkeypatch, tmp_path: Path):
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
    monkeypatch.setattr(repair, "OPAQUE_CHILD_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0))

    repair.run_local_persistence_migration_supervisor(threading.Event())

    assert len(run_calls) == 2
    assert published[0]["state"] == "retry_wait"
    assert published[0]["reason"] == "opaque_checkpoint_child_exit"
    assert published[0]["checkpoint_last_primary_key"] == [1748641]
    assert published[0]["error_type"] == "OpaqueMigrationChildExit"


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
    monkeypatch.setattr(repair, "MAX_OPAQUE_CHILD_RESTARTS", 1)
    monkeypatch.setattr(repair, "OPAQUE_CHILD_RETRY_DELAYS_SECONDS", (0.0,))

    repair.run_local_persistence_migration_supervisor(threading.Event())

    assert len(run_calls) == 2
    assert published[-1]["state"] == "failed"
    assert published[-1]["reason"] == "opaque_checkpoint_child_retry_exhausted"
    assert published[-1]["opaque_child_restarts"] == 1
