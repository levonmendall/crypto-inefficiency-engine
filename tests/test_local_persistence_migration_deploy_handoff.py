from __future__ import annotations

import json
import threading

import inefficiency_engine.render_combined_postbind_history_projection as runtime


def test_ready_incomplete_supervisor_retries_after_lock_handoff(monkeypatch) -> None:
    stop_event = threading.Event()
    runs: list[int] = []

    monkeypatch.setattr(runtime, "MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(runtime, "migration_preflight", lambda: (True, "ready"))

    def fake_run(event: threading.Event) -> None:
        runs.append(len(runs) + 1)
        if len(runs) == 2:
            event.set()

    monkeypatch.setattr(runtime, "run_local_persistence_migration_supervisor", fake_run)
    monkeypatch.setattr(
        runtime,
        "migration_status_payload",
        lambda: {
            "state": "blocked",
            "progress_state": "running",
            "supervisor_reason": "another_importer_holds_lock",
        },
    )

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert runs == [1, 2]


def test_stale_predecessor_running_status_does_not_strand_handoff(monkeypatch) -> None:
    stop_event = threading.Event()
    runs: list[int] = []

    monkeypatch.setattr(runtime, "MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(runtime, "migration_preflight", lambda: (True, "ready"))

    def fake_run(event: threading.Event) -> None:
        runs.append(len(runs) + 1)
        if len(runs) == 2:
            event.set()

    monkeypatch.setattr(runtime, "run_local_persistence_migration_supervisor", fake_run)
    monkeypatch.setattr(
        runtime,
        "migration_status_payload",
        lambda: {
            "state": "running",
            "progress_state": "running",
            "supervisor_reason": None,
        },
    )

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert runs == [1, 2]


def test_terminal_migration_failure_does_not_receive_new_retry_budget(monkeypatch) -> None:
    stop_event = threading.Event()
    runs: list[int] = []

    monkeypatch.setattr(runtime, "MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(runtime, "migration_preflight", lambda: (True, "ready"))
    monkeypatch.setattr(
        runtime,
        "run_local_persistence_migration_supervisor",
        lambda _event: runs.append(1),
    )
    monkeypatch.setattr(
        runtime,
        "migration_status_payload",
        lambda: {
            "state": "failed",
            "progress_state": "failed",
            "supervisor_reason": "migration_child_failed",
        },
    )

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert runs == [1]


def test_preflight_block_is_published_once_and_not_retried(monkeypatch) -> None:
    stop_event = threading.Event()
    runs: list[int] = []

    monkeypatch.setattr(
        runtime,
        "migration_preflight",
        lambda: (False, "local_history_authority_already_enabled"),
    )
    monkeypatch.setattr(
        runtime,
        "run_local_persistence_migration_supervisor",
        lambda _event: runs.append(1),
    )
    monkeypatch.setattr(
        runtime,
        "migration_status_payload",
        lambda: (_ for _ in ()).throw(AssertionError("status should not be reread")),
    )

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert runs == [1]


def test_fresh_guard_start_is_published_before_supervisor_entry(monkeypatch) -> None:
    stop_event = threading.Event()
    events: list[tuple[str, str]] = []

    monkeypatch.setattr(runtime, "migration_preflight", lambda: (True, "ready"))
    monkeypatch.setattr(
        runtime,
        "_safe_publish_migration_guard_status",
        lambda **payload: events.append(("status", str(payload["state"]))),
    )

    def fake_run(event: threading.Event) -> None:
        events.append(("supervisor", "entered"))
        event.set()

    monkeypatch.setattr(runtime, "run_local_persistence_migration_supervisor", fake_run)

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert events[:3] == [
        ("status", "started"),
        ("status", "running"),
        ("supervisor", "entered"),
    ]


def test_guard_exception_is_durable_and_retried_while_migration_is_nonterminal(
    monkeypatch,
) -> None:
    stop_event = threading.Event()
    runs: list[int] = []
    published: list[dict[str, object]] = []

    monkeypatch.setattr(runtime, "MIGRATION_GUARD_EXCEPTION_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(runtime, "migration_preflight", lambda: (True, "ready"))
    monkeypatch.setattr(
        runtime,
        "_safe_publish_migration_guard_status",
        lambda **payload: published.append(dict(payload)),
    )
    monkeypatch.setattr(
        runtime,
        "migration_status_payload",
        lambda: {
            "state": "running",
            "progress_state": "running",
            "postgresql_authoritative": True,
        },
    )

    def fake_run(event: threading.Event) -> None:
        runs.append(len(runs) + 1)
        if len(runs) == 1:
            raise RuntimeError("injected migration guard startup failure")
        event.set()

    monkeypatch.setattr(runtime, "run_local_persistence_migration_supervisor", fake_run)

    runtime._run_local_persistence_migration_with_deploy_handoff(stop_event)

    assert runs == [1, 2]
    errors = [item for item in published if item.get("state") == "error_retry_wait"]
    assert len(errors) == 1
    assert errors[0]["reason"] == "migration_guard_exception"
    assert errors[0]["error_type"] == "RuntimeError"
    assert errors[0]["attempt"] == 1


def test_guard_status_persists_release_identity_and_redacts_database_credentials(
    monkeypatch,
    tmp_path,
) -> None:
    path = tmp_path / runtime.MIGRATION_GUARD_STATUS_FILENAME
    monkeypatch.setattr(runtime, "_guard_status_path", lambda: path)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "deadbeef")

    runtime._publish_migration_guard_status(
        state="error_retry_wait",
        reason="migration_guard_exception",
        started_at="2026-08-29T02:10:00+00:00",
        attempt=2,
        error_type="RuntimeError",
        error="postgresql://user:secret@host/database connection failed",
    )

    payload = json.loads(path.read_text())
    assert payload["state"] == "error_retry_wait"
    assert payload["attempt"] == 2
    assert payload["release_commit"] == "deadbeef"
    assert "secret" not in payload["error"]
    assert "postgresql://***@host/database" in payload["error"]
    assert payload["postgresql_authoritative"] is True
    assert payload["cutover_ready"] is False
    assert payload["allocation_authority"] is False
    assert payload["live_execution_authority"] is False


def test_main_publishes_guard_startup_synchronously_before_threads_start(
    monkeypatch,
) -> None:
    events: list[str] = []

    class FakeThread:
        def __init__(self, *, target, args, name, daemon):
            self.name = name

        def start(self) -> None:
            events.append(f"start:{self.name}")

        def join(self, timeout=None) -> None:
            events.append(f"join:{self.name}")

    monkeypatch.setattr(runtime, "install_startup_database_recovery", lambda _base: None)
    monkeypatch.setattr(runtime.threading, "Thread", FakeThread)
    monkeypatch.setattr(
        runtime,
        "_publish_synchronous_migration_guard_startup",
        lambda: events.append("sync_guard_startup") or "2026-08-29T03:00:00+00:00",
    )
    monkeypatch.setattr(runtime.base, "main", lambda: 0)

    assert runtime.main() == 0
    assert events[0] == "sync_guard_startup"
    assert events[1:3] == [
        "start:durable-lane-history-projection-supervisor",
        "start:local-persistence-migration-supervisor",
    ]


def test_synchronous_guard_startup_records_durable_write_failure(
    monkeypatch,
) -> None:
    fallback: list[dict[str, object]] = []

    monkeypatch.setattr(
        runtime,
        "_publish_migration_guard_status",
        lambda **_payload: (_ for _ in ()).throw(OSError("No space left on device")),
    )
    monkeypatch.setattr(
        runtime,
        "_publish_migration_guard_fallback_status",
        lambda **payload: fallback.append(dict(payload)),
    )

    runtime._publish_synchronous_migration_guard_startup()

    assert len(fallback) == 1
    assert fallback[0]["state"] == "durable_status_write_failed"
    assert fallback[0]["reason"] == "main_startup_durable_status_write_failed"
    assert fallback[0]["attempt"] == 0
    assert fallback[0]["error_type"] == "OSError"
    assert "No space left on device" in str(fallback[0]["error"])


def test_background_guard_status_write_failure_is_not_silent(monkeypatch) -> None:
    fallback: list[dict[str, object]] = []

    monkeypatch.setattr(
        runtime,
        "_publish_migration_guard_status",
        lambda **_payload: (_ for _ in ()).throw(PermissionError("read-only disk")),
    )
    monkeypatch.setattr(
        runtime,
        "_publish_migration_guard_fallback_status",
        lambda **payload: fallback.append(dict(payload)),
    )

    runtime._safe_publish_migration_guard_status(
        state="running",
        reason="supervisor_entry",
        started_at="2026-08-29T03:00:00+00:00",
        attempt=1,
    )

    assert len(fallback) == 1
    assert fallback[0]["state"] == "durable_status_write_failed"
    assert fallback[0]["reason"] == "background_durable_status_write_failed"
    assert fallback[0]["error_type"] == "PermissionError"
