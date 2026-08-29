from __future__ import annotations

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
    # A predecessor can overwrite the brief lock-blocked row while shutting down. The
    # fresh process must still retry when its supervisor returned without terminal truth.
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
