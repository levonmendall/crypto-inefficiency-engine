from __future__ import annotations

import threading

from inefficiency_engine import render_combined_postbind_lane_repair as base
from inefficiency_engine.durable_lane_history_projection_supervisor import (
    run_durable_lane_history_projection_supervisor,
)
from inefficiency_engine.local_persistence_migration_supervisor import (
    migration_preflight,
    migration_status_payload,
    run_local_persistence_migration_supervisor,
)
from inefficiency_engine.startup_database_recovery import (
    install_startup_database_recovery,
)


MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS = 2.0
_TERMINAL_MIGRATION_STATES = {"failed", "interrupted", "verified"}


def _run_local_persistence_migration_with_deploy_handoff(
    stop_event: threading.Event,
) -> None:
    """Keep a fresh deploy eligible to take over an in-flight Stage 1 migration.

    Render can briefly overlap shutdown of the predecessor process with startup of the
    replacement. The migration supervisor correctly uses an exclusive durable-file lock
    to prevent concurrent importers, but a one-shot lock miss must not permanently strand
    a restart-safe migration. Retry only while preflight is still valid and durable state
    remains nonterminal. A real migration failure therefore still stops here and never
    receives a fresh source-disconnect retry budget merely because this guard exists.
    """

    while not stop_event.is_set():
        ready, _reason = migration_preflight()
        run_local_persistence_migration_supervisor(stop_event)
        if stop_event.is_set() or not ready:
            return

        status = migration_status_payload()
        state = str(status.get("state") or "")
        progress_state = str(status.get("progress_state") or "")
        supervisor_reason = str(status.get("supervisor_reason") or "")

        if state in _TERMINAL_MIGRATION_STATES:
            return
        if progress_state in {"failed", "verified"}:
            return
        if state == "blocked" and supervisor_reason != "another_importer_holds_lock":
            return

        # A ready, incomplete supervisor that returned without terminal truth is a
        # deploy-handoff/silent-startup condition, not a new migration attempt. Wait for
        # the predecessor lock to clear and re-enter the same restart-safe supervisor.
        if stop_event.wait(MIGRATION_DEPLOY_HANDOFF_RETRY_SECONDS):
            return


def main() -> int:
    """Run the canonical combined service plus independent durable background guards."""

    # Production schema bootstrap must remain serialized before permanent child startup,
    # but Render's attached PostgreSQL can be briefly unavailable in recovery mode during
    # a deploy. Patch only that bootstrap boundary with a bounded, recovery-specific retry.
    install_startup_database_recovery(base.base)

    stop_event = threading.Event()
    history_projection_guard = threading.Thread(
        target=run_durable_lane_history_projection_supervisor,
        args=(stop_event,),
        name="durable-lane-history-projection-supervisor",
        daemon=True,
    )
    migration_guard = threading.Thread(
        target=_run_local_persistence_migration_with_deploy_handoff,
        args=(stop_event,),
        name="local-persistence-migration-supervisor",
        daemon=True,
    )
    history_projection_guard.start()
    migration_guard.start()
    try:
        return base.main()
    finally:
        stop_event.set()
        history_projection_guard.join(timeout=10.0)
        migration_guard.join(timeout=10.0)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
