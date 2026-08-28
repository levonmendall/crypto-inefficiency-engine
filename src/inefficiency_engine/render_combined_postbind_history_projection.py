from __future__ import annotations

import threading

from inefficiency_engine import render_combined_postbind_lane_repair as base
from inefficiency_engine.durable_lane_history_projection_supervisor import (
    run_durable_lane_history_projection_supervisor,
)
from inefficiency_engine.local_persistence_migration_supervisor import (
    run_local_persistence_migration_supervisor,
)
from inefficiency_engine.startup_database_recovery import (
    install_startup_database_recovery,
)


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
        target=run_local_persistence_migration_supervisor,
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
