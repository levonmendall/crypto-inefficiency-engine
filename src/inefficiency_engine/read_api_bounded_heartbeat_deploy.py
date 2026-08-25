from __future__ import annotations

from inefficiency_engine.bounded_heartbeat_runtime import (
    install_bounded_evidence_heartbeat_read,
)
from inefficiency_engine.cycle_history_health_observability import (
    install_cycle_history_health_observability,
)


# Install before importing the composed API so every health/readiness layer uses the
# bounded append-only heartbeat tail. This changes only diagnostic read cost; it does
# not change worker state, evidence freshness, qualification, or execution authority.
install_bounded_evidence_heartbeat_read()

from inefficiency_engine import read_api_active_volume_deploy as active  # noqa: E402


# Expose presentation publishers and the independent cycle-history maintenance worker.
# A current compute heartbeat must not hide a failed compact projection/backfill task.
active._RUNTIME_HEARTBEATS.update(
    {
        "dashboard_projection": "dashboard-projection-publisher",
        "research_projection": "dashboard-research-projection-publisher",
        "cycle_history_backfill": "cycle-history-background-backfill",
    }
)
active._RUNTIME_STALE_AFTER_SECONDS.update(
    {
        "dashboard_projection": 600.0,
        "research_projection": 600.0,
        "cycle_history_backfill": 180.0,
    }
)
install_cycle_history_health_observability(active)

from inefficiency_engine.read_api_card_history_deploy import app  # noqa: E402,F401
