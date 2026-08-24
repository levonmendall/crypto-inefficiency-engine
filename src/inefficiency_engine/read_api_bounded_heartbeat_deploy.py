from __future__ import annotations

from inefficiency_engine.bounded_heartbeat_runtime import (
    install_bounded_evidence_heartbeat_read,
)


# Install before importing the composed API so every health/readiness layer uses the
# bounded append-only heartbeat tail. This changes only diagnostic read cost; it does
# not change worker state, evidence freshness, qualification, or execution authority.
install_bounded_evidence_heartbeat_read()

from inefficiency_engine import read_api_active_volume_deploy as active  # noqa: E402


# Expose the two presentation publishers independently from the portfolio/research
# workers. A current compute heartbeat must not hide a failed compact projection task.
active._RUNTIME_HEARTBEATS.update(
    {
        "dashboard_projection": "dashboard-projection-publisher",
        "research_projection": "dashboard-research-projection-publisher",
    }
)
active._RUNTIME_STALE_AFTER_SECONDS.update(
    {
        "dashboard_projection": 600.0,
        "research_projection": 600.0,
    }
)

from inefficiency_engine.read_api_card_history_deploy import app  # noqa: E402,F401
