from __future__ import annotations

from inefficiency_engine.bounded_heartbeat_runtime import (
    install_bounded_evidence_heartbeat_read,
)


# Install before importing the composed API so every health/readiness layer uses the
# bounded append-only heartbeat tail. This changes only diagnostic read cost; it does
# not change worker state, evidence freshness, qualification, or execution authority.
install_bounded_evidence_heartbeat_read()

from inefficiency_engine.read_api_card_history_deploy import app  # noqa: E402,F401
