from __future__ import annotations

from sqlalchemy import select

from inefficiency_engine.models import ShadowCycle
from inefficiency_engine.shadow import summarize_shadow_cycles


def summarize_evidence_store(store) -> dict[str, object]:
    """Build v0.7 empirical attribution metrics from append-only shadow evidence."""
    with store.engine.connect() as db:
        payloads = list(
            db.execute(
                select(store.shadow_cycles.c.payload_json).order_by(store.shadow_cycles.c.completed_at)
            ).scalars()
        )
    cycles = [ShadowCycle.model_validate_json(payload) for payload in payloads]
    return summarize_shadow_cycles(cycles)
