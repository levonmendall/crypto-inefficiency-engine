from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select

from inefficiency_engine.research_closure_worker import (
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)


# Microstructure already owns a dedicated raw order-book rejection funnel in the
# research-closure service. Do not overwrite that richer venue-specific diagnostic.
DASHBOARD_ALPHA_FUNNEL_LANES = frozenset(
    {
        "trend_momentum",
        "mean_reversion",
        "fundamental_onchain",
        "cross_sectional_relative_value",
        "event_driven",
    }
)


def publish_alpha_funnel_projection(
    store,
    diagnostics: dict[str, dict[str, object]],
    *,
    observed_at: datetime,
) -> bool:
    """Merge the latest alpha discovery funnel into dashboard-facing closure truth.

    The structural closure cycle runs earlier than alpha on the disposable cadence.
    Rather than reorder heavy research work, append a new compact closure projection
    after alpha completes. Existing structural/capital-location/maker diagnostics are
    preserved exactly; only the five alpha-only lane funnels are refreshed.

    This function has no allocation authority and never changes qualification state.
    """

    ledger = ResearchClosureSummaryLedger(store)
    with store.engine.connect() as db:
        raw = db.execute(
            select(ledger.table.c.payload_json)
            .order_by(ledger.table.c.id.desc())
            .limit(1)
        ).scalar_one_or_none()
    if raw is None:
        return False

    current = ResearchClosureCycleSummary.model_validate_json(raw)
    merged = dict(current.rejection_funnels)
    changed = False
    for lane in DASHBOARD_ALPHA_FUNNEL_LANES:
        row = diagnostics.get(lane)
        if not isinstance(row, dict):
            continue
        merged[lane] = dict(row)
        changed = True
    if not changed:
        return False

    projected = current.model_copy(
        deep=True,
        update={
            "summary_id": uuid.uuid4().hex,
            "observed_at": observed_at,
            "rejection_funnels": merged,
        },
    )
    ledger.record(projected)
    return True
