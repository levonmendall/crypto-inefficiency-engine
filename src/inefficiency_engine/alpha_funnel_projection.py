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
    """Merge latest alpha funnels without refreshing structural closure freshness.

    The structural closure cycle runs earlier than alpha on the disposable cadence.
    Rather than reorder heavy research work, append a compact projection after alpha
    completes. Existing structural/capital-location/maker diagnostics and the parent
    closure timestamp are preserved exactly. Each refreshed alpha funnel carries its
    own observation timestamp so the dashboard can distinguish their freshness.

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
        projected_row = dict(row)
        projected_row["alpha_funnel_observed_at"] = observed_at.isoformat()
        merged[lane] = projected_row
        changed = True
    if not changed:
        return False

    projected = current.model_copy(
        deep=True,
        update={
            "summary_id": uuid.uuid4().hex,
            "rejection_funnels": merged,
        },
    )
    ledger.record(projected)
    return True
