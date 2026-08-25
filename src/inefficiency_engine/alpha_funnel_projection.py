from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select

from inefficiency_engine.research_closure_worker import (
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)


# Every alpha lane, including microstructure, is projected from one discovery cycle.
# Structural closure still exposes source/usable order-book counts separately, but it
# must not combine those counts with an emitted-candidate count borrowed from a later
# operating snapshot. The alpha diagnostic carries both raw and emitted counts from
# the same cycle and is therefore the canonical dashboard funnel for microstructure.
DASHBOARD_ALPHA_FUNNEL_LANES = frozenset(
    {
        "trend_momentum",
        "mean_reversion",
        "fundamental_onchain",
        "cross_sectional_relative_value",
        "event_driven",
        "microstructure",
    }
)


def publish_alpha_funnel_projection(
    store,
    diagnostics: dict[str, dict[str, object]],
    *,
    observed_at: datetime,
) -> bool:
    """Merge latest same-cycle alpha funnels without refreshing structural closure freshness.

    Structural closure owns price/carry reconstruction, source order-book counts,
    capital-location diagnostics, and maker-shadow diagnostics. Alpha discovery owns
    the six alpha-lane candidate funnels. Existing structural fields and the parent
    closure timestamp are preserved exactly; each refreshed alpha funnel carries its
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
        projected_row["same_cycle_candidate_funnel"] = True
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
