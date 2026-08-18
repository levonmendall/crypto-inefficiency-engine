from __future__ import annotations

from inefficiency_engine.models import Opportunity


def opportunity_signature(opportunity: Opportunity) -> str:
    """Stable economic signature that ignores observation timestamps/IDs."""
    legs = "|".join(
        f"{leg.venue}:{leg.asset}:{leg.market_kind.value}:{leg.side.value}"
        for leg in opportunity.legs
    )
    return f"{opportunity.strategy.value}:{opportunity.asset}:{legs}"
