from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine.config import Settings
from inefficiency_engine.models import Opportunity


class RiskGate:
    def __init__(self, settings: Settings):
        self.settings = settings

    def filter(self, opportunities: list[Opportunity], now: datetime | None = None) -> list[Opportunity]:
        now = now or datetime.now(timezone.utc)
        accepted: list[Opportunity] = []
        for opportunity in opportunities:
            if not opportunity.paper_only:
                continue
            if opportunity.expires_at < now:
                continue
            if opportunity.net_annualized_return < self.settings.min_net_annualized_return:
                continue
            if opportunity.net_edge_bps_per_hour <= 0:
                continue
            if len(opportunity.legs) != 2:
                continue
            accepted.append(opportunity)
        return sorted(accepted, key=lambda x: x.net_annualized_return, reverse=True)
