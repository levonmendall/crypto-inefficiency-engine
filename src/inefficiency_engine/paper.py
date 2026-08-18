from __future__ import annotations

from datetime import datetime, timezone
from pydantic import BaseModel, Field

from inefficiency_engine.models import Opportunity


class PaperFill(BaseModel):
    opportunity_id: str
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    notional_usd_per_leg: float
    status: str = "paper_open"


class PaperExecutor:
    """Deliberately incapable of sending a live order."""

    def __init__(self):
        self.fills: list[PaperFill] = []

    def execute(self, opportunity: Opportunity, notional_usd_per_leg: float = 1000.0) -> PaperFill:
        if not opportunity.paper_only:
            raise RuntimeError("live opportunities are forbidden in V1")
        if notional_usd_per_leg <= 0:
            raise ValueError("notional must be positive")
        fill = PaperFill(opportunity_id=opportunity.id, notional_usd_per_leg=notional_usd_per_leg)
        self.fills.append(fill)
        return fill
