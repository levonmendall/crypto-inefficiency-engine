from __future__ import annotations

from pydantic import BaseModel, Field

from inefficiency_engine.models import Opportunity, OpportunityExecutability, Strategy


class RankedOpportunity(BaseModel):
    rank: int
    opportunity_id: str
    strategy: Strategy
    asset: str
    rank_basis: str = "capital_adjusted_net_annualized_return"
    rank_score: float
    selected_notional_usd_per_leg: float
    selected_capital_required_usd: float
    selected_net_annualized_return: float
    selected_total_modeled_cost_bps: float
    estimated_capacity_notional_usd: float
    max_qualified_notional_usd: float
    detector_module: str | None = None
    canonical_asset_id: str | None = None
    canonical_instrument_ids: list[str] = Field(default_factory=list)
    paper_only: bool = True


def rank_qualified_opportunities(
    opportunities: list[Opportunity],
    executability: list[OpportunityExecutability],
) -> list[RankedOpportunity]:
    """Rank qualified opportunities without allocating or authorizing capital.

    The comparator is the existing capital-adjusted net annualized return. This
    intentionally avoids inventing a portfolio allocator before independent
    strategy families exist. Capacity remains explicit so the future allocator
    can distinguish high-return/low-capacity and lower-return/high-capacity edge.
    """
    opportunity_by_id = {item.id: item for item in opportunities}
    rows: list[tuple[float, float, RankedOpportunity]] = []
    for execution in executability:
        opportunity = opportunity_by_id.get(execution.opportunity_id)
        if opportunity is None:
            continue
        qualified = [
            tier
            for tier in execution.tiers
            if tier.executable and tier.passes_return_hurdle
        ]
        if not qualified:
            continue
        selected = max(
            qualified,
            key=lambda tier: (tier.net_annualized_return, tier.notional_usd_per_leg),
        )
        evidence = opportunity.evidence
        instrument_ids = evidence.get("canonical_instrument_ids", [])
        if not isinstance(instrument_ids, list):
            instrument_ids = []
        row = RankedOpportunity(
            rank=0,
            opportunity_id=opportunity.id,
            strategy=opportunity.strategy,
            asset=opportunity.asset,
            rank_score=selected.net_annualized_return,
            selected_notional_usd_per_leg=selected.notional_usd_per_leg,
            selected_capital_required_usd=selected.capital_required_usd,
            selected_net_annualized_return=selected.net_annualized_return,
            selected_total_modeled_cost_bps=selected.total_modeled_cost_bps,
            estimated_capacity_notional_usd=execution.estimated_capacity_notional_usd,
            max_qualified_notional_usd=execution.max_qualified_notional_usd,
            detector_module=(str(evidence["detector_module"]) if evidence.get("detector_module") is not None else None),
            canonical_asset_id=(str(evidence["canonical_asset_id"]) if evidence.get("canonical_asset_id") is not None else None),
            canonical_instrument_ids=[str(value) for value in instrument_ids],
        )
        rows.append((row.rank_score, row.estimated_capacity_notional_usd, row))

    rows.sort(key=lambda item: (item[0], item[1]), reverse=True)
    ranked: list[RankedOpportunity] = []
    for index, (_, _, row) in enumerate(rows, start=1):
        ranked.append(row.model_copy(update={"rank": index}))
    return ranked
