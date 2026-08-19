from __future__ import annotations

from collections import defaultdict
from pydantic import BaseModel, Field

from inefficiency_engine.models import Opportunity, OpportunityExecutability


class AllocationConstraintSet(BaseModel):
    total_capital_usd: float = Field(gt=0)
    max_venue_fraction: float = Field(default=0.50, gt=0, le=1)
    max_asset_fraction: float = Field(default=0.50, gt=0, le=1)
    max_allocations: int = Field(default=10, gt=0)


class PaperAllocation(BaseModel):
    opportunity_id: str
    strategy: str
    asset: str
    venues: list[str]
    notional_usd_per_leg: float
    capital_required_usd: float
    net_annualized_return: float
    estimated_capacity_notional_usd: float
    canonical_instrument_ids: list[str] = Field(default_factory=list)


class AllocationPlan(BaseModel):
    total_capital_usd: float
    allocated_capital_usd: float
    unused_cash_usd: float
    weighted_expected_net_annualized_return: float
    allocations: list[PaperAllocation] = Field(default_factory=list)
    skipped: list[dict[str, object]] = Field(default_factory=list)
    authorizes_execution: bool = False
    paper_only: bool = True


def allocate_qualified_opportunities(opportunities: list[Opportunity], executability: list[OpportunityExecutability],
                                     constraints: AllocationConstraintSet) -> AllocationPlan:
    opportunity_by_id = {item.id: item for item in opportunities}
    candidates = []
    for execution in executability:
        opportunity = opportunity_by_id.get(execution.opportunity_id)
        if opportunity is None:
            continue
        for tier in execution.tiers:
            if tier.executable and tier.passes_return_hurdle and tier.capital_required_usd > 0:
                candidates.append((tier.net_annualized_return, tier.capital_required_usd, tier, opportunity, execution))
    candidates.sort(key=lambda row: (row[0], -row[1]), reverse=True)
    allocations: list[PaperAllocation] = []
    skipped: list[dict[str, object]] = []
    venue_cap = constraints.total_capital_usd * constraints.max_venue_fraction
    asset_cap = constraints.total_capital_usd * constraints.max_asset_fraction
    venue_used: dict[str,float] = defaultdict(float)
    asset_used: dict[str,float] = defaultdict(float)
    used_instruments: set[str] = set()
    allocated = 0.0
    considered: set[str] = set()
    for _, _, tier, opportunity, execution in candidates:
        if len(allocations) >= constraints.max_allocations:
            break
        if opportunity.id in considered:
            continue
        evidence_ids = opportunity.evidence.get("canonical_instrument_ids", [])
        instrument_ids = [str(value) for value in evidence_ids] if isinstance(evidence_ids, list) else []
        if used_instruments.intersection(instrument_ids):
            skipped.append({"opportunity_id": opportunity.id, "reason": "shared instrument conflict"})
            considered.add(opportunity.id)
            continue
        capital = tier.capital_required_usd
        if allocated + capital > constraints.total_capital_usd + 1e-9:
            continue
        venues = sorted({leg.venue for leg in opportunity.legs})
        venue_share = capital / max(1, len(venues))
        if any(venue_used[venue] + venue_share > venue_cap + 1e-9 for venue in venues):
            skipped.append({"opportunity_id": opportunity.id, "reason": "venue concentration cap"})
            considered.add(opportunity.id)
            continue
        if asset_used[opportunity.asset] + capital > asset_cap + 1e-9:
            skipped.append({"opportunity_id": opportunity.id, "reason": "asset concentration cap"})
            considered.add(opportunity.id)
            continue
        if tier.notional_usd_per_leg > execution.estimated_capacity_notional_usd + 1e-9:
            skipped.append({"opportunity_id": opportunity.id, "reason": "capacity exceeded"})
            considered.add(opportunity.id)
            continue
        allocations.append(PaperAllocation(
            opportunity_id=opportunity.id, strategy=opportunity.strategy.value, asset=opportunity.asset, venues=venues,
            notional_usd_per_leg=tier.notional_usd_per_leg, capital_required_usd=capital,
            net_annualized_return=tier.net_annualized_return,
            estimated_capacity_notional_usd=execution.estimated_capacity_notional_usd,
            canonical_instrument_ids=instrument_ids,
        ))
        allocated += capital
        asset_used[opportunity.asset] += capital
        for venue in venues:
            venue_used[venue] += venue_share
        used_instruments.update(instrument_ids)
        considered.add(opportunity.id)
    weighted = sum(item.capital_required_usd * item.net_annualized_return for item in allocations) / constraints.total_capital_usd
    return AllocationPlan(total_capital_usd=constraints.total_capital_usd, allocated_capital_usd=allocated,
                          unused_cash_usd=max(0.0, constraints.total_capital_usd-allocated),
                          weighted_expected_net_annualized_return=weighted, allocations=allocations, skipped=skipped)
