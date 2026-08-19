from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.models import Opportunity, OpportunityExecutability
from inefficiency_engine.portfolio_risk import ExposureKind, PortfolioRiskBudget, PortfolioRiskOverlay
from inefficiency_engine.service import OpportunityService

if TYPE_CHECKING:
    from inefficiency_engine.alpha_factory import AlphaFactoryService


class UnifiedPaperCandidate(BaseModel):
    candidate_id: str
    family: Literal["core_cex", "cex_dex", "alpha"]
    strategy: str
    asset: str
    venues: list[str]
    capital_required_usd: float = Field(gt=0)
    notional_usd_per_leg: float = Field(gt=0)
    expected_profit_usd_per_deployment: float = Field(ge=0)
    expected_return_on_reserved_capital: float = Field(ge=0)
    modeled_holding_hours: float | None = Field(default=None, gt=0)
    source_return_metric: str
    source_return_value: float
    exposure_kind: ExposureKind = "market_neutral"
    conflict_keys: list[str] = Field(default_factory=list)
    evidence_id: str | None = None
    opportunity_id: str | None = None
    capacity_reference_usd: float | None = Field(default=None, ge=0)
    capacity_claimed: bool = False
    allocation_eligible: bool = True
    executable_eligible: bool = False
    paper_only: bool = True


class UnifiedPaperAllocation(BaseModel):
    candidate_id: str
    family: str
    strategy: str
    asset: str
    venues: list[str]
    capital_required_usd: float
    notional_usd_per_leg: float
    expected_profit_usd_per_deployment: float
    expected_return_on_reserved_capital: float
    modeled_holding_hours: float | None = None
    source_return_metric: str
    source_return_value: float
    exposure_kind: ExposureKind = "market_neutral"
    evidence_id: str | None = None
    opportunity_id: str | None = None
    capacity_claimed: bool = False
    authorizes_execution: bool = False
    paper_only: bool = True


class UnifiedPaperAllocationPlan(BaseModel):
    observed_at: datetime
    rank_basis: str = "conservative_expected_return_on_reserved_capital_per_current_deployment"
    total_capital_usd: float = Field(gt=0)
    allocated_capital_usd: float = Field(ge=0)
    unused_cash_usd: float = Field(ge=0)
    expected_profit_usd_current_deployments: float = Field(ge=0)
    weighted_expected_return_on_reserved_capital: float = Field(ge=0)
    candidate_count: int = Field(ge=0)
    allocations: list[UnifiedPaperAllocation] = Field(default_factory=list)
    skipped: list[dict[str, object]] = Field(default_factory=list)
    portfolio_risk_budget: PortfolioRiskBudget | None = None
    authorizes_execution: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


def _core_candidates(
    opportunities: list[Opportunity],
    executability: list[OpportunityExecutability],
) -> list[UnifiedPaperCandidate]:
    by_id = {item.id: item for item in opportunities}
    rows: list[UnifiedPaperCandidate] = []
    for execution in executability:
        opportunity = by_id.get(execution.opportunity_id)
        if opportunity is None or opportunity.holding_hours <= 0:
            continue
        qualified = [
            tier for tier in execution.tiers
            if tier.executable and tier.passes_return_hurdle and tier.capital_required_usd > 0
        ]
        if not qualified:
            continue
        tier = max(
            qualified,
            key=lambda item: (item.net_annualized_return, item.notional_usd_per_leg),
        )
        deployment_return = max(
            0.0,
            tier.net_annualized_return * opportunity.holding_hours / (24.0 * 365.0),
        )
        profit = tier.capital_required_usd * deployment_return
        evidence_ids = opportunity.evidence.get("canonical_instrument_ids", [])
        instrument_ids = [str(value) for value in evidence_ids] if isinstance(evidence_ids, list) else []
        conflict_keys = [f"instrument:{value}" for value in instrument_ids]
        conflict_keys.extend(
            f"venue-symbol:{leg.venue}:{leg.symbol or leg.asset}" for leg in opportunity.legs
        )
        if not conflict_keys:
            conflict_keys = [
                f"leg:{leg.venue}:{leg.symbol or leg.asset}:{leg.market_kind.value}:{leg.side.value}"
                for leg in opportunity.legs
            ]
        rows.append(UnifiedPaperCandidate(
            candidate_id=f"core:{opportunity.id}",
            family="core_cex",
            strategy=opportunity.strategy.value,
            asset=opportunity.asset,
            venues=sorted({leg.venue for leg in opportunity.legs}),
            capital_required_usd=tier.capital_required_usd,
            notional_usd_per_leg=tier.notional_usd_per_leg,
            expected_profit_usd_per_deployment=profit,
            expected_return_on_reserved_capital=deployment_return,
            modeled_holding_hours=opportunity.holding_hours,
            source_return_metric="net_annualized_return",
            source_return_value=tier.net_annualized_return,
            exposure_kind="market_neutral",
            conflict_keys=sorted(set(conflict_keys)),
            opportunity_id=opportunity.id,
            capacity_reference_usd=execution.estimated_capacity_notional_usd,
            capacity_claimed=False,
            allocation_eligible=True,
            executable_eligible=False,
            paper_only=True,
        ))
    return rows


class UnifiedPaperAllocatorService:
    def __init__(
        self,
        core: OpportunityService,
        cex_dex: CexDexPaperPromotionService,
        alpha_factory: "AlphaFactoryService | None" = None,
    ):
        self.core = core
        self.cex_dex = cex_dex
        self.alpha_factory = alpha_factory
        self.settings = core.settings

    async def candidates(self, *, total_capital_usd: float) -> list[UnifiedPaperCandidate]:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = await self.core.collect_live_executability()
        rows = _core_candidates(snapshot.opportunities, snapshot.executability)

        cex_dex_probe = await self.cex_dex.live_qualification(
            paper_inventory_usd_per_side=total_capital_usd / 2.0
        )
        for item in cex_dex_probe.qualifications:
            if not item.paper_allocation_eligible:
                continue
            capital = item.paper_capital_required_usd
            profit = item.target_notional_usd * item.conservative_capture_edge_bps / 10_000.0
            deployment_return = profit / capital
            rows.append(UnifiedPaperCandidate(
                candidate_id=f"cex-dex:{item.composite_key}",
                family="cex_dex",
                strategy="cex_dex",
                asset=item.asset,
                venues=[item.cex_venue, item.dex_venue],
                capital_required_usd=capital,
                notional_usd_per_leg=item.target_notional_usd,
                expected_profit_usd_per_deployment=profit,
                expected_return_on_reserved_capital=deployment_return,
                modeled_holding_hours=None,
                source_return_metric="conservative_capture_edge_bps",
                source_return_value=item.conservative_capture_edge_bps,
                exposure_kind="market_neutral",
                conflict_keys=[
                    f"cex:{item.cex_venue}:{item.cex_symbol}",
                    f"venue-symbol:{item.cex_venue}:{item.cex_symbol}",
                    f"dex:ethereum:{item.asset}:{item.route_direction}",
                ],
                evidence_id=item.evidence_id,
                capacity_reference_usd=None,
                capacity_claimed=False,
                allocation_eligible=True,
                executable_eligible=False,
                paper_only=True,
            ))

        if self.alpha_factory is not None:
            alpha_rows = await self.alpha_factory.promoted_candidates(
                snapshot,
                total_capital_usd=total_capital_usd,
            )
            for item in alpha_rows:
                capital = item.capital_required_usd
                deployment_return = item.expected_profit_usd / capital
                exposure: ExposureKind = (
                    "directional_long" if item.direction == "long"
                    else "directional_short" if item.direction == "short"
                    else "market_neutral"
                )
                rows.append(UnifiedPaperCandidate(
                    candidate_id=item.candidate_id,
                    family="alpha",
                    strategy=item.strategy_id,
                    asset=item.asset,
                    venues=[item.venue],
                    capital_required_usd=capital,
                    notional_usd_per_leg=item.notional_usd,
                    expected_profit_usd_per_deployment=item.expected_profit_usd,
                    expected_return_on_reserved_capital=max(0.0, deployment_return),
                    modeled_holding_hours=item.horizon_hours,
                    source_return_metric="forward_ci_health_haircut_net_return",
                    source_return_value=item.expected_net_return,
                    exposure_kind=exposure,
                    conflict_keys=[
                        *item.conflict_keys,
                        f"venue-symbol:{item.venue}:{item.symbol}",
                    ],
                    capacity_reference_usd=None,
                    capacity_claimed=False,
                    allocation_eligible=True,
                    executable_eligible=False,
                    paper_only=True,
                ))

        rows.sort(
            key=lambda item: (
                item.expected_return_on_reserved_capital,
                item.expected_profit_usd_per_deployment,
                -item.capital_required_usd,
            ),
            reverse=True,
        )
        return rows

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        venue_fraction = max_venue_fraction or self.settings.allocator_max_venue_fraction
        asset_fraction = max_asset_fraction or self.settings.allocator_max_asset_fraction
        allocation_limit = max_allocations or self.settings.allocator_max_allocations
        if not 0 < venue_fraction <= 1 or not 0 < asset_fraction <= 1 or allocation_limit <= 0:
            raise ValueError("invalid allocation constraints")

        candidates = await self.candidates(total_capital_usd=total_capital_usd)
        venue_cap = total_capital_usd * venue_fraction
        asset_cap = total_capital_usd * asset_fraction
        venue_used: dict[str, float] = defaultdict(float)
        asset_used: dict[str, float] = defaultdict(float)
        used_conflicts: set[str] = set()
        risk_overlay = PortfolioRiskOverlay(self.settings, total_capital_usd=total_capital_usd)
        allocated = 0.0
        allocations: list[UnifiedPaperAllocation] = []
        skipped: list[dict[str, object]] = []

        for item in candidates:
            if len(allocations) >= allocation_limit:
                skipped.append({"candidate_id": item.candidate_id, "reason": "allocation count limit"})
                continue
            if used_conflicts.intersection(item.conflict_keys):
                skipped.append({"candidate_id": item.candidate_id, "reason": "shared instrument or route conflict"})
                continue
            capital = item.capital_required_usd
            if allocated + capital > total_capital_usd + 1e-9:
                skipped.append({"candidate_id": item.candidate_id, "reason": "total capital constraint"})
                continue
            venue_share = capital / max(1, len(item.venues))
            if any(venue_used[venue] + venue_share > venue_cap + 1e-9 for venue in item.venues):
                skipped.append({"candidate_id": item.candidate_id, "reason": "venue concentration cap"})
                continue
            if asset_used[item.asset] + capital > asset_cap + 1e-9:
                skipped.append({"candidate_id": item.candidate_id, "reason": "asset concentration cap"})
                continue
            risk_decision = risk_overlay.decision(item)
            if not risk_decision.accepted:
                skipped.append({
                    "candidate_id": item.candidate_id,
                    "reason": risk_decision.reason or "portfolio risk budget",
                })
                continue

            allocations.append(UnifiedPaperAllocation(
                candidate_id=item.candidate_id,
                family=item.family,
                strategy=item.strategy,
                asset=item.asset,
                venues=item.venues,
                capital_required_usd=capital,
                notional_usd_per_leg=item.notional_usd_per_leg,
                expected_profit_usd_per_deployment=item.expected_profit_usd_per_deployment,
                expected_return_on_reserved_capital=item.expected_return_on_reserved_capital,
                modeled_holding_hours=item.modeled_holding_hours,
                source_return_metric=item.source_return_metric,
                source_return_value=item.source_return_value,
                exposure_kind=item.exposure_kind,
                evidence_id=item.evidence_id,
                opportunity_id=item.opportunity_id,
                capacity_claimed=False,
                authorizes_execution=False,
                paper_only=True,
            ))
            risk_overlay.register(item)
            allocated += capital
            asset_used[item.asset] += capital
            for venue in item.venues:
                venue_used[venue] += venue_share
            used_conflicts.update(item.conflict_keys)

        profit = sum(item.expected_profit_usd_per_deployment for item in allocations)
        weighted_return = (
            sum(item.capital_required_usd * item.expected_return_on_reserved_capital for item in allocations)
            / total_capital_usd
        )
        return UnifiedPaperAllocationPlan(
            observed_at=datetime.now(timezone.utc),
            total_capital_usd=total_capital_usd,
            allocated_capital_usd=allocated,
            unused_cash_usd=max(0.0, total_capital_usd - allocated),
            expected_profit_usd_current_deployments=profit,
            weighted_expected_return_on_reserved_capital=weighted_return,
            candidate_count=len(candidates),
            allocations=allocations,
            skipped=skipped,
            portfolio_risk_budget=risk_overlay.snapshot(),
            authorizes_execution=False,
            live_execution_eligible=False,
            paper_only=True,
        )
