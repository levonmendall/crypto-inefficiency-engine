from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.cex_dex_composite_statistics import (
    CompositeEdgeStatisticalQualification,
    CompositeEdgeStatisticalService,
)
from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_operations import (
    CexDexOperationalQualification,
    HedgeRecoveryPolicy,
    PaperInventoryPolicy,
    hedge_policy_from_settings,
    qualify_cex_dex_operations,
)
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeLedger
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_statistics import DexStatisticalQualification, DexStatisticalQualificationService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_shadow import (
    StablecoinDepthLedger,
    StablecoinDepthProbeSpec,
    StablecoinDepthStatisticalQualification,
    StablecoinDepthStatisticalService,
    default_probe_specs,
)


class CexDexPaperQualification(BaseModel):
    evidence_id: str
    composite_key: str
    asset: str
    route_direction: str
    target_notional_usd: float = Field(gt=0)
    cex_venue: str
    cex_symbol: str
    dex_venue: str = "DEX:ethereum"
    current_net_edge_bps: float
    conservative_capture_edge_bps: float = Field(ge=0)
    paper_capital_required_usd: float = Field(gt=0)
    route_statistics: DexStatisticalQualification
    composite_statistics: CompositeEdgeStatisticalQualification
    stablecoin_statistics: list[StablecoinDepthStatisticalQualification] = Field(default_factory=list)
    stablecoin_depth_required: bool
    stablecoin_depth_qualified: bool
    operations: CexDexOperationalQualification
    paper_allocation_eligible: bool
    blockers: list[str] = Field(default_factory=list)
    capacity_claimed: bool = False
    executable_eligible: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


class CexDexPaperQualificationProbe(BaseModel):
    observed_at: datetime
    evidence_count: int = Field(ge=0)
    paper_allocation_eligible_count: int = Field(ge=0)
    qualifications: list[CexDexPaperQualification] = Field(default_factory=list)
    executable_eligible: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


class CexDexPaperAllocation(BaseModel):
    evidence_id: str
    composite_key: str
    asset: str
    route_direction: str
    venues: list[str]
    notional_usd_per_leg: float = Field(gt=0)
    capital_required_usd: float = Field(gt=0)
    conservative_capture_edge_bps: float = Field(ge=0)
    conservative_expected_profit_usd: float = Field(ge=0)
    expected_return_on_reserved_capital: float = Field(ge=0)
    capacity_claimed: bool = False
    authorizes_execution: bool = False
    paper_only: bool = True


class CexDexPaperAllocationPlan(BaseModel):
    total_capital_usd: float = Field(gt=0)
    allocated_capital_usd: float = Field(ge=0)
    unused_cash_usd: float = Field(ge=0)
    allocations: list[CexDexPaperAllocation] = Field(default_factory=list)
    skipped: list[dict[str, object]] = Field(default_factory=list)
    capacity_claimed: bool = False
    authorizes_execution: bool = False
    paper_only: bool = True


def _nearest_stablecoin_spec(
    evidence_leg,
    specs: tuple[StablecoinDepthProbeSpec, ...],
    tolerance_fraction: float,
) -> StablecoinDepthProbeSpec | None:
    candidates = [
        spec for spec in specs
        if spec.source_currency.upper() == evidence_leg.source_currency.upper()
        and spec.target_currency.upper() == evidence_leg.target_currency.upper()
    ]
    if not candidates:
        return None
    nearest = min(candidates, key=lambda spec: abs(spec.input_amount - evidence_leg.input_amount))
    tolerance = max(1.0, abs(evidence_leg.input_amount) * max(0.0, tolerance_fraction))
    return nearest if abs(nearest.input_amount - evidence_leg.input_amount) <= tolerance else None


def _paper_inventory_policy(limit_usd_per_side: float) -> PaperInventoryPolicy:
    limit = max(0.0, limit_usd_per_side)
    return PaperInventoryPolicy(
        cex_asset_inventory_usd_per_venue=limit,
        cex_quote_inventory_usd_per_venue=limit,
        dex_asset_inventory_usd=limit,
        dex_quote_inventory_usd=limit,
        source="paper-allocation-budget",
    )


def conservative_capture_edge_bps(
    evidence: CexDexCompositeEvidence,
    composite: CompositeEdgeStatisticalQualification,
    operations: CexDexOperationalQualification,
) -> float:
    survival_lower = composite.hurdle_survival.ci_lower or 0.0
    retained = composite.p10_retained_edge_fraction or 0.0
    statistical_edge = max(0.0, evidence.net_research_edge_bps * min(1.0, retained) * survival_lower)
    return max(0.0, min(statistical_edge, operations.recovery_adjusted_edge_bps))


class CexDexPaperPromotionService:
    def __init__(
        self,
        core: OpportunityService,
        composite_service: CexDexCompositeEvidenceService,
        store: EvidenceStore,
        *,
        hedge_policy: HedgeRecoveryPolicy | None = None,
    ):
        self.core = core
        self.settings: Settings = core.settings
        self.composite_service = composite_service
        self.store = store
        self.route_statistics = DexStatisticalQualificationService(store, self.settings)
        self.composite_statistics = CompositeEdgeStatisticalService(
            CexDexCompositeEdgeLedger(store), self.settings
        )
        self.stablecoin_ledger = StablecoinDepthLedger(store)
        self.stablecoin_statistics = StablecoinDepthStatisticalService(
            self.stablecoin_ledger, self.settings
        )
        self.hedge_policy = hedge_policy or hedge_policy_from_settings(self.settings)
        self.stablecoin_specs = default_probe_specs(self.settings)

    async def live_qualification(
        self,
        *,
        paper_inventory_usd_per_side: float,
    ) -> CexDexPaperQualificationProbe:
        if paper_inventory_usd_per_side < 0:
            raise ValueError("paper_inventory_usd_per_side cannot be negative")
        composite_probe = await self.composite_service.probe()
        snapshot = await self.core.collect_live_evidence()
        venues_by_asset: dict[str, set[str]] = defaultdict(set)
        for quote in snapshot.market_quotes:
            if quote.market_kind == MarketKind.SPOT:
                venues_by_asset[quote.asset.upper()].add(quote.venue)
        inventory = _paper_inventory_policy(paper_inventory_usd_per_side)
        tolerance = self.settings.dex_statistical_notional_tolerance_fraction

        qualifications: list[CexDexPaperQualification] = []
        for evidence in composite_probe.evidence:
            blockers: list[str] = []
            route_model = self.route_statistics.model(
                asset=evidence.asset,
                direction=evidence.route_direction,
                target_notional_usd=evidence.target_notional_usd,
            )
            if not route_model.statistically_qualified:
                blockers.append("DEX route statistical evidence is not qualified")

            composite_model = self.composite_statistics.model(evidence)
            if not composite_model.statistically_qualified:
                blockers.append("fully costed composite-edge statistical evidence is not qualified")

            stable_models: list[StablecoinDepthStatisticalQualification] = []
            stable_required = evidence.conversion_depth_quote is not None
            stable_qualified = True
            if evidence.conversion_depth_quote is not None:
                for leg in evidence.conversion_depth_quote.legs:
                    spec = _nearest_stablecoin_spec(leg, self.stablecoin_specs, tolerance)
                    if spec is None:
                        stable_qualified = False
                        blockers.append(
                            f"no statistically tracked stablecoin depth tier matches {leg.source_currency}->{leg.target_currency} amount"
                        )
                        continue
                    model = self.stablecoin_statistics.model(
                        spec.source_currency, spec.target_currency, spec.input_amount
                    )
                    stable_models.append(model)
                    if not model.statistically_qualified:
                        stable_qualified = False
                if not stable_qualified and not any("stablecoin" in item for item in blockers):
                    blockers.append("stablecoin conversion-depth statistical evidence is not qualified")

            operations = qualify_cex_dex_operations(
                evidence,
                alternate_cex_venues=sorted(venues_by_asset.get(evidence.asset.upper(), set())),
                inventory=inventory,
                hedge_policy=self.hedge_policy,
                settings=self.settings,
            )
            if not operations.paper_operationally_qualified:
                blockers.append("paper inventory/settlement/hedge-recovery model is not qualified")

            eligible = (
                evidence.evidence_complete
                and evidence.route_contiguous_acceptable
                and route_model.statistically_qualified
                and composite_model.statistically_qualified
                and stable_qualified
                and operations.paper_operationally_qualified
            )
            conservative_edge = (
                conservative_capture_edge_bps(evidence, composite_model, operations)
                if eligible
                else 0.0
            )
            if eligible and conservative_edge < self.settings.dex_statistical_min_net_edge_bps:
                eligible = False
                blockers.append("statistically haircutted capture edge is below the configured minimum")
                conservative_edge = 0.0

            qualifications.append(CexDexPaperQualification(
                evidence_id=evidence.evidence_id,
                composite_key=composite_model.composite_key,
                asset=evidence.asset.upper(),
                route_direction=evidence.route_direction,
                target_notional_usd=evidence.target_notional_usd,
                cex_venue=evidence.cex_venue,
                cex_symbol=evidence.cex_symbol,
                current_net_edge_bps=evidence.net_research_edge_bps,
                conservative_capture_edge_bps=conservative_edge,
                paper_capital_required_usd=2.0 * evidence.target_notional_usd,
                route_statistics=route_model,
                composite_statistics=composite_model,
                stablecoin_statistics=stable_models,
                stablecoin_depth_required=stable_required,
                stablecoin_depth_qualified=stable_qualified,
                operations=operations,
                paper_allocation_eligible=eligible,
                blockers=blockers,
                capacity_claimed=False,
                executable_eligible=False,
                live_execution_eligible=False,
                paper_only=True,
            ))

        qualifications.sort(
            key=lambda item: (item.paper_allocation_eligible, item.conservative_capture_edge_bps),
            reverse=True,
        )
        return CexDexPaperQualificationProbe(
            observed_at=datetime.now(timezone.utc),
            evidence_count=len(qualifications),
            paper_allocation_eligible_count=sum(item.paper_allocation_eligible for item in qualifications),
            qualifications=qualifications,
        )

    async def paper_allocation(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> CexDexPaperAllocationPlan:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        venue_fraction = max_venue_fraction or self.settings.allocator_max_venue_fraction
        asset_fraction = max_asset_fraction or self.settings.allocator_max_asset_fraction
        allocation_limit = max_allocations or self.settings.allocator_max_allocations
        if not 0 < venue_fraction <= 1 or not 0 < asset_fraction <= 1 or allocation_limit <= 0:
            raise ValueError("invalid paper allocation constraints")

        # A candidate must be pre-fundable on either side up to half of total capital;
        # the allocator then charges both legs against the total capital budget.
        probe = await self.live_qualification(
            paper_inventory_usd_per_side=total_capital_usd / 2.0
        )
        eligible = [item for item in probe.qualifications if item.paper_allocation_eligible]
        eligible.sort(
            key=lambda item: (
                item.conservative_capture_edge_bps / max(item.paper_capital_required_usd, 1.0),
                item.conservative_capture_edge_bps,
            ),
            reverse=True,
        )

        venue_cap = total_capital_usd * venue_fraction
        asset_cap = total_capital_usd * asset_fraction
        venue_used: dict[str, float] = defaultdict(float)
        asset_used: dict[str, float] = defaultdict(float)
        allocated = 0.0
        allocations: list[CexDexPaperAllocation] = []
        skipped: list[dict[str, object]] = []

        for item in eligible:
            if len(allocations) >= allocation_limit:
                break
            capital = item.paper_capital_required_usd
            if allocated + capital > total_capital_usd + 1e-9:
                skipped.append({"evidence_id": item.evidence_id, "reason": "total capital constraint"})
                continue
            venues = [item.cex_venue, item.dex_venue]
            per_venue = capital / 2.0
            if any(venue_used[venue] + per_venue > venue_cap + 1e-9 for venue in venues):
                skipped.append({"evidence_id": item.evidence_id, "reason": "venue concentration cap"})
                continue
            if asset_used[item.asset] + capital > asset_cap + 1e-9:
                skipped.append({"evidence_id": item.evidence_id, "reason": "asset concentration cap"})
                continue

            profit = item.target_notional_usd * item.conservative_capture_edge_bps / 10_000.0
            return_on_reserved = profit / capital
            allocations.append(CexDexPaperAllocation(
                evidence_id=item.evidence_id,
                composite_key=item.composite_key,
                asset=item.asset,
                route_direction=item.route_direction,
                venues=venues,
                notional_usd_per_leg=item.target_notional_usd,
                capital_required_usd=capital,
                conservative_capture_edge_bps=item.conservative_capture_edge_bps,
                conservative_expected_profit_usd=profit,
                expected_return_on_reserved_capital=return_on_reserved,
            ))
            allocated += capital
            asset_used[item.asset] += capital
            for venue in venues:
                venue_used[venue] += per_venue

        return CexDexPaperAllocationPlan(
            total_capital_usd=total_capital_usd,
            allocated_capital_usd=allocated,
            unused_cash_usd=max(0.0, total_capital_usd - allocated),
            allocations=allocations,
            skipped=skipped,
        )
