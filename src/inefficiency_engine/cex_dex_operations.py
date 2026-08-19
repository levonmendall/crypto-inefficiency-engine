from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.config import Settings
from inefficiency_engine.models import MarketKind
from inefficiency_engine.service import OpportunityService


class PaperInventoryPolicy(BaseModel):
    """Explicit simulated pre-funded inventory limits for paper qualification.

    Values are USD-equivalent risk limits, not claims about live account balances.
    They exist solely to prevent the research layer from assuming that capital can
    teleport between a CEX and a DEX during an opportunity.
    """

    cex_asset_inventory_usd_per_venue: float = Field(ge=0)
    cex_quote_inventory_usd_per_venue: float = Field(ge=0)
    dex_asset_inventory_usd: float = Field(ge=0)
    dex_quote_inventory_usd: float = Field(ge=0)
    source: str = "paper-policy"
    authoritative_live_balance: bool = False
    paper_only: bool = True


class HedgeRecoveryPolicy(BaseModel):
    max_unhedged_seconds: float = Field(gt=0)
    reserve_buffer_bps: float = Field(ge=0)
    minimum_alternate_cex_venues: int = Field(ge=0)
    paper_only: bool = True


class CexDexOperationalQualification(BaseModel):
    evidence_id: str
    asset: str
    route_direction: str
    target_notional_usd: float = Field(gt=0)
    cex_venue: str
    cex_symbol: str
    cex_quote_currency: str
    route_quote_currency: str
    required_cex_asset_inventory_usd: float = Field(ge=0)
    required_cex_quote_inventory_usd: float = Field(ge=0)
    required_dex_asset_inventory_usd: float = Field(ge=0)
    required_dex_quote_inventory_usd: float = Field(ge=0)
    alternate_cex_venues: list[str] = Field(default_factory=list)
    inventory_prefunded: bool
    settlement_dependency_during_trade: bool
    settlement_qualified: bool
    hedge_recovery_qualified: bool
    recovery_adjusted_edge_bps: float
    minimum_net_edge_bps: float
    paper_operationally_qualified: bool
    blockers: list[str] = Field(default_factory=list)
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    live_balance_verified: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


class CexDexOperationalProbe(BaseModel):
    observed_at: datetime
    evidence_count: int = Field(ge=0)
    paper_operationally_qualified_count: int = Field(ge=0)
    qualifications: list[CexDexOperationalQualification] = Field(default_factory=list)
    live_balance_verified: bool = False
    live_execution_eligible: bool = False
    paper_only: bool = True


def inventory_policy_from_settings(settings: Settings) -> PaperInventoryPolicy:
    # These optional settings deliberately default to zero. A deployment must make
    # an explicit paper-capital assumption before operational qualification can pass.
    return PaperInventoryPolicy(
        cex_asset_inventory_usd_per_venue=max(
            0.0, float(getattr(settings, "cex_dex_paper_cex_asset_inventory_usd", 0.0))
        ),
        cex_quote_inventory_usd_per_venue=max(
            0.0, float(getattr(settings, "cex_dex_paper_cex_quote_inventory_usd", 0.0))
        ),
        dex_asset_inventory_usd=max(
            0.0, float(getattr(settings, "cex_dex_paper_dex_asset_inventory_usd", 0.0))
        ),
        dex_quote_inventory_usd=max(
            0.0, float(getattr(settings, "cex_dex_paper_dex_quote_inventory_usd", 0.0))
        ),
    )


def hedge_policy_from_settings(settings: Settings) -> HedgeRecoveryPolicy:
    return HedgeRecoveryPolicy(
        max_unhedged_seconds=max(
            0.001, float(getattr(settings, "cex_dex_paper_max_unhedged_seconds", 2.0))
        ),
        reserve_buffer_bps=max(
            0.0, float(getattr(settings, "cex_dex_paper_recovery_buffer_bps", 25.0))
        ),
        minimum_alternate_cex_venues=max(
            0, int(getattr(settings, "cex_dex_paper_min_alternate_cex_venues", 1))
        ),
    )


def _inventory_requirements(evidence: CexDexCompositeEvidence) -> tuple[float, float, float, float]:
    # The composite contract retains the exact route notional. Use USD-equivalent
    # inventory limits for the paper model rather than pretending to know live balances.
    notional = max(evidence.target_notional_usd, evidence.route_quote_notional_usd_proxy)
    if evidence.route_direction == "buy_asset":
        # DEX spends quote to acquire asset while CEX supplies asset to the hedge leg.
        return notional, 0.0, 0.0, notional
    if evidence.route_direction == "sell_asset":
        # DEX supplies asset while CEX spends quote to acquire the hedge asset.
        return 0.0, notional, notional, 0.0
    raise ValueError(f"unsupported route direction: {evidence.route_direction}")


def qualify_cex_dex_operations(
    evidence: CexDexCompositeEvidence,
    *,
    alternate_cex_venues: list[str],
    inventory: PaperInventoryPolicy,
    hedge_policy: HedgeRecoveryPolicy,
    settings: Settings,
) -> CexDexOperationalQualification:
    cex_asset, cex_quote, dex_asset, dex_quote = _inventory_requirements(evidence)
    blockers: list[str] = []

    inventory_prefunded = (
        cex_asset <= inventory.cex_asset_inventory_usd_per_venue + 1e-9
        and cex_quote <= inventory.cex_quote_inventory_usd_per_venue + 1e-9
        and dex_asset <= inventory.dex_asset_inventory_usd + 1e-9
        and dex_quote <= inventory.dex_quote_inventory_usd + 1e-9
    )
    if not inventory_prefunded:
        blockers.append("paper pre-funded inventory limit is insufficient for both legs")

    # A qualifying paper trade must be pre-funded. Therefore no bridge, deposit, or
    # withdrawal is allowed to be a synchronous dependency of the trade itself.
    settlement_dependency_during_trade = not inventory_prefunded
    settlement_qualified = inventory_prefunded and not settlement_dependency_during_trade
    if not settlement_qualified:
        blockers.append("trade would depend on cross-venue settlement during the opportunity")

    alternates = sorted({venue for venue in alternate_cex_venues if venue.lower() != evidence.cex_venue.lower()})
    recovery_adjusted_edge = evidence.net_research_edge_bps - hedge_policy.reserve_buffer_bps
    hedge_recovery_qualified = (
        len(alternates) >= hedge_policy.minimum_alternate_cex_venues
        and recovery_adjusted_edge >= settings.dex_statistical_min_net_edge_bps
    )
    if len(alternates) < hedge_policy.minimum_alternate_cex_venues:
        blockers.append("insufficient independent CEX venues for modeled hedge recovery")
    if recovery_adjusted_edge < settings.dex_statistical_min_net_edge_bps:
        blockers.append("net edge does not survive the configured hedge-recovery reserve")

    if not evidence.evidence_complete:
        blockers.append("same-notional composite evidence is incomplete")
    if not evidence.route_contiguous_acceptable:
        blockers.append("DEX route is outside the contiguous acceptable frontier")

    paper_qualified = (
        evidence.evidence_complete
        and evidence.route_contiguous_acceptable
        and settlement_qualified
        and hedge_recovery_qualified
    )

    return CexDexOperationalQualification(
        evidence_id=evidence.evidence_id,
        asset=evidence.asset.upper(),
        route_direction=evidence.route_direction,
        target_notional_usd=evidence.target_notional_usd,
        cex_venue=evidence.cex_venue,
        cex_symbol=evidence.cex_symbol,
        cex_quote_currency=evidence.cex_quote_currency,
        route_quote_currency=evidence.route_quote_currency,
        required_cex_asset_inventory_usd=cex_asset,
        required_cex_quote_inventory_usd=cex_quote,
        required_dex_asset_inventory_usd=dex_asset,
        required_dex_quote_inventory_usd=dex_quote,
        alternate_cex_venues=alternates,
        inventory_prefunded=inventory_prefunded,
        settlement_dependency_during_trade=settlement_dependency_during_trade,
        settlement_qualified=settlement_qualified,
        hedge_recovery_qualified=hedge_recovery_qualified,
        recovery_adjusted_edge_bps=recovery_adjusted_edge,
        minimum_net_edge_bps=settings.dex_statistical_min_net_edge_bps,
        paper_operationally_qualified=paper_qualified,
        blockers=blockers,
        capacity_claimed=False,
        allocation_eligible=False,
        executable_eligible=False,
        live_balance_verified=False,
        live_execution_eligible=False,
        paper_only=True,
    )


class CexDexOperationalQualificationService:
    def __init__(
        self,
        core: OpportunityService,
        composite_service: CexDexCompositeEvidenceService,
        *,
        inventory_policy: PaperInventoryPolicy | None = None,
        hedge_policy: HedgeRecoveryPolicy | None = None,
    ):
        self.core = core
        self.settings = core.settings
        self.composite_service = composite_service
        self.inventory_policy = inventory_policy or inventory_policy_from_settings(self.settings)
        self.hedge_policy = hedge_policy or hedge_policy_from_settings(self.settings)

    async def live_qualification(self) -> CexDexOperationalProbe:
        composite = await self.composite_service.probe()
        snapshot = await self.core.collect_live_evidence()
        venues_by_asset: dict[str, set[str]] = defaultdict(set)
        for quote in snapshot.market_quotes:
            if quote.market_kind == MarketKind.SPOT:
                venues_by_asset[quote.asset.upper()].add(quote.venue)

        rows = [
            qualify_cex_dex_operations(
                evidence,
                alternate_cex_venues=sorted(venues_by_asset.get(evidence.asset.upper(), set())),
                inventory=self.inventory_policy,
                hedge_policy=self.hedge_policy,
                settings=self.settings,
            )
            for evidence in composite.evidence
        ]
        rows.sort(
            key=lambda item: (item.paper_operationally_qualified, item.recovery_adjusted_edge_bps),
            reverse=True,
        )
        return CexDexOperationalProbe(
            observed_at=datetime.now(timezone.utc),
            evidence_count=len(rows),
            paper_operationally_qualified_count=sum(item.paper_operationally_qualified for item in rows),
            qualifications=rows,
            live_balance_verified=False,
            live_execution_eligible=False,
            paper_only=True,
        )
