from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class FamilyCapability(BaseModel):
    family: str
    stage: Literal["paper_allocatable", "research_only"]
    discovery_available: bool
    exact_economic_evidence_available: bool
    statistical_evidence_available: bool
    paper_allocation_available: bool
    live_execution_available: bool = False
    blockers: list[str] = Field(default_factory=list)


class PaperV1CompletionStatus(BaseModel):
    version: str
    observed_at: datetime
    paper_v1_complete: bool
    objective: str
    definition_of_done: list[str]
    promotable_family_count: int = Field(ge=0)
    research_only_family_count: int = Field(ge=0)
    families: list[FamilyCapability]
    unified_allocator_available: bool
    durable_evidence_available: bool
    deterministic_replay_available: bool
    multi_horizon_shadow_available: bool
    live_execution_available: bool = False
    live_money_authorized: bool = False
    paper_only: bool = True


def paper_v1_status(version: str) -> PaperV1CompletionStatus:
    families = [
        FamilyCapability(
            family="funding_dispersion",
            stage="paper_allocatable",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=True,
        ),
        FamilyCapability(
            family="spot_perp_basis",
            stage="paper_allocatable",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=True,
        ),
        FamilyCapability(
            family="futures_basis",
            stage="paper_allocatable",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=True,
        ),
        FamilyCapability(
            family="cex_spot_dislocation",
            stage="paper_allocatable",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=True,
            blockers=["short-spot opportunities remain fail-closed when borrow economics are unavailable"],
        ),
        FamilyCapability(
            family="cex_dex",
            stage="paper_allocatable",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=True,
            blockers=[
                "paper promotion still requires sufficient accumulated route, conversion-depth and composite-edge samples",
                "live balances, custody and live settlement are intentionally outside paper V1",
            ],
        ),
        FamilyCapability(
            family="dex_dex",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=False,
            statistical_evidence_available=False,
            paper_allocation_available=False,
            blockers=["independent pool-specific executable route depth is not yet authoritative"],
        ),
        FamilyCapability(
            family="stablecoin_dislocation",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=True,
            statistical_evidence_available=True,
            paper_allocation_available=False,
            blockers=["redemption or market-neutral convergence path is not modeled; directional peg speculation is not promoted"],
        ),
        FamilyCapability(
            family="cross_chain",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=False,
            statistical_evidence_available=False,
            paper_allocation_available=False,
            blockers=["fresh authoritative bridge quote, fill-time and settlement-risk evidence are required"],
        ),
        FamilyCapability(
            family="solver",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=False,
            statistical_evidence_available=False,
            paper_allocation_available=False,
            blockers=["authoritative auction, capacity and settlement-guarantee feed is required"],
        ),
        FamilyCapability(
            family="liquidation_backstop",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=False,
            statistical_evidence_available=False,
            paper_allocation_available=False,
            blockers=["authoritative liquidation capacity, expiry, cost and recovery evidence are required"],
        ),
        FamilyCapability(
            family="option_relative_value",
            stage="research_only",
            discovery_available=True,
            exact_economic_evidence_available=False,
            statistical_evidence_available=False,
            paper_allocation_available=False,
            blockers=["option L2, fees, delta hedge, vega/gamma risk and paired capacity are not yet qualified"],
        ),
    ]
    promotable = sum(item.paper_allocation_available for item in families)
    research_only = sum(item.stage == "research_only" for item in families)
    definition = [
        "canonical strategy-agnostic market/opportunity graph",
        "public provider diagnostics and fail-closed data readiness",
        "explicit fees, financing, collateral, slippage, liquidity and recovery economics",
        "append-only point-in-time evidence with deterministic replay",
        "multi-horizon live shadow evidence and statistically gated empirical calibration",
        "amount-specific DEX routes and stablecoin conversion depth",
        "fully costed CEX↔DEX composite-edge survival and statistical promotion",
        "explicit paper inventory, settlement-dependency and hedge-recovery qualification",
        "strategy-neutral paper allocation across independently qualified families",
        "read-only/paper API observability with zero live execution authority",
    ]
    return PaperV1CompletionStatus(
        version=version,
        observed_at=datetime.now(timezone.utc),
        paper_v1_complete=True,
        objective=(
            "Continuously search accessible crypto markets for structural inefficiencies, price the true "
            "conservative net economics after costs and risk, learn which edges survive market contact, "
            "and allocate paper capital only to independently qualified opportunities."
        ),
        definition_of_done=definition,
        promotable_family_count=promotable,
        research_only_family_count=research_only,
        families=families,
        unified_allocator_available=True,
        durable_evidence_available=True,
        deterministic_replay_available=True,
        multi_horizon_shadow_available=True,
        live_execution_available=False,
        live_money_authorized=False,
        paper_only=True,
    )
