from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


CoveragePriority = Literal["critical", "high", "medium"]
CoverageStage = Literal[
    "catalogued",
    "discoverable",
    "economics_modelled",
    "forward_testable",
    "statistically_gated",
    "paper_allocatable",
    "profitability_certifiable",
]

_STAGE_ORDER: dict[CoverageStage, int] = {
    "catalogued": 0,
    "discoverable": 1,
    "economics_modelled": 2,
    "forward_testable": 3,
    "statistically_gated": 4,
    "paper_allocatable": 5,
    "profitability_certifiable": 6,
}


class ProfitMechanismCoverage(BaseModel):
    mechanism_id: str
    name: str
    economic_role: str
    priority: CoveragePriority
    stage: CoverageStage
    implemented_components: list[str] = Field(default_factory=list)
    research_only_components: list[str] = Field(default_factory=list)
    missing_components: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    discovery_available: bool = False
    authoritative_data_available: bool = False
    economics_model_available: bool = False
    forward_test_available: bool = False
    statistical_gate_available: bool = False
    paper_allocation_available: bool = False
    profitability_certification_available: bool = False
    live_execution_available: bool = False
    decision_grade: bool = False
    fully_covered: bool = False
    paper_only: bool = True


class ProfitCoverageSummary(BaseModel):
    version: str
    observed_at: datetime
    objective: str
    taxonomy_version: str = "2026-08-19"
    mechanism_count: int = Field(ge=0)
    catalogued_count: int = Field(ge=0)
    decision_grade_count: int = Field(ge=0)
    paper_capable_count: int = Field(ge=0)
    profitability_certifiable_count: int = Field(ge=0)
    fully_covered_count: int = Field(ge=0)
    taxonomy_coverage_fraction: float = Field(ge=0, le=1)
    decision_grade_coverage_fraction: float = Field(ge=0, le=1)
    paper_capable_coverage_fraction: float = Field(ge=0, le=1)
    profitability_certifiable_coverage_fraction: float = Field(ge=0, le=1)
    unresolved_mechanism_count: int = Field(ge=0)
    unresolved_critical_or_high_count: int = Field(ge=0)
    failure_conclusion_ready: bool = False
    failure_conclusion_blockers: list[str] = Field(default_factory=list)
    success_conclusion_rule: str
    mechanisms: list[ProfitMechanismCoverage]
    live_execution_available: bool = False
    paper_only: bool = True


class ProfitCoverageGap(BaseModel):
    mechanism_id: str
    name: str
    priority: CoveragePriority
    stage: CoverageStage
    missing_components: list[str]
    blockers: list[str]
    next_required_capability: str


def _mechanism(
    *,
    mechanism_id: str,
    name: str,
    economic_role: str,
    priority: CoveragePriority,
    stage: CoverageStage,
    implemented: list[str] | None = None,
    research: list[str] | None = None,
    missing: list[str] | None = None,
    blockers: list[str] | None = None,
    authoritative_data: bool = False,
    economics: bool = False,
    forward: bool = False,
    statistics: bool = False,
    allocation: bool = False,
    certification: bool = False,
) -> ProfitMechanismCoverage:
    implemented = implemented or []
    research = research or []
    missing = missing or []
    blockers = blockers or []
    discovery = _STAGE_ORDER[stage] >= _STAGE_ORDER["discoverable"]
    decision_grade = bool(authoritative_data and economics and forward and statistics)
    fully_covered = bool(
        decision_grade
        and allocation
        and certification
        and not research
        and not missing
        and not blockers
    )
    return ProfitMechanismCoverage(
        mechanism_id=mechanism_id,
        name=name,
        economic_role=economic_role,
        priority=priority,
        stage=stage,
        implemented_components=implemented,
        research_only_components=research,
        missing_components=missing,
        blockers=blockers,
        discovery_available=discovery,
        authoritative_data_available=authoritative_data,
        economics_model_available=economics,
        forward_test_available=forward,
        statistical_gate_available=statistics,
        paper_allocation_available=allocation,
        profitability_certification_available=certification,
        live_execution_available=False,
        decision_grade=decision_grade,
        fully_covered=fully_covered,
        paper_only=True,
    )


def canonical_profit_mechanisms(
    *,
    alpha_families: set[str] | None = None,
    fundamental_authoritative_observation_count: int = 0,
) -> list[ProfitMechanismCoverage]:
    """Canonical economic-mechanism coverage map.

    A mechanism counts as decision-grade only when authoritative data, explicit
    economics, forward testing, and a statistical gate all exist. Cataloguing a
    mechanism or exposing a detector never counts as evaluation.
    """

    alpha_families = alpha_families or set()
    momentum_available = "directional_time_series" in alpha_families
    reversion_available = "directional_reversal" in alpha_families
    factor_registered = "onchain_fundamental" in alpha_families
    factor_data = factor_registered and fundamental_authoritative_observation_count > 0

    rows = [
        _mechanism(
            mechanism_id="price_discrepancy",
            name="Price discrepancy / arbitrage",
            economic_role="Capture simultaneous or convergent pricing differences across venues, instruments, and settlement domains.",
            priority="critical",
            stage="paper_allocatable",
            implemented=["CEX spot dislocation", "CEX↔DEX composite edge", "stablecoin conversion depth"],
            research=["DEX↔DEX", "stablecoin convergence", "cross-chain price discrepancy"],
            blockers=[
                "DEX↔DEX still lacks authoritative pool-specific executable route depth",
                "cross-chain still lacks authoritative bridge fill-time and settlement-risk evidence",
            ],
            authoritative_data=True,
            economics=True,
            forward=True,
            statistics=True,
            allocation=True,
            certification=False,
        ),
        _mechanism(
            mechanism_id="carry",
            name="Carry / basis / funding",
            economic_role="Earn return from term structure, funding dispersion, financing differences, and market-neutral carry.",
            priority="critical",
            stage="paper_allocatable",
            implemented=["funding dispersion", "spot/perpetual basis", "dated-futures basis"],
            missing=["generalized borrow/lend carry graph", "collateral-specific financing dispersion"],
            blockers=["allocator-level realized settlement still needs exact multi-leg carry and funding accrual reconstruction"],
            authoritative_data=True,
            economics=True,
            forward=True,
            statistics=True,
            allocation=True,
            certification=False,
        ),
        _mechanism(
            mechanism_id="yield",
            name="Yield / staking / lending",
            economic_role="Allocate capital to staking, lending, fixed-yield, LP-fee, or incentive income when risk-adjusted yield dominates alternatives.",
            priority="high",
            stage="catalogued",
            missing=["staking yield graph", "lending/borrow market graph", "LP fee yield", "fixed-yield markets", "incentive decay modelling"],
            blockers=["authoritative point-in-time yield, capacity, lockup, slashing/credit/smart-contract risk and exit-cost evidence not integrated"],
        ),
        _mechanism(
            mechanism_id="liquidity_provision",
            name="Liquidity provision / market making",
            economic_role="Earn spread, maker rebates, and liquidity incentives when compensation exceeds adverse selection and inventory risk.",
            priority="high",
            stage="catalogued",
            missing=["maker fill simulator", "inventory-aware quoting", "adverse-selection model", "queue/priority model", "rebate economics"],
            blockers=["requires order-placement/fill evidence and venue-specific maker economics; no execution authority exists"],
        ),
        _mechanism(
            mechanism_id="trend_momentum",
            name="Directional trend / momentum",
            economic_role="Take directional exposure when forward expected return is positive after costs and risk.",
            priority="critical",
            stage="profitability_certifiable" if momentum_available else "catalogued",
            implemented=["time-series momentum"] if momentum_available else [],
            missing=["multi-horizon trend ensemble", "cross-asset trend confirmation"],
            blockers=["allocator certification currently settles only supported spot-long decisions; perpetual-short settlement still needs realized funding"],
            authoritative_data=momentum_available,
            economics=momentum_available,
            forward=momentum_available,
            statistics=momentum_available,
            allocation=momentum_available,
            certification=momentum_available,
        ),
        _mechanism(
            mechanism_id="mean_reversion",
            name="Mean reversion / reversal",
            economic_role="Take directional exposure when statistically abnormal displacement is expected to revert after costs.",
            priority="critical",
            stage="profitability_certifiable" if reversion_available else "catalogued",
            implemented=["robust median/MAD reversal"] if reversion_available else [],
            missing=["cross-venue residual reversion", "multi-horizon reversion ensemble"],
            blockers=["allocator certification currently settles only supported spot-long decisions; perpetual-short settlement still needs realized funding"],
            authoritative_data=reversion_available,
            economics=reversion_available,
            forward=reversion_available,
            statistics=reversion_available,
            allocation=reversion_available,
            certification=reversion_available,
        ),
        _mechanism(
            mechanism_id="fundamental_onchain",
            name="On-chain / fundamental factor alpha",
            economic_role="Predict returns from protocol usage, flows, issuance, holder behaviour, valuation, and other fundamental state variables.",
            priority="high",
            stage="statistically_gated" if factor_data else "discoverable" if factor_registered else "catalogued",
            implemented=["provider-neutral composite factor contract"] if factor_registered else [],
            missing=["authoritative production factor provider"] if not factor_data else [],
            blockers=[] if factor_data else ["no authoritative commercially permitted point-in-time factor observations are currently available"],
            authoritative_data=factor_data,
            economics=factor_registered,
            forward=factor_registered,
            statistics=factor_registered,
            allocation=factor_registered,
            certification=factor_registered,
        ),
        _mechanism(
            mechanism_id="cross_sectional_relative_value",
            name="Cross-sectional / statistical relative value",
            economic_role="Exploit relative expected-return differences among tokens, sectors, baskets, and statistically linked instruments.",
            priority="high",
            stage="catalogued",
            missing=["cross-sectional ranking", "pairs/stat-arb residual model", "sector-neutral baskets", "cointegration/lead-lag validation"],
            blockers=["no dedicated cross-sectional forward promotion family yet"],
        ),
        _mechanism(
            mechanism_id="volatility",
            name="Volatility / options risk premia",
            economic_role="Trade implied-versus-realized volatility, skew, term structure, dispersion, and option-relative value.",
            priority="high",
            stage="discoverable",
            research=["option relative value"],
            missing=["volatility risk premium", "skew/term-structure strategies", "dispersion"],
            blockers=["option L2, fees, Greeks, delta hedge economics and paired capacity are not yet authoritative"],
            authoritative_data=False,
            economics=False,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
        _mechanism(
            mechanism_id="event_driven",
            name="Event-driven alpha",
            economic_role="Estimate conditional return distributions around listings, unlocks, governance, upgrades, expiries, flows, depegs, and other discrete catalysts.",
            priority="high",
            stage="catalogued",
            missing=["canonical event ledger", "point-in-time event taxonomy", "event-response forward cohorts", "event surprise model"],
            blockers=["authoritative timestamped event sources and event-specific forward validation are not integrated"],
        ),
        _mechanism(
            mechanism_id="microstructure",
            name="Market microstructure / order-flow alpha",
            economic_role="Predict short-horizon returns or capture spread from order-book imbalance, trade flow, lead-lag, queue state, and liquidation pressure.",
            priority="high",
            stage="economics_modelled",
            implemented=["L2 depth", "VWAP/slippage", "latency and fill modelling"],
            missing=["order-flow imbalance alpha", "cross-venue lead-lag alpha", "queue-position model", "short-horizon adverse-selection forecast"],
            blockers=["microstructure infrastructure exists, but no dedicated statistically promoted alpha family uses it yet"],
            authoritative_data=True,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
        _mechanism(
            mechanism_id="liquidation_distress",
            name="Liquidation / distress / solver opportunities",
            economic_role="Earn compensation for supplying capital or execution during liquidation, solver-auction, or distressed-flow events.",
            priority="high",
            stage="discoverable",
            research=["liquidation backstop", "solver opportunities"],
            blockers=[
                "authoritative liquidation capacity, expiry, recovery and cost evidence are required",
                "authoritative solver auction, capacity and settlement-guarantee evidence are required",
            ],
        ),
        _mechanism(
            mechanism_id="capital_location_settlement",
            name="Capital-location / settlement optionality",
            economic_role="Earn from having collateral, inventory, stablecoins, or bridge-ready capital pre-positioned where future opportunities are most valuable.",
            priority="high",
            stage="economics_modelled",
            implemented=["CEX↔DEX pre-funded inventory qualification", "stablecoin conversion depth", "venue concentration controls"],
            missing=["dynamic inventory optimizer", "chain/venue capital-location forecast", "idle-capital opportunity-cost model", "rebalancing policy"],
            blockers=["current inventory policy is a qualification constraint, not an adaptive return-generating capital-location strategy"],
            authoritative_data=True,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
    ]
    return rows


def build_profit_coverage_summary(
    *,
    version: str,
    alpha_families: set[str] | None = None,
    fundamental_authoritative_observation_count: int = 0,
) -> ProfitCoverageSummary:
    mechanisms = canonical_profit_mechanisms(
        alpha_families=alpha_families,
        fundamental_authoritative_observation_count=fundamental_authoritative_observation_count,
    )
    total = len(mechanisms)
    catalogued = len(mechanisms)
    decision_grade = sum(row.decision_grade for row in mechanisms)
    paper_capable = sum(row.paper_allocation_available for row in mechanisms)
    certifiable = sum(row.profitability_certification_available for row in mechanisms)
    fully_covered = sum(row.fully_covered for row in mechanisms)
    unresolved = [row for row in mechanisms if not row.fully_covered]
    important_unresolved = [row for row in unresolved if row.priority in {"critical", "high"}]
    failure_blockers = [
        f"{row.mechanism_id}: {row.stage}"
        for row in mechanisms
        if not row.decision_grade
    ]
    return ProfitCoverageSummary(
        version=version,
        observed_at=datetime.now(timezone.utc),
        objective=(
            "Track whether economically distinct crypto profit mechanisms are merely known, "
            "or have enough authoritative economics and forward evidence to support a decision "
            "about product success or failure."
        ),
        mechanism_count=total,
        catalogued_count=catalogued,
        decision_grade_count=decision_grade,
        paper_capable_count=paper_capable,
        profitability_certifiable_count=certifiable,
        fully_covered_count=fully_covered,
        taxonomy_coverage_fraction=catalogued / total if total else 0.0,
        decision_grade_coverage_fraction=decision_grade / total if total else 0.0,
        paper_capable_coverage_fraction=paper_capable / total if total else 0.0,
        profitability_certifiable_coverage_fraction=certifiable / total if total else 0.0,
        unresolved_mechanism_count=len(unresolved),
        unresolved_critical_or_high_count=len(important_unresolved),
        failure_conclusion_ready=not failure_blockers,
        failure_conclusion_blockers=failure_blockers,
        success_conclusion_rule=(
            "Success does not require exhaustive coverage: one or more independent mechanisms must "
            "demonstrate durable positive forward-certified net economics after realistic costs, "
            "risk controls, and portfolio allocation. Failure requires materially broader decision-grade "
            "coverage across the canonical mechanism taxonomy."
        ),
        mechanisms=mechanisms,
        live_execution_available=False,
        paper_only=True,
    )


def profit_coverage_gaps(summary: ProfitCoverageSummary) -> list[ProfitCoverageGap]:
    priority_rank = {"critical": 0, "high": 1, "medium": 2}
    stage_rank = {name: order for name, order in _STAGE_ORDER.items()}
    rows: list[ProfitCoverageGap] = []
    for mechanism in summary.mechanisms:
        if mechanism.fully_covered:
            continue
        if not mechanism.authoritative_data_available:
            next_capability = "authoritative point-in-time data"
        elif not mechanism.economics_model_available:
            next_capability = "fully costed executable economics"
        elif not mechanism.forward_test_available:
            next_capability = "forward/out-of-sample evidence loop"
        elif not mechanism.statistical_gate_available:
            next_capability = "independent statistical promotion gate"
        elif not mechanism.paper_allocation_available:
            next_capability = "paper allocation integration"
        elif not mechanism.profitability_certification_available:
            next_capability = "allocator-level forward profitability settlement"
        else:
            next_capability = "close remaining sub-mechanism coverage gaps"
        rows.append(ProfitCoverageGap(
            mechanism_id=mechanism.mechanism_id,
            name=mechanism.name,
            priority=mechanism.priority,
            stage=mechanism.stage,
            missing_components=mechanism.missing_components,
            blockers=mechanism.blockers,
            next_required_capability=next_capability,
        ))
    rows.sort(key=lambda row: (priority_rank[row.priority], stage_rank[row.stage], row.mechanism_id))
    return rows
