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
    taxonomy_version: str = "2026-08-20-v3.8.1"
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
    event_authoritative_observation_count: int = 0,
    yield_authoritative_observation_count: int = 0,
    option_authoritative_observation_count: int = 0,
    distress_authoritative_observation_count: int = 0,
) -> list[ProfitMechanismCoverage]:
    """Canonical economic-mechanism coverage map.

    Decision-grade remains an evidence claim, not a code-completeness claim. V3.8.1
    reflects the currently deployed paper-settlement capabilities while provider-
    dependent families remain below decision-grade until authoritative point-in-time
    observations and the required forward/statistical evidence actually exist.
    """

    alpha_families = alpha_families or set()
    momentum = "directional_time_series" in alpha_families
    reversion = "directional_reversal" in alpha_families
    factor_registered = "onchain_fundamental" in alpha_families
    cross_sectional = "cross_sectional_relative_value" in alpha_families
    microstructure = "microstructure_orderflow" in alpha_families
    event_registered = "event_driven" in alpha_families
    factor_data = factor_registered and fundamental_authoritative_observation_count > 0
    event_data = event_registered and event_authoritative_observation_count > 0
    yield_data = yield_authoritative_observation_count > 0
    option_data = option_authoritative_observation_count > 0
    distress_data = distress_authoritative_observation_count > 0

    return [
        _mechanism(
            mechanism_id="price_discrepancy",
            name="Price discrepancy / arbitrage",
            economic_role="Capture simultaneous or convergent pricing differences across venues, instruments, and settlement domains.",
            priority="critical",
            stage="profitability_certifiable",
            implemented=[
                "CEX spot dislocation",
                "CEX↔DEX composite edge",
                "stablecoin conversion depth",
                "canonical CEX two-leg visible-L2 settlement",
                "canonical CEX↔DEX amount-specific persisted requalification settlement",
            ],
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
            stage="profitability_certifiable",
            implemented=[
                "funding dispersion",
                "spot/perpetual basis",
                "dated-futures basis",
                "canonical visible-L2 two-leg settlement",
                "observed perpetual funding accrual",
            ],
            research=["provider-neutral generalized yield/carry contract"],
            missing=["collateral-specific financing dispersion"],
            blockers=[],
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
            stage="economics_modelled",
            implemented=[
                "provider-neutral staking/lending/fixed-yield/LP/incentive observation contract",
                "capacity and holding-period model",
                "entry/exit cost annualization",
                "protocol/credit/slashing/liquidation/incentive risk haircuts",
            ],
            missing=["forward realized-yield cohort", "exit-liquidity outcome ledger", "protocol-loss statistical calibration"],
            blockers=([] if yield_data else ["no authoritative commercially permitted point-in-time yield observations are currently available"]),
            authoritative_data=yield_data,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
        _mechanism(
            mechanism_id="liquidity_provision",
            name="Liquidity provision / market making",
            economic_role="Earn spread, maker rebates, and liquidity incentives when compensation exceeds adverse selection and inventory risk.",
            priority="high",
            stage="economics_modelled",
            implemented=[
                "visible-L2 spread/depth economics",
                "fill-probability input contract",
                "maker rebate model",
                "adverse-selection haircut",
                "inventory penalty",
                "queue-model qualification gate",
            ],
            missing=["empirical maker queue-position observations", "empirical maker fill outcomes", "inventory-policy forward cohorts"],
            blockers=["current public execution evidence does not establish empirical maker queue priority or maker fill probability"],
            authoritative_data=True,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
        _mechanism(
            mechanism_id="trend_momentum",
            name="Directional trend / momentum",
            economic_role="Take directional exposure when forward expected return is positive after costs and risk.",
            priority="critical",
            stage="profitability_certifiable" if momentum else "catalogued",
            implemented=(
                [
                    "time-series momentum",
                    "cycle-aware 7/30/90/180-day trend ensemble",
                    "BTC trend and cross-asset breadth confirmation",
                    "bounded Bitcoin-halving-cycle prior",
                    "spot-long and observed-funding perpetual-short settlement",
                ]
                if momentum
                else []
            ),
            missing=[],
            blockers=[],
            authoritative_data=momentum,
            economics=momentum,
            forward=momentum,
            statistics=momentum,
            allocation=momentum,
            certification=momentum,
        ),
        _mechanism(
            mechanism_id="mean_reversion",
            name="Mean reversion / reversal",
            economic_role="Take directional exposure when statistically abnormal displacement is expected to revert after costs.",
            priority="critical",
            stage="profitability_certifiable" if reversion else "catalogued",
            implemented=(
                [
                    "robust median/MAD reversal",
                    "spot-long and observed-funding perpetual-short settlement",
                ]
                if reversion
                else []
            ),
            missing=["cross-venue residual reversion", "multi-horizon reversion ensemble"],
            blockers=[],
            authoritative_data=reversion,
            economics=reversion,
            forward=reversion,
            statistics=reversion,
            allocation=reversion,
            certification=reversion,
        ),
        _mechanism(
            mechanism_id="fundamental_onchain",
            name="On-chain / fundamental factor alpha",
            economic_role="Predict returns from protocol usage, flows, issuance, holder behaviour, valuation, and other fundamental state variables.",
            priority="high",
            stage="profitability_certifiable" if factor_data else "forward_testable" if factor_registered else "catalogued",
            implemented=["provider-neutral point-in-time factor contract", "shared forward/statistical promotion"] if factor_registered else [],
            missing=[] if factor_data else ["authoritative production factor provider"],
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
            stage="profitability_certifiable" if cross_sectional else "catalogued",
            implemented=(
                [
                    "cross-sectional residual ranking",
                    "robust median/MAD dispersion",
                    "shared forward/statistical promotion",
                    "observed-funding perpetual-short settlement for directional legs",
                ]
                if cross_sectional
                else []
            ),
            missing=["sector-neutral baskets", "cointegration/pairs ensemble", "dedicated multi-leg relative-value settlement"],
            blockers=[],
            authoritative_data=cross_sectional,
            economics=cross_sectional,
            forward=cross_sectional,
            statistics=cross_sectional,
            allocation=cross_sectional,
            certification=cross_sectional,
        ),
        _mechanism(
            mechanism_id="volatility",
            name="Volatility / options risk premia",
            economic_role="Trade implied-versus-realized volatility, skew, term structure, dispersion, and option-relative value.",
            priority="high",
            stage="economics_modelled",
            implemented=[
                "provider-neutral option quote/Greeks contract",
                "near-ATM volatility surface grouping",
                "implied-versus-realized volatility risk-premium detection",
                "bid/ask and hedge-cost evidence fields",
            ],
            research=["option relative value", "volatility risk premium"],
            missing=["authoritative option L2/capacity", "delta-hedge forward ledger", "skew/term-structure/dispersion promotion families"],
            blockers=([] if option_data else ["no authoritative commercially permitted point-in-time option observations are currently available"]),
            authoritative_data=option_data,
            economics=True,
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
            stage="profitability_certifiable" if event_data else "forward_testable" if event_registered else "catalogued",
            implemented=["append-only event ledger", "known-at/event-at timestamps", "surprise/confidence model", "shared forward/statistical promotion"] if event_registered else [],
            missing=[] if event_data else ["authoritative timestamped event provider"],
            blockers=[] if event_data else ["no authoritative commercially permitted point-in-time event observations are currently available"],
            authoritative_data=event_data,
            economics=event_registered,
            forward=event_registered,
            statistics=event_registered,
            allocation=event_registered,
            certification=event_registered,
        ),
        _mechanism(
            mechanism_id="microstructure",
            name="Market microstructure / order-flow alpha",
            economic_role="Predict short-horizon returns from order-book imbalance, trade flow, lead-lag, queue state, and liquidation pressure.",
            priority="high",
            stage="profitability_certifiable" if microstructure else "economics_modelled",
            implemented=[
                "L2 depth", "VWAP/slippage", "latency/fill modelling", "L2 depth-imbalance alpha", "shared forward/statistical promotion"
            ] if microstructure else ["L2 depth", "VWAP/slippage", "latency/fill modelling"],
            missing=["trade-flow imbalance", "cross-venue lead-lag ensemble", "empirical maker queue model"],
            blockers=["maker-specific economics remain separate and fail-closed; directional microstructure uses taker/L2 economics only"],
            authoritative_data=True,
            economics=True,
            forward=microstructure,
            statistics=microstructure,
            allocation=microstructure,
            certification=microstructure,
        ),
        _mechanism(
            mechanism_id="liquidation_distress",
            name="Liquidation / distress / solver opportunities",
            economic_role="Earn compensation for supplying capital or execution during liquidation, solver-auction, or distressed-flow events.",
            priority="high",
            stage="economics_modelled",
            implemented=[
                "provider-neutral liquidation/solver/backstop observation contract",
                "capacity, reward, execution-cost and recovery-loss model",
                "capture × settlement probability haircut",
                "failure-state expected-loss economics",
            ],
            missing=["independent capture-probability forward calibration", "auction/order-selection outcome ledger", "recovery settlement validation"],
            blockers=([] if distress_data else ["no authoritative commercially permitted liquidation/solver observations are currently available"]),
            authoritative_data=distress_data,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
        _mechanism(
            mechanism_id="capital_location_settlement",
            name="Capital-location / settlement optionality",
            economic_role="Earn from having collateral, inventory, stablecoins, or bridge-ready capital pre-positioned where future opportunities are most valuable.",
            priority="high",
            stage="economics_modelled",
            implemented=[
                "CEX↔DEX pre-funded inventory qualification",
                "stablecoin conversion depth",
                "venue concentration controls",
                "historical opportunity-incidence location optimizer",
            ],
            missing=["forward location-policy cohort", "authoritative rebalancing/withdrawal/transfer cost and latency evidence", "idle-capital opportunity-cost calibration"],
            blockers=["location recommendations are research-only until their incremental forward value after rebalancing costs is certified"],
            authoritative_data=True,
            economics=True,
            forward=False,
            statistics=False,
            allocation=False,
            certification=False,
        ),
    ]


def build_profit_coverage_summary(
    *,
    version: str,
    alpha_families: set[str] | None = None,
    fundamental_authoritative_observation_count: int = 0,
    event_authoritative_observation_count: int = 0,
    yield_authoritative_observation_count: int = 0,
    option_authoritative_observation_count: int = 0,
    distress_authoritative_observation_count: int = 0,
) -> ProfitCoverageSummary:
    mechanisms = canonical_profit_mechanisms(
        alpha_families=alpha_families,
        fundamental_authoritative_observation_count=fundamental_authoritative_observation_count,
        event_authoritative_observation_count=event_authoritative_observation_count,
        yield_authoritative_observation_count=yield_authoritative_observation_count,
        option_authoritative_observation_count=option_authoritative_observation_count,
        distress_authoritative_observation_count=distress_authoritative_observation_count,
    )
    total = len(mechanisms)
    decision_grade = sum(row.decision_grade for row in mechanisms)
    paper_capable = sum(row.paper_allocation_available for row in mechanisms)
    certifiable = sum(row.profitability_certification_available for row in mechanisms)
    fully_covered = sum(row.fully_covered for row in mechanisms)
    unresolved = [row for row in mechanisms if not row.fully_covered]
    important_unresolved = [row for row in unresolved if row.priority in {"critical", "high"}]
    failure_blockers = [f"{row.mechanism_id}: {row.stage}" for row in mechanisms if not row.decision_grade]
    return ProfitCoverageSummary(
        version=version,
        observed_at=datetime.now(timezone.utc),
        objective=(
            "Track whether economically distinct crypto profit mechanisms are merely implemented as research, "
            "or have enough authoritative economics and forward evidence to support a decision about product success or failure."
        ),
        mechanism_count=total,
        catalogued_count=total,
        decision_grade_count=decision_grade,
        paper_capable_count=paper_capable,
        profitability_certifiable_count=certifiable,
        fully_covered_count=fully_covered,
        taxonomy_coverage_fraction=1.0 if total else 0.0,
        decision_grade_coverage_fraction=decision_grade / total if total else 0.0,
        paper_capable_coverage_fraction=paper_capable / total if total else 0.0,
        profitability_certifiable_coverage_fraction=certifiable / total if total else 0.0,
        unresolved_mechanism_count=len(unresolved),
        unresolved_critical_or_high_count=len(important_unresolved),
        failure_conclusion_ready=not failure_blockers,
        failure_conclusion_blockers=failure_blockers,
        success_conclusion_rule=(
            "Success does not require exhaustive coverage: one or more independent mechanisms must demonstrate durable "
            "positive forward-certified net economics after realistic costs, risk controls, and portfolio allocation. "
            "Failure requires decision-grade coverage across the canonical mechanism taxonomy; code-complete research "
            "pipelines without authoritative evidence do not satisfy that standard."
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
