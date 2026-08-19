from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert, select

from inefficiency_engine.alpha_factory import AlphaForwardOutcome, _mean_lower, _wilson_lower
from inefficiency_engine.allocation_certification import AllocationForwardCertificationService, PaperAllocationOutcome
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.profit_coverage import ProfitMechanismCoverage, build_profit_coverage_summary
from inefficiency_engine.research_mechanisms import (
    CapitalLocationResearchService,
    DistressResearchService,
    VolatilityResearchService,
    YieldResearchService,
)


OperatingState = Literal[
    "provider_gap",
    "collecting",
    "poor_economics",
    "statistical_failure",
    "execution_blocked",
    "settlement_blocked",
    "certifying",
    "certified",
]

_ALPHA_MECHANISM_FAMILIES: dict[str, str] = {
    "trend_momentum": "directional_time_series",
    "mean_reversion": "directional_reversal",
    "fundamental_onchain": "onchain_fundamental",
    "cross_sectional_relative_value": "cross_sectional_relative_value",
    "event_driven": "event_driven",
    "microstructure": "microstructure_orderflow",
}


class MechanismOperatingStatus(BaseModel):
    mechanism_id: str
    name: str
    state: OperatingState
    stage: str
    provider_ready: bool
    authoritative_observation_count: int = Field(default=0, ge=0)
    economic_candidate_count: int = Field(default=0, ge=0)
    forward_signal_count: int = Field(default=0, ge=0)
    independent_forward_outcome_count: int = Field(default=0, ge=0)
    current_candidate_count: int = Field(default=0, ge=0)
    current_statistically_qualified_count: int = Field(default=0, ge=0)
    current_promoted_count: int = Field(default=0, ge=0)
    settled_allocator_outcome_count: int = Field(default=0, ge=0)
    mean_forward_net_return: float | None = None
    mean_forward_net_return_ci_lower: float | None = None
    forward_hit_rate: float | None = None
    forward_hit_rate_ci_lower: float | None = None
    allocator_realized_profit_usd: float | None = None
    allocator_mean_net_return_ci_lower: float | None = None
    allocator_profitable_rate_ci_lower: float | None = None
    profitability_certified: bool = False
    primary_reason: str
    next_action: str
    blockers: list[str] = Field(default_factory=list)
    paper_only: bool = True
    live_execution_authority: bool = False


class OperatingCertificationSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    version: str
    public_market_provider_healthy: bool
    public_market_surface_count: int = Field(ge=0)
    public_market_surface_ok_count: int = Field(ge=0)
    public_order_book_probe_count: int = Field(ge=0)
    public_order_book_probe_ok_count: int = Field(ge=0)
    market_quote_count: int = Field(ge=0)
    funding_quote_count: int = Field(ge=0)
    mechanism_count: int = Field(ge=0)
    provider_gap_count: int = Field(ge=0)
    collecting_count: int = Field(ge=0)
    poor_economics_count: int = Field(ge=0)
    blocked_count: int = Field(ge=0)
    certifying_count: int = Field(ge=0)
    certified_count: int = Field(ge=0)
    all_mechanisms_decision_grade: bool = False
    failure_conclusion_ready: bool = False
    mechanisms: list[MechanismOperatingStatus]
    paper_only: bool = True
    live_execution_authority: bool = False


class OperatingCertificationCycle(BaseModel):
    cycle_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    observed_at: datetime
    snapshot_id: str
    mechanism_count: int = Field(ge=0)
    certified_count: int = Field(ge=0)
    provider_gap_count: int = Field(ge=0)
    poor_economics_count: int = Field(ge=0)
    paper_only: bool = True


def _serialized(value: BaseModel) -> tuple[str, str]:
    raw = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return raw, hashlib.sha256(raw.encode()).hexdigest()


class OperatingCertificationLedger:
    """Append-only point-in-time operating/profitability certification history."""

    def __init__(self, store):
        self.store = store
        metadata = MetaData()
        self.snapshots = Table(
            "operating_certification_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(64), nullable=False, unique=True),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", String(64), nullable=False),
        )
        Index("ix_operating_certification_observed", self.snapshots.c.observed_at)
        metadata.create_all(store.engine)

    def record(self, snapshot: OperatingCertificationSnapshot) -> str:
        raw, lineage = _serialized(snapshot)
        with self.store.engine.begin() as db:
            exists = db.execute(
                select(self.snapshots.c.snapshot_id).where(self.snapshots.c.snapshot_id == snapshot.snapshot_id)
            ).scalar_one_or_none()
            if exists is None:
                db.execute(insert(self.snapshots), {
                    "snapshot_id": snapshot.snapshot_id,
                    "observed_at": snapshot.observed_at.isoformat(),
                    "payload_json": raw,
                    "lineage_hash": lineage,
                })
        return snapshot.snapshot_id

    def latest(self) -> OperatingCertificationSnapshot | None:
        with self.store.engine.connect() as db:
            payload = db.execute(
                select(self.snapshots.c.payload_json).order_by(self.snapshots.c.id.desc()).limit(1)
            ).scalar_one_or_none()
        return OperatingCertificationSnapshot.model_validate_json(payload) if payload else None

    def history(self, *, limit: int = 50) -> list[OperatingCertificationSnapshot]:
        limit = max(1, min(500, int(limit)))
        with self.store.engine.connect() as db:
            payloads = list(db.execute(
                select(self.snapshots.c.payload_json).order_by(self.snapshots.c.id.desc()).limit(limit)
            ).scalars())
        return [OperatingCertificationSnapshot.model_validate_json(payload) for payload in payloads]

    def summary(self) -> dict[str, object]:
        latest = self.latest()
        with self.store.engine.connect() as db:
            count = len(list(db.execute(select(self.snapshots.c.id)).scalars()))
        return {
            "snapshot_count": count,
            "latest": latest.model_dump(mode="json") if latest is not None else None,
            "paper_only": True,
            "live_execution_authority": False,
        }


def _independent_family_outcomes(alpha_factory: ExpandedAlphaFactoryService) -> dict[str, list[AlphaForwardOutcome]]:
    grouped: dict[tuple[str, str, str], list[AlphaForwardOutcome]] = defaultdict(list)
    for outcome in alpha_factory.ledger.outcomes():
        grouped[(outcome.strategy_id, outcome.asset, outcome.direction)].append(outcome)
    by_family: dict[str, list[AlphaForwardOutcome]] = defaultdict(list)
    for rows in grouped.values():
        for outcome in alpha_factory._independent_outcomes(rows):
            by_family[outcome.family].append(outcome)
    return by_family


def _family_signal_counts(alpha_factory: ExpandedAlphaFactoryService) -> dict[str, int]:
    table = alpha_factory.ledger.events
    with alpha_factory.store.engine.connect() as db:
        rows = list(db.execute(select(table.c.family, table.c.event_type).order_by(table.c.id)))
    counts: dict[str, int] = defaultdict(int)
    for family, event_type in rows:
        if event_type == "signal":
            counts[str(family)] += 1
    return dict(counts)


def _forward_stats(rows: list[AlphaForwardOutcome]) -> dict[str, float | int | None]:
    values = [row.realized_net_return for row in rows]
    positives = sum(value > 0 for value in values)
    return {
        "count": len(values),
        "mean": statistics.fmean(values) if values else None,
        "mean_lower": _mean_lower(values),
        "hit_rate": positives / len(values) if values else None,
        "hit_lower": _wilson_lower(positives, len(values)),
    }


def _allocator_family_stats(
    rows: list[PaperAllocationOutcome],
    strategy_to_family: dict[str, str],
) -> dict[str, dict[str, float | int | None]]:
    grouped: dict[str, list[PaperAllocationOutcome]] = defaultdict(list)
    for row in rows:
        family = strategy_to_family.get(row.strategy)
        if family is not None:
            grouped[family].append(row)
    result: dict[str, dict[str, float | int | None]] = {}
    for family, outcomes in grouped.items():
        values = [row.realized_net_return for row in outcomes]
        positives = sum(value > 0 for value in values)
        result[family] = {
            "count": len(outcomes),
            "realized_profit": sum(row.realized_profit_usd for row in outcomes),
            "mean_lower": _mean_lower(values),
            "hit_lower": _wilson_lower(positives, len(values)),
        }
    return result


class OperatingCertificationService:
    """Interpret durable evidence without creating allocation or execution authority."""

    def __init__(
        self,
        core,
        store,
        alpha_factory: ExpandedAlphaFactoryService,
        allocation_certification: AllocationForwardCertificationService,
        *,
        version: str,
    ):
        self.core = core
        self.store = store
        self.alpha_factory = alpha_factory
        self.allocation_certification = allocation_certification
        self.version = version
        self.ledger = OperatingCertificationLedger(store)
        self.yield_service = YieldResearchService(store)
        self.volatility_service = VolatilityResearchService(store)
        self.distress_service = DistressResearchService(store)
        self.location_service = CapitalLocationResearchService(store)

    @property
    def min_forward_samples(self) -> int:
        return max(1, int(self.core.settings.alpha_min_forward_samples))

    @property
    def min_allocator_settled_trials(self) -> int:
        return max(5, int(getattr(self.core.settings, "operating_certification_min_settled_trials", 20)))

    @property
    def min_allocator_profitable_rate_lower(self) -> float:
        return float(getattr(self.core.settings, "operating_certification_min_profitable_rate_lower", 0.50))

    @staticmethod
    def _research_state(
        coverage: ProfitMechanismCoverage,
        *,
        authoritative_count: int,
        economic_candidate_count: int,
        best_economics: float | None,
        next_evidence_action: str,
    ) -> tuple[OperatingState, str, str]:
        if authoritative_count <= 0:
            return (
                "provider_gap",
                "authoritative point-in-time evidence is missing",
                "connect or qualify an authoritative provider and begin append-only observations",
            )
        if best_economics is not None and best_economics <= 0:
            return (
                "poor_economics",
                "authoritative observations exist but the best conservative economics are non-positive",
                "continue observing; do not promote unless after-cost/risk economics become positive",
            )
        if not coverage.forward_test_available:
            return (
                "collecting",
                "authoritative economics exist but independent forward outcome evidence is incomplete",
                next_evidence_action,
            )
        if economic_candidate_count <= 0:
            return (
                "collecting",
                "no current qualifying economic candidate is present",
                "continue point-in-time evidence collection without lowering thresholds",
            )
        return (
            "certifying",
            "evidence is available and the mechanism is progressing through certification",
            next_evidence_action,
        )

    def _alpha_state(
        self,
        coverage: ProfitMechanismCoverage,
        family: str,
        *,
        signal_count: int,
        forward: dict[str, float | int | None],
        current_candidate_count: int,
        qualified_count: int,
        promoted_count: int,
        allocator: dict[str, float | int | None] | None,
        provider_ready: bool,
    ) -> tuple[OperatingState, str, str, bool]:
        del family
        count = int(forward.get("count") or 0)
        mean = forward.get("mean")
        mean_lower = forward.get("mean_lower")
        if not provider_ready or not coverage.authoritative_data_available:
            return (
                "provider_gap",
                "required authoritative input evidence is not currently available",
                "restore or connect the required authoritative provider before interpreting strategy economics",
                False,
            )
        if signal_count <= 0 or count < self.min_forward_samples:
            return (
                "collecting",
                f"independent forward evidence is accumulating ({count}/{self.min_forward_samples} outcomes)",
                "keep the worker running until the predeclared independent forward sample requirement is met",
                False,
            )
        if isinstance(mean, (float, int)) and mean <= 0:
            return (
                "poor_economics",
                "the completed independent forward cohort has non-positive mean net return",
                "continue evidence collection and allow the strategy to remain unallocated unless economics recover",
                False,
            )
        required = float(self.core.settings.alpha_min_forward_mean_return)
        if mean_lower is None or float(mean_lower) <= required:
            return (
                "statistical_failure",
                "forward returns are not statistically strong enough after the confidence hurdle",
                "continue independent forward testing; do not weaken the confidence or return hurdle",
                False,
            )
        if current_candidate_count <= 0:
            return (
                "collecting",
                "historical forward evidence is constructive but there is no current signal to exercise promotion gates",
                "continue running until the strategy produces another current opportunity under unchanged rules",
                False,
            )
        if qualified_count <= 0:
            return (
                "statistical_failure",
                "current candidates do not pass the complete candidate-level statistical/regime gate",
                "continue forward testing across independent regimes without lowering promotion requirements",
                False,
            )
        if promoted_count <= 0:
            return (
                "execution_blocked",
                "statistically qualified candidates fail current L2, cost, capacity, or adaptive-health promotion",
                "keep collecting fresh execution evidence and require positive capturable economics after current costs",
                False,
            )
        settled = int((allocator or {}).get("count") or 0)
        realized_profit = float((allocator or {}).get("realized_profit") or 0.0)
        allocator_mean_lower = (allocator or {}).get("mean_lower")
        allocator_hit_lower = (allocator or {}).get("hit_lower")
        certified = bool(
            settled >= self.min_allocator_settled_trials
            and allocator_mean_lower is not None
            and float(allocator_mean_lower) > 0
            and allocator_hit_lower is not None
            and float(allocator_hit_lower) >= self.min_allocator_profitable_rate_lower
            and realized_profit > 0
        )
        if certified:
            return (
                "certified",
                "the strategy has positive statistically conservative allocator-level forward profitability",
                "maintain forward monitoring; revoke certification automatically if future evidence degrades",
                True,
            )
        return (
            "certifying",
            f"strategy is promotable; allocator settlement evidence is accumulating ({settled}/{self.min_allocator_settled_trials})",
            "continue allocator forward settlement until the profitability certification cohort is complete",
            False,
        )

    @staticmethod
    def _base_status(
        coverage: ProfitMechanismCoverage,
        *,
        state: OperatingState,
        provider_ready: bool,
        reason: str,
        next_action: str,
        authoritative_count: int = 0,
        economic_candidate_count: int = 0,
    ) -> MechanismOperatingStatus:
        return MechanismOperatingStatus(
            mechanism_id=coverage.mechanism_id,
            name=coverage.name,
            state=state,
            stage=coverage.stage,
            provider_ready=provider_ready,
            authoritative_observation_count=authoritative_count,
            economic_candidate_count=economic_candidate_count,
            primary_reason=reason,
            next_action=next_action,
            blockers=list(coverage.blockers),
        )

    async def run_cycle(self, *, total_capital_usd: float = 100000.0) -> OperatingCertificationCycle:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")

        diagnostic = await self.core.provider_diagnostic()
        live_snapshot = await self.core.collect_live_executability()
        candidates = self.alpha_factory.discover(live_snapshot, total_capital_usd=total_capital_usd)
        qualifications = {candidate.candidate_id: self.alpha_factory.qualification(candidate) for candidate in candidates}
        promotion_error: str | None = None
        try:
            promoted = await self.alpha_factory.promoted_candidates(live_snapshot, total_capital_usd=total_capital_usd)
        except Exception as exc:
            promoted = []
            promotion_error = type(exc).__name__

        fundamental_summary = self.alpha_factory.fundamental_summary()
        event_summary = self.alpha_factory.event_summary()
        yield_summary = self.yield_service.summary()
        volatility_summary = self.volatility_service.summary()
        distress_summary = self.distress_service.summary()
        yield_candidates = self.yield_service.candidates(now=live_snapshot.completed_at)
        distress_candidates = self.distress_service.candidates(now=live_snapshot.completed_at)
        location_plan = self.location_service.plan(reserve_capital_usd=total_capital_usd)

        families = {manifest.family for manifest in self.alpha_factory.manifests()}
        coverage_summary = build_profit_coverage_summary(
            version=self.version,
            alpha_families=families,
            fundamental_authoritative_observation_count=int(fundamental_summary.get("authoritative_count", 0)),
            event_authoritative_observation_count=int(event_summary.get("authoritative_count", 0)),
            yield_authoritative_observation_count=int(yield_summary.get("authoritative_count", 0)),
            option_authoritative_observation_count=int(volatility_summary.get("authoritative_count", 0)),
            distress_authoritative_observation_count=int(distress_summary.get("authoritative_count", 0)),
        )

        family_outcomes = _independent_family_outcomes(self.alpha_factory)
        family_forward = {family: _forward_stats(rows) for family, rows in family_outcomes.items()}
        signal_counts = _family_signal_counts(self.alpha_factory)
        strategy_to_family = {manifest.strategy_id: manifest.family for manifest in self.alpha_factory.manifests()}
        allocator_stats = _allocator_family_stats(self.allocation_certification.ledger.outcomes(), strategy_to_family)

        candidates_by_family: dict[str, int] = defaultdict(int)
        qualified_by_family: dict[str, int] = defaultdict(int)
        promoted_by_family: dict[str, int] = defaultdict(int)
        for candidate in candidates:
            candidates_by_family[candidate.family] += 1
            if qualifications[candidate.candidate_id].statistically_qualified:
                qualified_by_family[candidate.family] += 1
        for candidate in promoted:
            promoted_by_family[candidate.family] += 1

        surface_ok = sum(surface.ok for surface in diagnostic.surfaces)
        book_ok = sum(book.ok for book in diagnostic.order_books)
        market_provider_ready = diagnostic.market_quote_count > 0 and surface_ok > 0
        funding_provider_ready = diagnostic.funding_quote_count > 0
        l2_provider_ready = book_ok > 0 or bool(live_snapshot.order_books)

        statuses: list[MechanismOperatingStatus] = []
        for coverage in coverage_summary.mechanisms:
            family = _ALPHA_MECHANISM_FAMILIES.get(coverage.mechanism_id)
            if family is not None:
                provider_ready = market_provider_ready
                if coverage.mechanism_id == "microstructure":
                    provider_ready = provider_ready and l2_provider_ready
                elif coverage.mechanism_id == "fundamental_onchain":
                    provider_ready = int(fundamental_summary.get("authoritative_count", 0)) > 0
                elif coverage.mechanism_id == "event_driven":
                    provider_ready = int(event_summary.get("authoritative_count", 0)) > 0

                forward = family_forward.get(family, {
                    "count": 0,
                    "mean": None,
                    "mean_lower": None,
                    "hit_rate": None,
                    "hit_lower": None,
                })
                allocator = allocator_stats.get(family)
                state, reason, next_action, certified = self._alpha_state(
                    coverage,
                    family,
                    signal_count=signal_counts.get(family, 0),
                    forward=forward,
                    current_candidate_count=candidates_by_family.get(family, 0),
                    qualified_count=qualified_by_family.get(family, 0),
                    promoted_count=promoted_by_family.get(family, 0),
                    allocator=allocator,
                    provider_ready=provider_ready,
                )
                if coverage.mechanism_id == "fundamental_onchain":
                    authoritative_count = int(fundamental_summary.get("authoritative_count", 0))
                elif coverage.mechanism_id == "event_driven":
                    authoritative_count = int(event_summary.get("authoritative_count", 0))
                else:
                    authoritative_count = diagnostic.market_quote_count
                blockers = list(coverage.blockers)
                if promotion_error and qualified_by_family.get(family, 0) > 0:
                    blockers.append(f"current promotion probe failed: {promotion_error}")
                statuses.append(MechanismOperatingStatus(
                    mechanism_id=coverage.mechanism_id,
                    name=coverage.name,
                    state=state,
                    stage=coverage.stage,
                    provider_ready=provider_ready,
                    authoritative_observation_count=authoritative_count,
                    forward_signal_count=signal_counts.get(family, 0),
                    independent_forward_outcome_count=int(forward.get("count") or 0),
                    current_candidate_count=candidates_by_family.get(family, 0),
                    current_statistically_qualified_count=qualified_by_family.get(family, 0),
                    current_promoted_count=promoted_by_family.get(family, 0),
                    settled_allocator_outcome_count=int((allocator or {}).get("count") or 0),
                    mean_forward_net_return=forward.get("mean"),
                    mean_forward_net_return_ci_lower=forward.get("mean_lower"),
                    forward_hit_rate=forward.get("hit_rate"),
                    forward_hit_rate_ci_lower=forward.get("hit_lower"),
                    allocator_realized_profit_usd=(allocator or {}).get("realized_profit"),
                    allocator_mean_net_return_ci_lower=(allocator or {}).get("mean_lower"),
                    allocator_profitable_rate_ci_lower=(allocator or {}).get("hit_lower"),
                    profitability_certified=certified,
                    primary_reason=reason,
                    next_action=next_action,
                    blockers=blockers,
                ))
                continue

            if coverage.mechanism_id == "yield":
                auth = int(yield_summary.get("authoritative_count", 0))
                best = max((row.conservative_net_apy for row in yield_candidates), default=None)
                state, reason, next_action = self._research_state(
                    coverage,
                    authoritative_count=auth,
                    economic_candidate_count=len(yield_candidates),
                    best_economics=best,
                    next_evidence_action="accumulate realized-yield, exit-liquidity, and protocol-loss forward outcomes",
                )
                statuses.append(self._base_status(
                    coverage,
                    state=state,
                    provider_ready=auth > 0,
                    reason=reason,
                    next_action=next_action,
                    authoritative_count=auth,
                    economic_candidate_count=len(yield_candidates),
                ))
                continue

            if coverage.mechanism_id == "volatility":
                auth = int(volatility_summary.get("authoritative_count", 0))
                state, reason, next_action = self._research_state(
                    coverage,
                    authoritative_count=auth,
                    economic_candidate_count=0,
                    best_economics=None,
                    next_evidence_action="collect executable option L2 and forward delta-hedge outcomes before statistical promotion",
                )
                statuses.append(self._base_status(
                    coverage,
                    state=state,
                    provider_ready=auth > 0,
                    reason=reason,
                    next_action=next_action,
                    authoritative_count=auth,
                ))
                continue

            if coverage.mechanism_id == "liquidation_distress":
                auth = int(distress_summary.get("authoritative_count", 0))
                best = max((row.conservative_return_on_capacity for row in distress_candidates), default=None)
                state, reason, next_action = self._research_state(
                    coverage,
                    authoritative_count=auth,
                    economic_candidate_count=len(distress_candidates),
                    best_economics=best,
                    next_evidence_action="accumulate independent capture, selection, settlement, and recovery outcomes",
                )
                statuses.append(self._base_status(
                    coverage,
                    state=state,
                    provider_ready=auth > 0,
                    reason=reason,
                    next_action=next_action,
                    authoritative_count=auth,
                    economic_candidate_count=len(distress_candidates),
                ))
                continue

            if coverage.mechanism_id == "liquidity_provision":
                reason = (
                    "public L2 exists, but empirical maker queue/fill/adverse-selection outcomes are still missing"
                    if l2_provider_ready
                    else "no usable public L2 is currently available for maker research"
                )
                statuses.append(self._base_status(
                    coverage,
                    state="collecting" if l2_provider_ready else "provider_gap",
                    provider_ready=l2_provider_ready,
                    reason=reason,
                    next_action="collect empirical maker queue, fill, post-fill adverse-selection, and inventory outcomes without assuming fills",
                    authoritative_count=book_ok,
                ))
                continue

            if coverage.mechanism_id == "capital_location_settlement":
                reason = (
                    "positive opportunity history exists and capital-location recommendations can be forward evaluated"
                    if location_plan.historical_opportunity_count > 0
                    else "insufficient persisted positive opportunity history for forward capital-location evaluation"
                )
                statuses.append(self._base_status(
                    coverage,
                    state="collecting",
                    provider_ready=True,
                    reason=reason,
                    next_action="compare recommended reserve locations with future opportunity incidence and transfer-cost/latency evidence",
                    authoritative_count=location_plan.historical_opportunity_count,
                    economic_candidate_count=len(location_plan.recommendations),
                ))
                continue

            provider_ready = (
                diagnostic.market_quote_count >= 2
                if coverage.mechanism_id == "price_discrepancy"
                else funding_provider_ready and diagnostic.market_quote_count > 0
                if coverage.mechanism_id == "carry"
                else coverage.authoritative_data_available
            )
            if not provider_ready:
                state: OperatingState = "provider_gap"
                reason = "required public market evidence is not currently available"
                next_action = "restore the required public market/funding provider surface"
            elif coverage.paper_allocation_available and not coverage.profitability_certification_available:
                state = "settlement_blocked"
                reason = "paper allocation is available but allocator-level realized multi-leg settlement is incomplete"
                next_action = "implement and accumulate exact multi-leg realized settlement without weakening economics"
            else:
                state = "collecting"
                reason = "evidence is operating but profitability certification is not yet complete"
                next_action = "continue forward evidence accumulation"
            statuses.append(self._base_status(
                coverage,
                state=state,
                provider_ready=provider_ready,
                reason=reason,
                next_action=next_action,
                authoritative_count=diagnostic.market_quote_count + diagnostic.funding_quote_count,
            ))

        blocked_states = {"statistical_failure", "execution_blocked", "settlement_blocked"}
        snapshot = OperatingCertificationSnapshot(
            observed_at=live_snapshot.completed_at,
            version=self.version,
            public_market_provider_healthy=diagnostic.healthy,
            public_market_surface_count=len(diagnostic.surfaces),
            public_market_surface_ok_count=surface_ok,
            public_order_book_probe_count=len(diagnostic.order_books),
            public_order_book_probe_ok_count=book_ok,
            market_quote_count=diagnostic.market_quote_count,
            funding_quote_count=diagnostic.funding_quote_count,
            mechanism_count=len(statuses),
            provider_gap_count=sum(row.state == "provider_gap" for row in statuses),
            collecting_count=sum(row.state == "collecting" for row in statuses),
            poor_economics_count=sum(row.state == "poor_economics" for row in statuses),
            blocked_count=sum(row.state in blocked_states for row in statuses),
            certifying_count=sum(row.state == "certifying" for row in statuses),
            certified_count=sum(row.state == "certified" for row in statuses),
            all_mechanisms_decision_grade=all(item.decision_grade for item in coverage_summary.mechanisms),
            failure_conclusion_ready=coverage_summary.failure_conclusion_ready,
            mechanisms=statuses,
        )
        self.ledger.record(snapshot)
        return OperatingCertificationCycle(
            observed_at=snapshot.observed_at,
            snapshot_id=snapshot.snapshot_id,
            mechanism_count=snapshot.mechanism_count,
            certified_count=snapshot.certified_count,
            provider_gap_count=snapshot.provider_gap_count,
            poor_economics_count=snapshot.poor_economics_count,
        )
