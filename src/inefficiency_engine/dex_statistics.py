from __future__ import annotations

from collections import defaultdict
from statistics import NormalDist
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import select

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier
from inefficiency_engine.dex_shadow import DexRouteShadowCycle, DexRouteShadowObservation
from inefficiency_engine.evidence import EvidenceStore


class ProbabilityEstimate(BaseModel):
    successes: int = Field(ge=0)
    sample_count: int = Field(ge=0)
    probability: float | None = Field(default=None, ge=0, le=1)
    ci_lower: float | None = Field(default=None, ge=0, le=1)
    ci_upper: float | None = Field(default=None, ge=0, le=1)
    ci_width: float | None = Field(default=None, ge=0, le=1)


class DexStatisticalQualification(BaseModel):
    asset: str
    direction: Literal["buy_asset", "sell_asset"]
    target_notional_usd: float = Field(gt=0)
    reference_horizon_seconds: float = Field(ge=0)
    notional_tolerance_fraction: float = Field(ge=0)
    confidence_level: float = Field(gt=0.5, lt=1)
    shadow_effective_sample_count: int = Field(ge=0)
    frontier_effective_sample_count: int = Field(ge=0)
    adverse_tail_sample_count: int = Field(ge=0)
    survival: ProbabilityEstimate
    frontier_acceptance: ProbabilityEstimate
    p95_adverse_deterioration_bps: float | None = None
    route_change_rate: float | None = Field(default=None, ge=0, le=1)
    statistically_qualified: bool
    reasons: list[str] = Field(default_factory=list)
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexResearchQualification(BaseModel):
    evidence_id: str
    asset: str
    route_direction: Literal["buy_asset", "sell_asset"]
    target_notional_usd: float = Field(gt=0)
    cex_venue: str
    net_research_edge_bps: float
    minimum_net_edge_bps: float
    current_evidence_complete: bool
    route_contiguous_acceptable: bool
    statistical_model: DexStatisticalQualification
    research_qualified: bool
    remaining_blockers: list[str] = Field(default_factory=list)
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexQualificationProbe(BaseModel):
    evidence_count: int = Field(ge=0)
    research_qualified_count: int = Field(ge=0)
    qualifications: list[CexDexResearchQualification] = Field(default_factory=list)
    capacity_claimed: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    q = min(1.0, max(0.0, q))
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _wilson(successes: int, n: int, confidence: float) -> ProbabilityEstimate:
    if n <= 0:
        return ProbabilityEstimate(successes=0, sample_count=0)
    confidence = min(0.999999, max(0.500001, confidence))
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    phat = successes / n
    z2 = z * z
    denominator = 1.0 + (z2 / n)
    center = (phat + (z2 / (2.0 * n))) / denominator
    margin = z * ((phat * (1.0 - phat) / n + z2 / (4.0 * n * n)) ** 0.5) / denominator
    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)
    return ProbabilityEstimate(
        successes=successes,
        sample_count=n,
        probability=phat,
        ci_lower=lower,
        ci_upper=upper,
        ci_width=upper - lower,
    )


def _same_horizon(observed: float, requested: float) -> bool:
    return abs(observed - requested) <= max(1e-9, abs(requested) * 1e-9)


def _same_notional(observed: float, requested: float, tolerance_fraction: float) -> bool:
    tolerance = max(1.0, abs(requested) * max(0.0, tolerance_fraction))
    return abs(observed - requested) <= tolerance


def _shadow_clusters(
    cycles: list[DexRouteShadowCycle],
    *,
    asset: str,
    direction: str,
    target_notional_usd: float,
    horizon_seconds: float,
    tolerance_fraction: float,
) -> dict[str, list[DexRouteShadowObservation]]:
    grouped: dict[str, list[DexRouteShadowObservation]] = defaultdict(list)
    for cycle in cycles:
        for row in cycle.observations:
            if row.asset.upper() != asset.upper() or row.direction != direction:
                continue
            if not _same_horizon(row.delay_seconds, horizon_seconds):
                continue
            if not _same_notional(row.quote_notional_usd_proxy, target_notional_usd, tolerance_fraction):
                continue
            grouped[cycle.cycle_id].append(row)
    return grouped


def _frontier_outcomes(
    frontiers: list[DexRouteSizeFrontier],
    *,
    asset: str,
    direction: str,
    target_notional_usd: float,
) -> list[bool]:
    outcomes: list[bool] = []
    for frontier in frontiers:
        if frontier.asset.upper() != asset.upper() or frontier.direction != direction:
            continue
        point = next(
            (
                item
                for item in frontier.points
                if abs(item.target_notional_usd - target_notional_usd)
                <= max(1e-6, abs(target_notional_usd) * 1e-9)
            ),
            None,
        )
        if point is None:
            continue
        outcomes.append(bool(point.quoted and point.contiguous_acceptable))
    return outcomes


def build_dex_statistical_qualification(
    cycles: list[DexRouteShadowCycle],
    frontiers: list[DexRouteSizeFrontier],
    *,
    asset: str,
    direction: Literal["buy_asset", "sell_asset"],
    target_notional_usd: float,
    settings: Settings,
) -> DexStatisticalQualification:
    if target_notional_usd <= 0:
        raise ValueError("target_notional_usd must be positive")
    horizon = settings.dex_statistical_reference_horizon_seconds
    tolerance = settings.dex_statistical_notional_tolerance_fraction
    confidence = settings.dex_statistical_confidence_level
    grouped = _shadow_clusters(
        cycles,
        asset=asset,
        direction=direction,
        target_notional_usd=target_notional_usd,
        horizon_seconds=horizon,
        tolerance_fraction=tolerance,
    )

    shadow_successes = 0
    adverse: list[float] = []
    route_change_events = 0
    route_change_observations = 0
    for rows in grouped.values():
        survived = bool(rows) and all(row.survived for row in rows)
        shadow_successes += int(survived)
        if survived:
            row_adverse = [
                max(0.0, row.price_deterioration_bps)
                for row in rows
                if row.price_deterioration_bps is not None
            ]
            if row_adverse:
                adverse.append(max(row_adverse))
            changed = [row.route_changed for row in rows if row.route_changed is not None]
            if changed:
                route_change_observations += 1
                route_change_events += int(any(changed))

    survival = _wilson(shadow_successes, len(grouped), confidence)
    frontier_outcomes = _frontier_outcomes(
        frontiers,
        asset=asset,
        direction=direction,
        target_notional_usd=target_notional_usd,
    )
    frontier_acceptance = _wilson(sum(frontier_outcomes), len(frontier_outcomes), confidence)
    p95_adverse = _quantile(adverse, 0.95)
    route_change_rate = (
        route_change_events / route_change_observations if route_change_observations else None
    )

    reasons: list[str] = []
    if len(grouped) < settings.dex_statistical_min_effective_samples:
        reasons.append(
            f"shadow effective samples {len(grouped)} < {settings.dex_statistical_min_effective_samples}"
        )
    if len(frontier_outcomes) < settings.dex_statistical_min_effective_samples:
        reasons.append(
            f"frontier effective samples {len(frontier_outcomes)} < {settings.dex_statistical_min_effective_samples}"
        )
    if len(adverse) < settings.dex_statistical_min_tail_samples:
        reasons.append(
            f"adverse tail samples {len(adverse)} < {settings.dex_statistical_min_tail_samples}"
        )
    if survival.ci_lower is None or survival.ci_lower < settings.dex_statistical_min_survival_lower_bound:
        reasons.append(
            "shadow survival Wilson lower bound below configured minimum"
        )
    if (
        frontier_acceptance.ci_lower is None
        or frontier_acceptance.ci_lower < settings.dex_statistical_min_frontier_acceptance_lower_bound
    ):
        reasons.append(
            "frontier acceptance Wilson lower bound below configured minimum"
        )
    if survival.ci_width is None or survival.ci_width > settings.dex_statistical_max_ci_width:
        reasons.append("shadow survival confidence interval too wide")
    if (
        frontier_acceptance.ci_width is None
        or frontier_acceptance.ci_width > settings.dex_statistical_max_ci_width
    ):
        reasons.append("frontier acceptance confidence interval too wide")
    if p95_adverse is None or p95_adverse > settings.dex_statistical_max_p95_deterioration_bps:
        reasons.append("p95 adverse route deterioration exceeds configured ceiling or is unavailable")

    return DexStatisticalQualification(
        asset=asset.upper(),
        direction=direction,
        target_notional_usd=target_notional_usd,
        reference_horizon_seconds=horizon,
        notional_tolerance_fraction=tolerance,
        confidence_level=confidence,
        shadow_effective_sample_count=len(grouped),
        frontier_effective_sample_count=len(frontier_outcomes),
        adverse_tail_sample_count=len(adverse),
        survival=survival,
        frontier_acceptance=frontier_acceptance,
        p95_adverse_deterioration_bps=p95_adverse,
        route_change_rate=route_change_rate,
        statistically_qualified=not reasons,
        reasons=reasons,
        capacity_claimed=False,
        allocation_eligible=False,
        executable_eligible=False,
        paper_only=True,
    )


def qualify_cex_dex_research_evidence(
    evidence: CexDexCompositeEvidence,
    statistical_model: DexStatisticalQualification,
    settings: Settings,
) -> CexDexResearchQualification:
    blockers: list[str] = []
    if not evidence.evidence_complete:
        blockers.append("current same-notional composite evidence is incomplete")
    if not evidence.route_contiguous_acceptable:
        blockers.append("current route tier is outside the contiguous acceptable frontier")
    if evidence.net_research_edge_bps < settings.dex_statistical_min_net_edge_bps:
        blockers.append("current net research edge is below the configured minimum")
    if not statistical_model.statistically_qualified:
        blockers.append("historical DEX route statistical evidence is not qualified")
    research_qualified = not blockers
    blockers.extend([
        "cross-venue inventory and settlement model is not qualified",
        "atomic hedge and recovery model is not qualified",
        "real-money execution remains separately blocked",
    ])
    return CexDexResearchQualification(
        evidence_id=evidence.evidence_id,
        asset=evidence.asset.upper(),
        route_direction=evidence.route_direction,
        target_notional_usd=evidence.target_notional_usd,
        cex_venue=evidence.cex_venue,
        net_research_edge_bps=evidence.net_research_edge_bps,
        minimum_net_edge_bps=settings.dex_statistical_min_net_edge_bps,
        current_evidence_complete=evidence.evidence_complete,
        route_contiguous_acceptable=evidence.route_contiguous_acceptable,
        statistical_model=statistical_model,
        research_qualified=research_qualified,
        remaining_blockers=blockers,
        capacity_claimed=False,
        allocation_eligible=False,
        executable_eligible=False,
        paper_only=True,
    )


class DexStatisticalQualificationService:
    def __init__(
        self,
        store: EvidenceStore,
        settings: Settings,
        *,
        composite_service: CexDexCompositeEvidenceService | None = None,
    ):
        self.store = store
        self.settings = settings
        self.composite_service = composite_service

    def _cycles(self) -> list[DexRouteShadowCycle]:
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.store.dex_route_shadow_cycles.c.payload_json).order_by(
                        self.store.dex_route_shadow_cycles.c.completed_at
                    )
                ).scalars()
            )
        return [DexRouteShadowCycle.model_validate_json(payload) for payload in payloads]

    def _frontiers(self) -> list[DexRouteSizeFrontier]:
        with self.store.engine.connect() as db:
            payloads = list(
                db.execute(
                    select(self.store.dex_route_size_frontiers.c.payload_json).order_by(
                        self.store.dex_route_size_frontiers.c.observed_at
                    )
                ).scalars()
            )
        return [DexRouteSizeFrontier.model_validate_json(payload) for payload in payloads]

    def model(
        self,
        *,
        asset: str,
        direction: Literal["buy_asset", "sell_asset"],
        target_notional_usd: float,
    ) -> DexStatisticalQualification:
        return build_dex_statistical_qualification(
            self._cycles(),
            self._frontiers(),
            asset=asset,
            direction=direction,
            target_notional_usd=target_notional_usd,
            settings=self.settings,
        )

    async def live_composite_qualification(self) -> CexDexQualificationProbe:
        if self.composite_service is None:
            raise RuntimeError("composite evidence service is not configured")
        probe = await self.composite_service.probe()
        cycles = self._cycles()
        frontiers = self._frontiers()
        cache: dict[tuple[str, str, float], DexStatisticalQualification] = {}
        qualifications: list[CexDexResearchQualification] = []
        for evidence in probe.evidence:
            key = (evidence.asset.upper(), evidence.route_direction, evidence.target_notional_usd)
            model = cache.get(key)
            if model is None:
                model = build_dex_statistical_qualification(
                    cycles,
                    frontiers,
                    asset=key[0],
                    direction=key[1],
                    target_notional_usd=key[2],
                    settings=self.settings,
                )
                cache[key] = model
            qualifications.append(qualify_cex_dex_research_evidence(evidence, model, self.settings))
        qualifications.sort(
            key=lambda row: (row.research_qualified, row.net_research_edge_bps),
            reverse=True,
        )
        return CexDexQualificationProbe(
            evidence_count=len(qualifications),
            research_qualified_count=sum(item.research_qualified for item in qualifications),
            qualifications=qualifications,
            capacity_claimed=False,
            allocation_eligible=False,
            executable_eligible=False,
            paper_only=True,
        )
