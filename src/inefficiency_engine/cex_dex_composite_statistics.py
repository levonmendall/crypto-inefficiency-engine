from __future__ import annotations

from statistics import NormalDist

from pydantic import BaseModel, Field
from sqlalchemy import select

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.cex_dex_shadow import (
    CexDexCompositeEdgeLedger,
    CexDexCompositeEdgeObservation,
    CexDexCompositeEdgeShadowCycle,
    composite_edge_key,
)
from inefficiency_engine.config import Settings


class ProbabilityEstimate(BaseModel):
    successes: int = Field(ge=0)
    sample_count: int = Field(ge=0)
    probability: float | None = Field(default=None, ge=0, le=1)
    ci_lower: float | None = Field(default=None, ge=0, le=1)
    ci_upper: float | None = Field(default=None, ge=0, le=1)
    ci_width: float | None = Field(default=None, ge=0, le=1)


class CompositeEdgeStatisticalQualification(BaseModel):
    composite_key: str
    evidence_id: str
    asset: str
    route_direction: str
    target_notional_usd: float = Field(gt=0)
    cex_venue: str
    cex_symbol: str
    reference_horizon_seconds: float = Field(ge=0)
    effective_sample_count: int = Field(ge=0)
    adverse_tail_sample_count: int = Field(ge=0)
    retained_edge_sample_count: int = Field(ge=0)
    hurdle_survival: ProbabilityEstimate
    p95_adverse_deterioration_bps: float | None = None
    p10_retained_edge_fraction: float | None = None
    statistically_qualified: bool
    reasons: list[str] = Field(default_factory=list)
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
    position = (len(ordered) - 1) * min(1.0, max(0.0, q))
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
    denominator = 1.0 + z2 / n
    center = (phat + z2 / (2.0 * n)) / denominator
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


def _cycles(ledger: CexDexCompositeEdgeLedger) -> list[CexDexCompositeEdgeShadowCycle]:
    with ledger.engine.connect() as db:
        payloads = list(db.execute(select(ledger.cycles.c.payload_json).order_by(ledger.cycles.c.completed_at)).scalars())
    return [CexDexCompositeEdgeShadowCycle.model_validate_json(payload) for payload in payloads]


def build_composite_edge_statistical_qualification(
    cycles: list[CexDexCompositeEdgeShadowCycle],
    evidence: CexDexCompositeEvidence,
    settings: Settings,
) -> CompositeEdgeStatisticalQualification:
    key = composite_edge_key(evidence)
    horizon = float(getattr(settings, "cex_dex_composite_statistical_reference_horizon_seconds", settings.dex_statistical_reference_horizon_seconds))
    confidence = float(getattr(settings, "cex_dex_composite_statistical_confidence_level", settings.dex_statistical_confidence_level))
    min_samples = int(getattr(settings, "cex_dex_composite_statistical_min_effective_samples", settings.dex_statistical_min_effective_samples))
    min_tail = int(getattr(settings, "cex_dex_composite_statistical_min_tail_samples", settings.dex_statistical_min_tail_samples))
    min_survival = float(getattr(settings, "cex_dex_composite_statistical_min_survival_lower_bound", settings.dex_statistical_min_survival_lower_bound))
    max_ci_width = float(getattr(settings, "cex_dex_composite_statistical_max_ci_width", settings.dex_statistical_max_ci_width))
    max_p95 = float(getattr(settings, "cex_dex_composite_statistical_max_p95_deterioration_bps", settings.dex_statistical_max_p95_deterioration_bps))
    min_p10_retained = float(getattr(settings, "cex_dex_composite_statistical_min_p10_retained_edge_fraction", 0.50))

    rows: list[CexDexCompositeEdgeObservation] = []
    for cycle in cycles:
        match = next(
            (
                row for row in cycle.observations
                if row.composite_key == key
                and row.initial_above_hurdle
                and abs(row.horizon_seconds - horizon) <= 1e-9
            ),
            None,
        )
        if match is not None:
            rows.append(match)

    hurdle_survival = _wilson(sum(row.hurdle_survived for row in rows), len(rows), confidence)
    adverse = [
        row.adverse_deterioration_bps
        for row in rows
        if row.survived and row.adverse_deterioration_bps is not None
    ]
    retained = [
        row.retained_edge_fraction
        for row in rows
        if row.hurdle_survived and row.retained_edge_fraction is not None
    ]
    p95_adverse = _quantile(adverse, 0.95)
    p10_retained = _quantile(retained, 0.10)

    reasons: list[str] = []
    if len(rows) < min_samples:
        reasons.append(f"effective composite-edge samples {len(rows)} < {min_samples}")
    if len(adverse) < min_tail:
        reasons.append(f"composite adverse tail samples {len(adverse)} < {min_tail}")
    if hurdle_survival.ci_lower is None or hurdle_survival.ci_lower < min_survival:
        reasons.append("composite edge survival Wilson lower bound below configured minimum")
    if hurdle_survival.ci_width is None or hurdle_survival.ci_width > max_ci_width:
        reasons.append("composite edge survival confidence interval too wide")
    if p95_adverse is None or p95_adverse > max_p95:
        reasons.append("p95 composite net-edge deterioration exceeds configured ceiling or is unavailable")
    if p10_retained is None or p10_retained < min_p10_retained:
        reasons.append("p10 retained net-edge fraction is below configured minimum or unavailable")

    return CompositeEdgeStatisticalQualification(
        composite_key=key,
        evidence_id=evidence.evidence_id,
        asset=evidence.asset.upper(),
        route_direction=evidence.route_direction,
        target_notional_usd=evidence.target_notional_usd,
        cex_venue=evidence.cex_venue,
        cex_symbol=evidence.cex_symbol,
        reference_horizon_seconds=horizon,
        effective_sample_count=len(rows),
        adverse_tail_sample_count=len(adverse),
        retained_edge_sample_count=len(retained),
        hurdle_survival=hurdle_survival,
        p95_adverse_deterioration_bps=p95_adverse,
        p10_retained_edge_fraction=p10_retained,
        statistically_qualified=not reasons,
        reasons=reasons,
    )


class CompositeEdgeStatisticalService:
    def __init__(self, ledger: CexDexCompositeEdgeLedger, settings: Settings):
        self.ledger = ledger
        self.settings = settings

    def model(self, evidence: CexDexCompositeEvidence) -> CompositeEdgeStatisticalQualification:
        return build_composite_edge_statistical_qualification(_cycles(self.ledger), evidence, self.settings)
