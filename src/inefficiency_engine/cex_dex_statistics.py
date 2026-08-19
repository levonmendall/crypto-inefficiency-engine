from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import sqrt
from statistics import NormalDist
from typing import Literal
import uuid

from pydantic import BaseModel, Field

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence


class CexDexStudyObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cycle_id: str
    group_key: str
    asset: str
    route_direction: Literal["buy_asset", "sell_asset"]
    cex_venue: str
    cex_symbol: str
    target_notional_usd: float = Field(gt=0)
    attempted_at: datetime
    evidence_complete: bool
    net_research_edge_bps: float | None = None
    evidence_id: str | None = None
    failure_type: str | None = None
    evidence: CexDexCompositeEvidence | None = None
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexStudyCycle(BaseModel):
    cycle_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime
    observations: list[CexDexStudyObservation] = Field(default_factory=list)
    capacity_claimed: bool = False
    executable_eligible: bool = False
    paper_only: bool = True


class CexDexGroupStatistics(BaseModel):
    group_key: str
    asset: str
    route_direction: str
    cex_venue: str
    cex_symbol: str
    target_notional_usd: float
    effective_sample_size: int
    complete_count: int
    positive_count: int
    complete_rate: float | None = None
    complete_ci_lower: float | None = None
    complete_ci_upper: float | None = None
    positive_rate: float | None = None
    positive_ci_lower: float | None = None
    positive_ci_upper: float | None = None
    net_edge_p10_bps: float | None = None
    net_edge_p50_bps: float | None = None
    min_effective_samples: int
    min_complete_ci_lower: float
    min_positive_ci_lower: float
    min_net_edge_p10_bps: float
    confidence_level: float
    research_qualified: bool = False
    allocation_eligible: bool = False
    executable_eligible: bool = False
    reason: str | None = None


def group_key(asset: str, route_direction: str, cex_venue: str, cex_symbol: str, target_notional_usd: float) -> str:
    return f"{asset.upper()}:{route_direction}:{cex_venue}:{cex_symbol}:{target_notional_usd:.8f}"


def wilson_interval(successes: int, total: int, confidence_level: float = 0.95) -> tuple[float, float] | None:
    if total <= 0:
        return None
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be between 0 and 1")
    z = NormalDist().inv_cdf(0.5 + confidence_level / 2.0)
    p = successes / total
    denom = 1.0 + (z * z) / total
    center = (p + (z * z) / (2.0 * total)) / denom
    margin = z * sqrt((p * (1.0 - p) / total) + (z * z) / (4.0 * total * total)) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def _lower_quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be between 0 and 1")
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(q * len(ordered)) - (1 if q > 0 else 0)))
    return ordered[index]


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def summarize_study_cycles(
    cycles: list[CexDexStudyCycle],
    *,
    min_effective_samples: int = 30,
    min_complete_ci_lower: float = 0.70,
    min_positive_ci_lower: float = 0.60,
    min_net_edge_p10_bps: float = 0.0,
    confidence_level: float = 0.95,
) -> list[CexDexGroupStatistics]:
    grouped: dict[str, list[CexDexStudyObservation]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    for cycle in cycles:
        per_group: dict[str, list[CexDexStudyObservation]] = defaultdict(list)
        for obs in cycle.observations:
            per_group[obs.group_key].append(obs)
        for key, rows in per_group.items():
            token = (cycle.cycle_id, key)
            if token in seen:
                continue
            seen.add(token)
            # Multiple rows for the same group in one cycle cannot inflate the
            # effective sample size. Aggregate them conservatively: any failure
            # makes the cycle/group incomplete; otherwise keep the worst net edge.
            complete = all(row.evidence_complete for row in rows)
            if complete:
                worst = min(rows, key=lambda row: row.net_research_edge_bps if row.net_research_edge_bps is not None else float("-inf"))
                grouped[key].append(worst)
            else:
                template = rows[0]
                grouped[key].append(template.model_copy(update={
                    "evidence_complete": False,
                    "net_research_edge_bps": None,
                    "evidence_id": None,
                    "evidence": None,
                    "failure_type": next((row.failure_type for row in rows if row.failure_type), "CycleGroupIncomplete"),
                }))

    stats: list[CexDexGroupStatistics] = []
    for key, rows in grouped.items():
        sample_count = len(rows)
        complete_rows = [row for row in rows if row.evidence_complete and row.net_research_edge_bps is not None]
        positive_rows = [row for row in complete_rows if (row.net_research_edge_bps or 0.0) > 0.0]
        complete_ci = wilson_interval(len(complete_rows), sample_count, confidence_level)
        # Positive probability is intentionally measured over all attempts so
        # missing/incomplete evidence behaves as non-positive evidence.
        positive_ci = wilson_interval(len(positive_rows), sample_count, confidence_level)
        edges = [float(row.net_research_edge_bps) for row in complete_rows if row.net_research_edge_bps is not None]
        template = rows[0]
        p10 = _lower_quantile(edges, 0.10)
        p50 = _median(edges)
        reasons: list[str] = []
        if sample_count < min_effective_samples:
            reasons.append("insufficient_effective_samples")
        if complete_ci is None or complete_ci[0] < min_complete_ci_lower:
            reasons.append("complete_rate_confidence_too_low")
        if positive_ci is None or positive_ci[0] < min_positive_ci_lower:
            reasons.append("positive_edge_confidence_too_low")
        if p10 is None or p10 <= min_net_edge_p10_bps:
            reasons.append("net_edge_lower_tail_not_positive")
        qualified = not reasons
        stats.append(CexDexGroupStatistics(
            group_key=key,
            asset=template.asset,
            route_direction=template.route_direction,
            cex_venue=template.cex_venue,
            cex_symbol=template.cex_symbol,
            target_notional_usd=template.target_notional_usd,
            effective_sample_size=sample_count,
            complete_count=len(complete_rows),
            positive_count=len(positive_rows),
            complete_rate=len(complete_rows) / sample_count if sample_count else None,
            complete_ci_lower=complete_ci[0] if complete_ci else None,
            complete_ci_upper=complete_ci[1] if complete_ci else None,
            positive_rate=len(positive_rows) / sample_count if sample_count else None,
            positive_ci_lower=positive_ci[0] if positive_ci else None,
            positive_ci_upper=positive_ci[1] if positive_ci else None,
            net_edge_p10_bps=p10,
            net_edge_p50_bps=p50,
            min_effective_samples=min_effective_samples,
            min_complete_ci_lower=min_complete_ci_lower,
            min_positive_ci_lower=min_positive_ci_lower,
            min_net_edge_p10_bps=min_net_edge_p10_bps,
            confidence_level=confidence_level,
            research_qualified=qualified,
            allocation_eligible=False,
            executable_eligible=False,
            reason=None if qualified else ",".join(reasons),
        ))
    return sorted(stats, key=lambda item: (item.research_qualified, item.net_edge_p10_bps or float("-inf")), reverse=True)
