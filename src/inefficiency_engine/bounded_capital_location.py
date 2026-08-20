from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from inefficiency_engine.models import Opportunity
from inefficiency_engine.research_mechanisms import (
    CapitalLocationPlan,
    CapitalLocationResearchService,
    CapitalLocationScore,
)


class MemoryBoundedCapitalLocationResearchService(CapitalLocationResearchService):
    """Build capital-location research from a bounded recent opportunity window.

    The original research implementation materialized every opportunity ever stored.
    That is inappropriate inside the long-running production research worker because
    the append-only evidence history is intentionally unbounded. This implementation
    keeps the same research-only scoring rule while bounding both time and row count.
    Invalid legacy payloads are skipped rather than aborting unrelated diagnostics.
    """

    def __init__(
        self,
        store,
        *,
        history_hours: float = 72.0,
        max_history_records: int = 5_000,
    ):
        super().__init__(store)
        self.history_hours = max(1.0, float(history_hours))
        self.max_history_records = max(100, int(max_history_records))

    def plan(
        self,
        *,
        reserve_capital_usd: float,
        max_location_fraction: float = 0.35,
        now: datetime | None = None,
    ) -> CapitalLocationPlan:
        if reserve_capital_usd <= 0:
            raise ValueError("reserve_capital_usd must be positive")
        current = now or datetime.now(timezone.utc)
        cutoff = current - timedelta(hours=self.history_hours)
        query = (
            select(self.store.opportunities.c.payload_json)
            .where(self.store.opportunities.c.observed_at >= cutoff.isoformat())
            .where(self.store.opportunities.c.observed_at <= current.isoformat())
            .order_by(self.store.opportunities.c.id.desc())
            .limit(self.max_history_records)
        )
        with self.store.engine.connect() as db:
            payloads = list(db.execute(query).scalars())

        opportunities: list[Opportunity] = []
        invalid_payload_count = 0
        for payload in reversed(payloads):
            try:
                opportunities.append(Opportunity.model_validate_json(payload))
            except Exception:
                invalid_payload_count += 1

        positive = [item for item in opportunities if item.net_annualized_return > 0]
        by_location: dict[tuple[str, str], list[float]] = defaultdict(list)
        for opportunity in positive:
            for leg in opportunity.legs:
                by_location[(leg.venue, opportunity.asset.upper())].append(
                    opportunity.net_annualized_return
                )

        provenance = [
            f"capital-location research uses at most the latest {self.max_history_records} opportunities within {self.history_hours:g} hours"
        ]
        if invalid_payload_count:
            provenance.append(
                f"{invalid_payload_count} incompatible legacy opportunity payloads were excluded from this research-only cohort"
            )

        if not by_location:
            return CapitalLocationPlan(
                observed_at=current,
                reserve_capital_usd=reserve_capital_usd,
                historical_opportunity_count=len(positive),
                recommendations=[],
                blockers=[
                    "no positive compatible persisted opportunity history is available for location learning",
                    *provenance,
                ],
            )

        raw: dict[tuple[str, str], float] = {}
        for key, values in by_location.items():
            mean = statistics.fmean(values)
            raw[key] = max(0.0, len(values) * math.log1p(max(0.0, mean)))
        total_score = sum(raw.values())
        if total_score <= 0:
            return CapitalLocationPlan(
                observed_at=current,
                reserve_capital_usd=reserve_capital_usd,
                historical_opportunity_count=len(positive),
                recommendations=[],
                blockers=[
                    "persisted opportunity history has no positive capital-location score",
                    *provenance,
                ],
            )

        preliminary = {
            key: min(max_location_fraction, value / total_score)
            for key, value in raw.items()
        }
        normalization = sum(preliminary.values()) or 1.0
        recommendations: list[CapitalLocationScore] = []
        for key, score in raw.items():
            values = by_location[key]
            weight = preliminary[key] / normalization
            recommendations.append(
                CapitalLocationScore(
                    venue=key[0],
                    asset=key[1],
                    opportunity_count=len(values),
                    mean_positive_net_annualized_return=statistics.fmean(values),
                    max_positive_net_annualized_return=max(values),
                    raw_score=score,
                    recommended_weight=weight,
                    recommended_reserve_usd=reserve_capital_usd * weight,
                )
            )
        recommendations.sort(key=lambda item: item.recommended_weight, reverse=True)
        return CapitalLocationPlan(
            observed_at=current,
            reserve_capital_usd=reserve_capital_usd,
            historical_opportunity_count=len(positive),
            recommendations=recommendations,
            blockers=[
                "recommendation is learned from a bounded point-in-time opportunity cohort and is not yet forward-certified",
                "rebalancing costs and transfer/withdrawal latency require venue-specific authoritative evidence before allocation authority",
                *provenance,
            ],
        )
