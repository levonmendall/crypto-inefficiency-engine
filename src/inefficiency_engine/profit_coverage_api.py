from __future__ import annotations

from fastapi import APIRouter, HTTPException

from inefficiency_engine.alpha_coverage_strategies import EventLedger
from inefficiency_engine.alpha_extensions import FundamentalFactorLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.profit_coverage import build_profit_coverage_summary, profit_coverage_gaps
from inefficiency_engine.research_mechanisms import DistressResearchService, VolatilityResearchService, YieldResearchService
from inefficiency_engine.source_coverage import LANES, SourceCoveragePlane


_ALPHA_FAMILIES = {
    "directional_time_series",
    "directional_reversal",
    "onchain_fundamental",
    "cross_sectional_relative_value",
    "microstructure_orderflow",
    "event_driven",
}


def build_profit_coverage_router(*, version: str, evidence_store: EvidenceStore | None) -> APIRouter:
    router = APIRouter()
    source_plane = SourceCoveragePlane(evidence_store) if evidence_store is not None else None

    def summary():
        factor_count = event_count = yield_count = option_count = distress_count = 0
        if evidence_store is not None:
            factor_count = int(FundamentalFactorLedger(evidence_store).summary().get("authoritative_count", 0))
            event_count = int(EventLedger(evidence_store).summary().get("authoritative_count", 0))
            yield_count = int(YieldResearchService(evidence_store).summary().get("authoritative_count", 0))
            option_count = int(VolatilityResearchService(evidence_store).summary().get("authoritative_count", 0))
            distress_count = int(DistressResearchService(evidence_store).summary().get("authoritative_count", 0))
        return build_profit_coverage_summary(
            version=version,
            alpha_families=_ALPHA_FAMILIES,
            fundamental_authoritative_observation_count=factor_count,
            event_authoritative_observation_count=event_count,
            yield_authoritative_observation_count=yield_count,
            option_authoritative_observation_count=option_count,
            distress_authoritative_observation_count=distress_count,
        )

    @router.get("/v2/profit-mechanisms/coverage")
    def profit_mechanism_coverage():
        return summary().model_dump(mode="json")

    @router.get("/v2/profit-mechanisms/gaps")
    def profit_mechanism_gaps():
        coverage = summary()
        gaps = profit_coverage_gaps(coverage)
        return {
            "paper_only": True,
            "failure_conclusion_ready": coverage.failure_conclusion_ready,
            "count": len(gaps),
            "gaps": [item.model_dump(mode="json") for item in gaps],
        }

    @router.get("/v2/source-coverage")
    def source_coverage():
        if source_plane is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")
        return source_plane.snapshot().model_dump(mode="json")

    @router.get("/v2/source-coverage/{lane_id}")
    def source_coverage_lane(lane_id: str):
        if source_plane is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")
        if lane_id not in LANES:
            raise HTTPException(status_code=404, detail="unknown profit-mechanism lane")
        return source_plane.lane(lane_id).model_dump(mode="json")

    return router
