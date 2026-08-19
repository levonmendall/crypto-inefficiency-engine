from __future__ import annotations

from fastapi import APIRouter

from inefficiency_engine.alpha_extensions import FundamentalFactorLedger
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.profit_coverage import build_profit_coverage_summary, profit_coverage_gaps


_ALPHA_FAMILIES = {
    "directional_time_series",
    "directional_reversal",
    "onchain_fundamental",
}


def build_profit_coverage_router(*, version: str, evidence_store: EvidenceStore | None) -> APIRouter:
    router = APIRouter()

    def summary():
        authoritative_factor_count = 0
        if evidence_store is not None:
            factor_summary = FundamentalFactorLedger(evidence_store).summary()
            authoritative_factor_count = int(factor_summary.get("authoritative_count", 0))
        return build_profit_coverage_summary(
            version=version,
            alpha_families=_ALPHA_FAMILIES,
            fundamental_authoritative_observation_count=authoritative_factor_count,
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

    return router
