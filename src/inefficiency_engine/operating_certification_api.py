from __future__ import annotations

from fastapi import APIRouter, HTTPException

from inefficiency_engine.allocation_certification import AllocationForwardCertificationService
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_certification import OperatingCertificationService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService


_STATE_PRIORITY = {
    "provider_gap": 0,
    "poor_economics": 1,
    "statistical_failure": 2,
    "execution_blocked": 3,
    "settlement_blocked": 4,
    "collecting": 5,
    "certifying": 6,
    "certified": 7,
}


def build_operating_certification_router(
    *,
    version: str,
    evidence_store: EvidenceStore | None,
    service: OpportunityService,
) -> APIRouter:
    router = APIRouter()
    operating: OperatingCertificationService | None = None
    if evidence_store is not None:
        universal = UniversalOpportunityService(service)
        composite = CexDexCompositeEvidenceService(service, universal=universal)
        promotion = CexDexPaperPromotionService(service, composite, evidence_store)
        alpha_factory = ExpandedAlphaFactoryService(service, evidence_store)
        unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
        allocation_certification = AllocationForwardCertificationService(service, unified, evidence_store)
        operating = OperatingCertificationService(
            service,
            evidence_store,
            alpha_factory,
            allocation_certification,
            version=version,
        )

    def require_operating() -> OperatingCertificationService:
        if operating is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")
        return operating

    @router.get("/v3/operations/certification/latest")
    def operating_certification_latest():
        engine = require_operating()
        latest = engine.ledger.latest()
        if latest is None:
            return {
                "available": False,
                "paper_only": True,
                "message": "no operating certification cycle has been recorded yet",
            }
        payload = latest.model_dump(mode="json")
        payload["available"] = True
        return payload

    @router.get("/v3/operations/certification/history")
    def operating_certification_history(limit: int = 50):
        engine = require_operating()
        rows = engine.ledger.history(limit=limit)
        return {
            "paper_only": True,
            "count": len(rows),
            "snapshots": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/operations/certification/summary")
    def operating_certification_summary():
        return require_operating().ledger.summary()

    @router.post("/v3/operations/certification/cycle")
    async def operating_certification_cycle(capital_usd: float = 100000.0):
        if capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        engine = require_operating()
        try:
            cycle = await engine.run_cycle(total_capital_usd=capital_usd)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"operating certification cycle failed: {type(exc).__name__}",
            ) from exc
        latest = engine.ledger.latest()
        return {
            "cycle": cycle.model_dump(mode="json"),
            "snapshot": latest.model_dump(mode="json") if latest is not None else None,
        }

    @router.get("/v3/operations/mechanisms")
    def operating_mechanisms():
        engine = require_operating()
        latest = engine.ledger.latest()
        if latest is None:
            return {"paper_only": True, "count": 0, "mechanisms": []}
        return {
            "paper_only": True,
            "count": len(latest.mechanisms),
            "mechanisms": [row.model_dump(mode="json") for row in latest.mechanisms],
        }

    @router.get("/v3/operations/action-queue")
    def operating_action_queue():
        engine = require_operating()
        latest = engine.ledger.latest()
        if latest is None:
            return {"paper_only": True, "count": 0, "actions": []}
        rows = sorted(
            (row for row in latest.mechanisms if row.state != "certified"),
            key=lambda row: (_STATE_PRIORITY.get(row.state, 99), row.mechanism_id),
        )
        return {
            "paper_only": True,
            "count": len(rows),
            "actions": [
                {
                    "mechanism_id": row.mechanism_id,
                    "name": row.name,
                    "state": row.state,
                    "primary_reason": row.primary_reason,
                    "next_action": row.next_action,
                    "blockers": row.blockers,
                }
                for row in rows
            ],
        }

    return router
