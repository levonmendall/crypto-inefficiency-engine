from __future__ import annotations

from fastapi import APIRouter, HTTPException

from inefficiency_engine.cex_dex_composite_statistics import CompositeEdgeStatisticalService
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_operations import (
    CexDexOperationalQualificationService,
    PaperInventoryPolicy,
)
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.cex_dex_shadow import CexDexCompositeEdgeLedger
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.stablecoin_depth_service import StablecoinConversionDepthService
from inefficiency_engine.stablecoin_depth_shadow import (
    StablecoinDepthLedger,
    StablecoinDepthShadowService,
    StablecoinDepthStatisticalService,
)
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService


def build_advanced_router(
    *,
    settings: Settings,
    evidence_store: EvidenceStore | None,
    service: OpportunityService,
    composite_service: CexDexCompositeEvidenceService,
    conversion_depth_service: StablecoinConversionDepthService,
) -> APIRouter:
    router = APIRouter()

    composite_ledger = CexDexCompositeEdgeLedger(evidence_store) if evidence_store is not None else None
    composite_statistics = (
        CompositeEdgeStatisticalService(composite_ledger, settings)
        if composite_ledger is not None
        else None
    )
    stablecoin_ledger = StablecoinDepthLedger(evidence_store) if evidence_store is not None else None
    stablecoin_statistics = (
        StablecoinDepthStatisticalService(stablecoin_ledger, settings)
        if stablecoin_ledger is not None
        else None
    )
    promotion = (
        CexDexPaperPromotionService(service, composite_service, evidence_store)
        if evidence_store is not None
        else None
    )
    unified = UnifiedPaperAllocatorService(service, promotion) if promotion is not None else None

    def require_store() -> None:
        if evidence_store is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")

    @router.get("/v1/cex-dex/composite-shadow/summary")
    def cex_dex_composite_shadow_summary():
        require_store()
        assert composite_ledger is not None
        return composite_ledger.summary()

    @router.get("/v1/cex-dex/composite-statistical/live")
    async def cex_dex_composite_statistical_live():
        require_store()
        assert composite_statistics is not None
        try:
            probe = await composite_service.probe()
            models = [composite_statistics.model(row) for row in probe.evidence]
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"CEX DEX composite statistical qualification failed: {type(exc).__name__}",
            ) from exc
        models.sort(key=lambda item: item.statistically_qualified, reverse=True)
        return {
            "paper_only": True,
            "allocation_authority": False,
            "count": len(models),
            "qualified_count": sum(item.statistically_qualified for item in models),
            "models": [item.model_dump(mode="json") for item in models],
        }

    @router.post("/v1/stablecoins/depth-shadow/cycle")
    async def stablecoin_depth_shadow_cycle():
        require_store()
        try:
            shadow = StablecoinDepthShadowService(
                conversion_depth_service,
                evidence_store=evidence_store,
            )
            cycle = await shadow.run_cycle()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"stablecoin depth shadow cycle failed: {type(exc).__name__}",
            ) from exc
        return cycle.model_dump(mode="json")

    @router.get("/v1/stablecoins/depth-shadow/summary")
    def stablecoin_depth_shadow_summary():
        require_store()
        assert stablecoin_ledger is not None
        return stablecoin_ledger.summary()

    @router.get("/v1/stablecoins/depth-statistical-model")
    def stablecoin_depth_statistical_model(source: str, target: str, amount: float = 1000.0):
        require_store()
        if amount <= 0:
            raise HTTPException(status_code=400, detail="amount must be positive")
        assert stablecoin_statistics is not None
        try:
            model = stablecoin_statistics.model(source, target, amount)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"stablecoin depth statistical model failed: {type(exc).__name__}",
            ) from exc
        return model.model_dump(mode="json")

    @router.get("/v1/cex-dex/operational/live")
    async def cex_dex_operational_live(paper_inventory_usd_per_side: float = 0.0):
        if paper_inventory_usd_per_side < 0:
            raise HTTPException(status_code=400, detail="paper_inventory_usd_per_side cannot be negative")
        inventory = PaperInventoryPolicy(
            cex_asset_inventory_usd_per_venue=paper_inventory_usd_per_side,
            cex_quote_inventory_usd_per_venue=paper_inventory_usd_per_side,
            dex_asset_inventory_usd=paper_inventory_usd_per_side,
            dex_quote_inventory_usd=paper_inventory_usd_per_side,
            source="api-paper-budget",
        )
        operational = CexDexOperationalQualificationService(
            service,
            composite_service,
            inventory_policy=inventory,
        )
        try:
            probe = await operational.live_qualification()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"CEX DEX paper operational qualification failed: {type(exc).__name__}",
            ) from exc
        return probe.model_dump(mode="json")

    @router.get("/v1/cex-dex/paper-qualification/live")
    async def cex_dex_paper_qualification_live(capital_usd: float = 100000.0):
        require_store()
        if capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        assert promotion is not None
        try:
            probe = await promotion.live_qualification(
                paper_inventory_usd_per_side=capital_usd / 2.0
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"CEX DEX final paper qualification failed: {type(exc).__name__}",
            ) from exc
        return probe.model_dump(mode="json")

    @router.get("/v1/cex-dex/allocation/live")
    async def cex_dex_paper_allocation(
        capital_usd: float = 100000.0,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ):
        require_store()
        if capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        assert promotion is not None
        try:
            plan = await promotion.paper_allocation(
                total_capital_usd=capital_usd,
                max_venue_fraction=max_venue_fraction,
                max_asset_fraction=max_asset_fraction,
                max_allocations=max_allocations,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"CEX DEX paper allocation failed: {type(exc).__name__}",
            ) from exc
        return plan.model_dump(mode="json")

    @router.get("/v1/allocation/unified/candidates/live")
    async def unified_paper_candidates(capital_usd: float = 100000.0):
        require_store()
        if capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        assert unified is not None
        try:
            rows = await unified.candidates(total_capital_usd=capital_usd)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"unified paper candidate ranking failed: {type(exc).__name__}",
            ) from exc
        return {
            "paper_only": True,
            "execution_authority": False,
            "rank_basis": "conservative_expected_return_on_reserved_capital_per_current_deployment",
            "count": len(rows),
            "candidates": [item.model_dump(mode="json") for item in rows],
        }

    @router.get("/v1/allocation/unified/live")
    async def unified_paper_allocation(
        capital_usd: float = 100000.0,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ):
        require_store()
        if capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        assert unified is not None
        try:
            plan = await unified.allocate(
                total_capital_usd=capital_usd,
                max_venue_fraction=max_venue_fraction,
                max_asset_fraction=max_asset_fraction,
                max_allocations=max_allocations,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"unified paper allocation failed: {type(exc).__name__}",
            ) from exc
        return plan.model_dump(mode="json")

    return router
