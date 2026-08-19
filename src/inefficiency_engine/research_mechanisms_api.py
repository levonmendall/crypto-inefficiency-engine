from __future__ import annotations

from fastapi import APIRouter, HTTPException

from inefficiency_engine import __version__
from inefficiency_engine.alpha_coverage_strategies import EventLedger
from inefficiency_engine.canonical_paper_portfolio_api import build_canonical_paper_portfolio_router
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification_api import build_operating_certification_router
from inefficiency_engine.research_mechanisms import (
    CapitalLocationResearchService,
    DistressResearchService,
    MarketMakingResearchService,
    VolatilityResearchService,
    YieldResearchService,
)
from inefficiency_engine.service import OpportunityService


def build_research_mechanisms_router(*, evidence_store: EvidenceStore | None, service: OpportunityService) -> APIRouter:
    router = APIRouter()
    yield_service = YieldResearchService(evidence_store) if evidence_store is not None else None
    volatility_service = VolatilityResearchService(evidence_store) if evidence_store is not None else None
    distress_service = DistressResearchService(evidence_store) if evidence_store is not None else None
    location_service = CapitalLocationResearchService(evidence_store) if evidence_store is not None else None
    event_ledger = EventLedger(evidence_store) if evidence_store is not None else None

    def require_store() -> None:
        if evidence_store is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")

    @router.get("/v3/research/events/summary")
    def event_summary():
        require_store()
        assert event_ledger is not None
        return event_ledger.summary()

    @router.get("/v3/research/yield/summary")
    def yield_summary():
        require_store()
        assert yield_service is not None
        return yield_service.summary()

    @router.get("/v3/research/yield/candidates")
    def yield_candidates():
        require_store()
        assert yield_service is not None
        rows = yield_service.candidates()
        return {
            "paper_only": True,
            "allocation_authority": False,
            "count": len(rows),
            "candidates": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/research/options/summary")
    def option_summary():
        require_store()
        assert volatility_service is not None
        return volatility_service.summary()

    @router.get("/v3/research/options/candidates")
    def option_candidates(underlying: str, realized_volatility: float, hedge_cost_bps: float = 0.0):
        require_store()
        if realized_volatility <= 0:
            raise HTTPException(status_code=400, detail="realized_volatility must be positive")
        if hedge_cost_bps < 0:
            raise HTTPException(status_code=400, detail="hedge_cost_bps cannot be negative")
        assert volatility_service is not None
        rows = volatility_service.candidates(
            realized_volatility_by_underlying={underlying.upper(): realized_volatility},
            hedge_cost_bps=hedge_cost_bps,
        )
        return {
            "paper_only": True,
            "allocation_authority": False,
            "count": len(rows),
            "candidates": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/research/distress/summary")
    def distress_summary():
        require_store()
        assert distress_service is not None
        return distress_service.summary()

    @router.get("/v3/research/distress/candidates")
    def distress_candidates():
        require_store()
        assert distress_service is not None
        rows = distress_service.candidates()
        return {
            "paper_only": True,
            "allocation_authority": False,
            "count": len(rows),
            "candidates": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/research/capital-location/plan")
    def capital_location_plan(reserve_capital_usd: float = 100000.0, max_location_fraction: float = 0.35):
        require_store()
        if reserve_capital_usd <= 0:
            raise HTTPException(status_code=400, detail="reserve_capital_usd must be positive")
        if not 0 < max_location_fraction <= 1:
            raise HTTPException(status_code=400, detail="max_location_fraction must be in (0, 1]")
        assert location_service is not None
        return location_service.plan(
            reserve_capital_usd=reserve_capital_usd,
            max_location_fraction=max_location_fraction,
        ).model_dump(mode="json")

    @router.get("/v3/research/market-making/live")
    async def market_making_live():
        require_store()
        try:
            snapshot = await service.collect_live_executability()
            latency = service.empirical_latency_model()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"market-making research scan failed: {type(exc).__name__}") from exc
        rows = [
            MarketMakingResearchService.simulate(
                book,
                empirical_fill_probability=latency.maker_fill_probability,
                adverse_selection_bps=latency.adverse_selection_p50_bps,
                queue_model_empirical=latency.queue_position_supported,
            )
            for book in snapshot.order_books
        ]
        return {
            "paper_only": True,
            "allocation_authority": False,
            "execution_authority": False,
            "count": len(rows),
            "simulations": [row.model_dump(mode="json") for row in rows],
        }

    router.include_router(build_operating_certification_router(
        version=__version__,
        evidence_store=evidence_store,
        service=service,
    ))
    router.include_router(build_canonical_paper_portfolio_router(
        evidence_store=evidence_store,
        service=service,
    ))
    return router
