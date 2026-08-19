from __future__ import annotations

from fastapi import APIRouter, HTTPException

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CANONICAL_PORTFOLIO_ID,
    CanonicalPaperPortfolioService,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService


def build_canonical_paper_portfolio_router(
    *,
    evidence_store: EvidenceStore | None,
    service: OpportunityService,
) -> APIRouter:
    router = APIRouter()
    portfolio: CanonicalPaperPortfolioService | None = None
    if evidence_store is not None:
        universal = UniversalOpportunityService(service)
        composite = CexDexCompositeEvidenceService(service, universal=universal)
        promotion = CexDexPaperPromotionService(service, composite, evidence_store)
        alpha_factory = ExpandedAlphaFactoryService(service, evidence_store)
        unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
        portfolio = CanonicalPaperPortfolioService(service, unified, evidence_store)

    def require_portfolio() -> CanonicalPaperPortfolioService:
        if portfolio is None:
            raise HTTPException(status_code=503, detail="evidence persistence is not configured")
        return portfolio

    @router.get("/v3/portfolio/canonical")
    def canonical_portfolio():
        engine = require_portfolio()
        latest = engine.ledger.latest_snapshot()
        if latest is None:
            return {
                "available": False,
                "portfolio_id": CANONICAL_PORTFOLIO_ID,
                "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
                "paper_only": True,
                "message": "canonical paper portfolio is awaiting its first worker cycle",
            }
        payload = latest.model_dump(mode="json")
        payload["available"] = True
        return payload

    @router.get("/v3/portfolio/performance")
    def canonical_portfolio_performance():
        return require_portfolio().performance_summary()

    @router.get("/v3/portfolio/positions")
    def canonical_portfolio_positions():
        engine = require_portfolio()
        latest = engine.ledger.latest_snapshot()
        positions = [] if latest is None else latest.positions
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "count": len(positions),
            "positions": [position.model_dump(mode="json") for position in positions],
        }

    @router.get("/v3/portfolio/trades")
    def canonical_portfolio_trades(limit: int = 100):
        engine = require_portfolio()
        rows = engine.ledger.trade_history(limit=limit)
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "count": len(rows),
            "trades": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/portfolio/history")
    def canonical_portfolio_history(limit: int = 100):
        engine = require_portfolio()
        rows = engine.ledger.snapshot_history(limit=limit)
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "count": len(rows),
            "snapshots": [row.model_dump(mode="json") for row in rows],
        }

    @router.get("/v3/portfolio/attribution")
    def canonical_portfolio_attribution():
        engine = require_portfolio()
        latest = engine.ledger.latest_snapshot()
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "pnl_by_mechanism_usd": {} if latest is None else latest.pnl_by_mechanism_usd,
            "pnl_by_strategy_usd": {} if latest is None else latest.pnl_by_strategy_usd,
        }

    @router.post("/v3/portfolio/cycle")
    async def canonical_portfolio_cycle():
        engine = require_portfolio()
        try:
            cycle = await engine.run_cycle()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"canonical paper portfolio cycle failed: {type(exc).__name__}") from exc
        latest = engine.ledger.latest_snapshot()
        return {
            "cycle": cycle.model_dump(mode="json"),
            "portfolio": latest.model_dump(mode="json") if latest is not None else None,
        }

    return router
