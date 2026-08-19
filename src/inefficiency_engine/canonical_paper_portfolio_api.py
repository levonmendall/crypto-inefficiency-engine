from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CANONICAL_PORTFOLIO_ID,
    CanonicalPortfolioEvent,
)
from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.cex_dex_promotion import CexDexPaperPromotionService
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService
from inefficiency_engine.operating_worker import PORTFOLIO_WORKER_ID
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.unified_allocation import UnifiedPaperAllocatorService
from inefficiency_engine.universal_service import UniversalOpportunityService


def build_canonical_paper_portfolio_router(
    *,
    evidence_store: EvidenceStore | None,
    service: OpportunityService,
) -> APIRouter:
    router = APIRouter()
    portfolio: OperationallyResilientPaperPortfolioService | None = None
    if evidence_store is not None:
        universal = UniversalOpportunityService(service)
        composite = CexDexCompositeEvidenceService(service, universal=universal)
        promotion = CexDexPaperPromotionService(service, composite, evidence_store)
        alpha_factory = ExpandedAlphaFactoryService(service, evidence_store)
        unified = UnifiedPaperAllocatorService(service, promotion, alpha_factory)
        portfolio = OperationallyResilientPaperPortfolioService(service, unified, evidence_store)

    def require_portfolio() -> OperationallyResilientPaperPortfolioService:
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

    @router.get("/v3/portfolio/runtime-status")
    def canonical_portfolio_runtime_status():
        engine = require_portfolio()
        now = datetime.now(timezone.utc)
        account = engine.ledger.latest_snapshot()
        integrity = engine.integrity.latest()
        heartbeat = engine.store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
        expected_interval = max(60.0, service.settings.shadow_cycle_interval_seconds * 10.0)
        stale_after = max(600.0, expected_interval * 2.5)

        account_age = (
            max(0.0, (now - account.observed_at).total_seconds()) if account is not None else None
        )
        market_age = (
            max(0.0, (now - integrity.market_evidence_at).total_seconds())
            if integrity is not None and integrity.market_evidence_at is not None else None
        )
        heartbeat_age = (
            max(0.0, (now - heartbeat.observed_at).total_seconds()) if heartbeat is not None else None
        )
        heartbeat_recent = bool(
            heartbeat is not None and heartbeat_age is not None and heartbeat_age <= stale_after
        )
        accounting_fresh = bool(
            account is not None and account_age is not None and account_age <= stale_after
        )
        valuation_status = integrity.valuation_status if integrity is not None else "unavailable"
        valuation_fresh = bool(
            integrity is not None
            and (
                valuation_status == "cash_only"
                or (
                    valuation_status == "fresh"
                    and market_age is not None
                    and market_age <= stale_after
                )
            )
        )
        cycle_failed = bool(integrity is not None and integrity.cycle_status == "failed")
        operational = bool(
            heartbeat_recent
            and heartbeat is not None
            and heartbeat.state not in {"error", "stopped"}
            and accounting_fresh
            and valuation_fresh
            and not cycle_failed
        )
        degraded = bool(
            (heartbeat is not None and heartbeat.state == "degraded")
            or (integrity is not None and integrity.cycle_status == "degraded")
            or (account is not None and not valuation_fresh)
        )

        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "operational": operational,
            "degraded": degraded,
            "expected_cycle_interval_seconds": expected_interval,
            "stale_after_seconds": stale_after,
            "snapshot_fresh": accounting_fresh,
            "accounting_snapshot_fresh": accounting_fresh,
            "snapshot_age_seconds": account_age,
            "latest_snapshot_observed_at": account.observed_at if account is not None else None,
            "valuation_status": valuation_status,
            "valuation_fresh": valuation_fresh,
            "market_evidence_observed_at": (
                integrity.market_evidence_at if integrity is not None else None
            ),
            "market_evidence_age_seconds": market_age,
            "cycle_status": integrity.cycle_status if integrity is not None else None,
            "fallback_snapshot": integrity.fallback_snapshot if integrity is not None else False,
            "cycle_error_type": integrity.cycle_error_type if integrity is not None else None,
            "stale_position_count": integrity.stale_position_count if integrity is not None else None,
            "settlement_evidence_blocked_count": (
                integrity.settlement_evidence_blocked_count if integrity is not None else 0
            ),
            "allocation_family_failures": (
                list(integrity.allocation_family_failures) if integrity is not None else []
            ),
            "market_snapshot_id": integrity.market_snapshot_id if integrity is not None else None,
            "heartbeat_age_seconds": heartbeat_age,
            "heartbeat": heartbeat.model_dump(mode="json") if heartbeat is not None else None,
        }

    @router.get("/v3/portfolio/integrity/history")
    def canonical_portfolio_integrity_history(limit: int = 100):
        engine = require_portfolio()
        rows = engine.integrity.history(limit=limit)
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "count": len(rows),
            "integrity": [row.model_dump(mode="json") for row in rows],
        }

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

    @router.get("/v3/portfolio/skips")
    def canonical_portfolio_skips(limit: int = 100):
        engine = require_portfolio()
        bounded = max(1, min(1000, int(limit)))
        table = engine.ledger.events
        with engine.store.engine.connect() as db:
            payloads = list(db.execute(
                select(table.c.payload_json)
                .where(table.c.portfolio_id == CANONICAL_PORTFOLIO_ID)
                .where(table.c.event_type == "skip")
                .order_by(table.c.id.desc())
                .limit(bounded)
            ).scalars())
        rows = [CanonicalPortfolioEvent.model_validate_json(payload) for payload in payloads]
        return {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "count": len(rows),
            "skips": [row.model_dump(mode="json") for row in rows],
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
        integrity = engine.integrity.latest()
        return {
            "cycle": cycle.model_dump(mode="json"),
            "portfolio": latest.model_dump(mode="json") if latest is not None else None,
            "integrity": integrity.model_dump(mode="json") if integrity is not None else None,
        }

    return router
