from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

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

_RESEARCH_WORKER_ID = "shadow-research-auxiliary"
_ALPHA_MECHANISMS = {
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
}


def _parse_timestamp(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _max_timestamp(*values: object | None) -> datetime | None:
    parsed = [item for item in (_parse_timestamp(value) for value in values) if item is not None]
    return max(parsed) if parsed else None


def _live_evidence_overlay(
    store: EvidenceStore,
    settings,
    rows: list[dict[str, object]],
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Overlay current durable collection telemetry without rewriting certification history.

    Operating-certification snapshots remain append-only and point-in-time. The dashboard,
    however, refreshes every 30 seconds and should be able to distinguish a healthy collector
    with no qualifying signal from a stalled collector. This read-only overlay uses current
    persisted row counts, the research worker heartbeat, and database liveness while leaving
    all certification thresholds and historical snapshots unchanged.
    """

    now = now or datetime.now(timezone.utc)
    persistence_healthy = False
    try:
        persistence_healthy = bool(store.ping())
    except Exception:
        persistence_healthy = False

    counts = {
        "market": 0,
        "funding": 0,
        "order_book": 0,
        "dex_route": 0,
    }
    latest = {
        "market": None,
        "funding": None,
        "order_book": None,
        "dex_route": None,
        "scan": None,
    }
    heartbeat_payloads: list[str] = []
    try:
        with store.engine.connect() as db:
            counts["market"] = int(db.execute(select(func.count()).select_from(store.market_quotes)).scalar_one())
            counts["funding"] = int(db.execute(select(func.count()).select_from(store.funding_quotes)).scalar_one())
            counts["order_book"] = int(db.execute(select(func.count()).select_from(store.order_books)).scalar_one())
            counts["dex_route"] = int(db.execute(select(func.count()).select_from(store.dex_route_quotes)).scalar_one())
            latest["market"] = _parse_timestamp(db.execute(select(func.max(store.market_quotes.c.observed_at))).scalar_one_or_none())
            latest["funding"] = _parse_timestamp(db.execute(select(func.max(store.funding_quotes.c.observed_at))).scalar_one_or_none())
            latest["order_book"] = _parse_timestamp(db.execute(select(func.max(store.order_books.c.observed_at))).scalar_one_or_none())
            latest["dex_route"] = _parse_timestamp(db.execute(select(func.max(store.dex_route_quotes.c.observed_at))).scalar_one_or_none())
            latest["scan"] = _parse_timestamp(db.execute(select(func.max(store.scans.c.completed_at))).scalar_one_or_none())
            heartbeat_payloads = list(db.execute(
                select(store.worker_heartbeats.c.payload_json)
                .where(store.worker_heartbeats.c.worker_id == _RESEARCH_WORKER_ID)
                .order_by(store.worker_heartbeats.c.id.desc())
                .limit(200)
            ).scalars())
    except Exception:
        heartbeat_payloads = []

    latest_heartbeat: dict[str, object] | None = None
    latest_success: dict[str, object] | None = None
    latest_alpha_cycle: dict[str, object] | None = None
    for raw in heartbeat_payloads:
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if latest_heartbeat is None:
            latest_heartbeat = payload
        if latest_success is None and payload.get("state") == "success":
            latest_success = payload
        detail = payload.get("detail") or {}
        if (
            latest_alpha_cycle is None
            and isinstance(detail, dict)
            and "alpha_forward_evidence_cycle_id" in detail
        ):
            latest_alpha_cycle = payload
        if latest_heartbeat is not None and latest_success is not None and latest_alpha_cycle is not None:
            break

    horizon_seconds = tuple(getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,))
    max_horizon = max((float(value) for value in horizon_seconds), default=60.0)
    configured_interval = max(1.0, float(getattr(settings, "shadow_cycle_interval_seconds", 30.0)))
    core_expected_interval = max(1.0, max_horizon + configured_interval)
    alpha_every = max(1, int(getattr(settings, "alpha_evidence_every_cycles", 10)))
    alpha_expected_interval = core_expected_interval * alpha_every
    stale_after = max(
        float(getattr(settings, "worker_heartbeat_stale_seconds", 180.0)),
        core_expected_interval * 3.0,
    )

    heartbeat_at = _parse_timestamp((latest_heartbeat or {}).get("observed_at"))
    heartbeat_state = str((latest_heartbeat or {}).get("state") or "")
    worker_healthy: bool | None = None
    if heartbeat_at is not None:
        age = max(0.0, (now - heartbeat_at).total_seconds())
        worker_healthy = heartbeat_state in {"starting", "running", "success"} and age <= stale_after

    last_success_at = _parse_timestamp((latest_success or {}).get("observed_at"))
    latest_scan_at = latest["scan"]
    core_collection_at = _max_timestamp(latest_scan_at, last_success_at)
    alpha_cycle_at = _parse_timestamp((latest_alpha_cycle or {}).get("observed_at"))

    live_rows: list[dict[str, object]] = []
    for original in rows:
        row = dict(original)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = worker_healthy
        row["forward_evidence_persistence_healthy"] = persistence_healthy

        authoritative_count: int | None = None
        authoritative_at: datetime | None = None
        if mechanism_id == "price_discrepancy":
            authoritative_count = counts["market"] + counts["dex_route"]
            authoritative_at = _max_timestamp(latest["market"], latest["dex_route"])
        elif mechanism_id == "carry":
            authoritative_count = counts["market"] + counts["funding"]
            authoritative_at = _max_timestamp(latest["market"], latest["funding"])
        elif mechanism_id in {"trend_momentum", "mean_reversion", "cross_sectional_relative_value"}:
            authoritative_count = counts["market"]
            authoritative_at = latest["market"]
        elif mechanism_id == "microstructure":
            authoritative_count = counts["market"] + counts["order_book"]
            authoritative_at = _max_timestamp(latest["market"], latest["order_book"])
        elif mechanism_id == "liquidity_provision":
            authoritative_count = counts["order_book"]
            authoritative_at = latest["order_book"]

        if authoritative_count is not None:
            row["authoritative_observation_count"] = max(
                int(row.get("authoritative_observation_count") or 0),
                authoritative_count,
            )
        if authoritative_at is not None:
            row["authoritative_observation_last_at"] = authoritative_at.isoformat()

        if mechanism_id in _ALPHA_MECHANISMS and alpha_cycle_at is not None:
            row["forward_evidence_last_cycle_at"] = alpha_cycle_at.isoformat()
            row["forward_evidence_next_expected_at"] = (
                alpha_cycle_at + timedelta(seconds=alpha_expected_interval)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = alpha_expected_interval
        elif core_collection_at is not None:
            row["forward_evidence_last_cycle_at"] = core_collection_at.isoformat()
            row["forward_evidence_next_expected_at"] = (
                core_collection_at + timedelta(seconds=core_expected_interval)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = core_expected_interval

        live_rows.append(row)

    newest_authoritative = _max_timestamp(
        latest["market"], latest["funding"], latest["order_book"], latest["dex_route"]
    )
    telemetry = {
        "available": heartbeat_at is not None or newest_authoritative is not None,
        "worker_id": _RESEARCH_WORKER_ID,
        "worker_healthy": worker_healthy,
        "persistence_healthy": persistence_healthy,
        "worker_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "latest_collection_at": core_collection_at.isoformat() if core_collection_at is not None else None,
        "latest_authoritative_observation_at": (
            newest_authoritative.isoformat() if newest_authoritative is not None else None
        ),
        "durable_counts": counts,
    }
    return live_rows, telemetry


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

    def evidence_requirements(engine: OperatingCertificationService) -> dict[str, int]:
        """Expose the immutable evidence hurdles used by the operating interpreter.

        The dashboard consumes these only for visibility. Returning the thresholds
        from the same service that enforces them avoids duplicating or drifting UI
        assumptions, and does not create allocation or execution authority.
        """

        return {
            "independent_forward_outcomes": engine.min_forward_samples,
            "settled_allocator_outcomes": engine.min_allocator_settled_trials,
        }

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
        requirements = evidence_requirements(engine)
        if latest is None:
            return {
                "paper_only": True,
                "count": 0,
                "observed_at": None,
                "requirements": requirements,
                "live_telemetry": {
                    "available": False,
                    "worker_id": _RESEARCH_WORKER_ID,
                    "worker_healthy": None,
                    "persistence_healthy": bool(evidence_store and evidence_store.ping()),
                },
                "mechanisms": [],
            }
        mechanisms, live_telemetry = _live_evidence_overlay(
            engine.store,
            engine.core.settings,
            [row.model_dump(mode="json") for row in latest.mechanisms],
        )
        return {
            "paper_only": True,
            "count": len(mechanisms),
            "observed_at": latest.observed_at,
            "version": latest.version,
            "requirements": requirements,
            "live_telemetry": live_telemetry,
            "mechanisms": mechanisms,
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
