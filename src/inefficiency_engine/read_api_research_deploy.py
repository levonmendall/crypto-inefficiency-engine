from __future__ import annotations

import json

from fastapi import HTTPException
from sqlalchemy import text

from inefficiency_engine import __version__
from inefficiency_engine import evidence as evidence_module
from inefficiency_engine.read_evidence import build_read_only_evidence_store
from inefficiency_engine.strategy_evidence_read import (
    augment_mechanism_payload,
    reconcile_action_queue,
)


# Production import bootstrap: keep the deployment guarantees while layering the
# research-closure read plane. The web process never owns schema.
_original_builder = evidence_module.build_evidence_store
evidence_module.build_evidence_store = build_read_only_evidence_store
try:
    from inefficiency_engine import read_api as _base
    from inefficiency_engine import read_api_fast as _fast
    from inefficiency_engine import read_api_research as _research
finally:
    evidence_module.build_evidence_store = _original_builder

app = _research.app


# Render liveness proves only the web process. PostgreSQL readiness is separately
# observable and bounded by the read-only evidence store's connection timeout.
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/health"
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None


@app.get("/health")
def deployment_health():
    store = _base.evidence_store
    return {
        "status": "ok",
        "version": __version__,
        "paper_only": True,
        "read_plane": True,
        "live_execution": False,
        "evidence_persistence": store is not None,
        "evidence_backend": getattr(store, "backend", None),
        "database_check": "deferred_to_readiness",
        "schema_owner": "worker",
        "research_closure": True,
        "dashboard_projection": "portfolio_plus_live_research",
        "strategy_evidence_attribution": True,
    }


@app.get("/ready")
def deployment_readiness():
    store = _base.evidence_store
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        database_ok = bool(store.ping())
    except Exception as exc:
        raise HTTPException(status_code=503, detail="evidence database is unavailable") from exc
    if not database_ok:
        raise HTTPException(status_code=503, detail="evidence database is unavailable")
    return {
        "status": "ready",
        "version": __version__,
        "paper_only": True,
        "read_plane": True,
        "database_ok": True,
        "evidence_backend": store.backend,
        "schema_owner": "worker",
        "research_closure": True,
        "dashboard_projection": "portfolio_plus_live_research",
        "strategy_evidence_attribution": True,
    }


def _projection_table_exists(db, backend: str, table_name: str) -> bool:
    if backend == "postgresql":
        return db.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        ).scalar_one_or_none() is not None
    return db.execute(
        text(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name=:table_name LIMIT 1"
        ),
        {"table_name": table_name},
    ).scalar_one_or_none() is not None


def _decode_projection(raw: object | None, *, label: str) -> dict[str, object] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail=f"{label} dashboard projection is invalid") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=503, detail=f"{label} dashboard projection is invalid")
    return payload


def _attributed_sections(
    store,
    mechanisms: dict[str, object],
    queue: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Presentation-only strategy attribution; never changes portfolio authority."""
    try:
        attributed = augment_mechanism_payload(store, _base.settings, mechanisms)
        return attributed, reconcile_action_queue(queue, attributed)
    except Exception:
        # Dashboard attribution is fail-contained. The durable operating projection
        # remains authoritative if diagnostics cannot be enriched.
        return mechanisms, queue


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    """Return portfolio state plus the independently refreshed research-card projection.

    The browser still makes one request. The API performs bounded tail reads inside
    one transaction: the latest portfolio-led snapshot plus, when available, the
    research snapshot published after the latest successful research cycle. Strategy
    attribution is cached by append-only ledger tails and is presentation-only.
    """
    store = _base.evidence_store
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        with store.engine.begin() as db:
            if store.backend == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
                db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
            base_raw = db.execute(
                text(
                    "SELECT payload_json FROM dashboard_projection_snapshots "
                    "ORDER BY id DESC LIMIT 1"
                )
            ).scalar_one_or_none()
            research_raw = None
            if _projection_table_exists(db, store.backend, "dashboard_research_projection_snapshots"):
                research_raw = db.execute(
                    text(
                        "SELECT payload_json FROM dashboard_research_projection_snapshots "
                        "ORDER BY id DESC LIMIT 1"
                    )
                ).scalar_one_or_none()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="dashboard projection is temporarily unavailable",
        ) from exc

    base = _decode_projection(base_raw, label="portfolio")
    if base is None:
        raise HTTPException(
            status_code=503,
            detail="dashboard projection is awaiting its first worker publication",
        )
    research = _decode_projection(research_raw, label="research")

    source_mechanisms = (
        (research or {}).get("mechanisms")
        or base.get("mechanisms")
        or {}
    )
    source_queue = (
        (research or {}).get("queue")
        or base.get("queue")
        or {}
    )
    mechanisms, queue = _attributed_sections(
        store,
        dict(source_mechanisms) if isinstance(source_mechanisms, dict) else {},
        dict(source_queue) if isinstance(source_queue, dict) else {},
    )

    if research is None:
        combined = dict(base)
        combined["mechanisms"] = mechanisms
        combined["queue"] = queue
        combined["strategy_evidence_attribution"] = bool(
            mechanisms.get("strategy_evidence_attribution")
        )
        return combined

    combined = dict(base)
    combined.update({
        "projection_version": 2,
        "projection_mode": "portfolio_plus_live_research",
        "observed_at": research.get("observed_at") or base.get("observed_at"),
        "research_projection_observed_at": research.get("observed_at"),
        "source_operating_observed_at": research.get("source_operating_observed_at"),
        "source_research_closure_observed_at": research.get("source_research_closure_observed_at"),
        "source_research_heartbeat_at": research.get("source_research_heartbeat_at"),
        "mechanisms": mechanisms,
        "queue": queue,
        "strategy_evidence_attribution": bool(
            mechanisms.get("strategy_evidence_attribution")
        ),
    })
    return combined
