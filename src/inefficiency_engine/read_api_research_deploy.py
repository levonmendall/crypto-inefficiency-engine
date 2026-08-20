from __future__ import annotations

from fastapi import HTTPException

from inefficiency_engine import __version__
from inefficiency_engine import evidence as evidence_module
from inefficiency_engine.read_evidence import build_read_only_evidence_store


# Production import bootstrap: keep the v3.5.26 deployment guarantees while
# layering the research-closure read plane. The web process never owns schema.
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
    }
