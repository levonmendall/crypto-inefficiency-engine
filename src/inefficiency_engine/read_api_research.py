from __future__ import annotations

from inefficiency_engine.dashboard_research_closure import build_research_closure_dashboard_router
from inefficiency_engine.read_api import _latest_payload, _require_store
from inefficiency_engine.read_api_fast import app


# Replace only the HTML presentation routes. The API remains the v3.5.25 fast,
# database-backed read plane and does not construct providers, scanners, allocators,
# or execution services.
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in {"/", "/dashboard"}
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None
app.include_router(build_research_closure_dashboard_router())


@app.get("/v3/operations/research-closure")
def research_closure_status():
    """Return the latest compact research-closure checkpoint.

    This endpoint is read-only. It exposes diagnostic funnels, capability truth,
    provider admission readiness, and forward-research cohort summaries without
    creating strategy, allocation, or execution authority.
    """

    store = _require_store()
    latest = _latest_payload(store, "research_closure_cycle_summaries")
    if latest is None:
        return {
            "available": False,
            "paper_only": True,
            "live_execution_authority": False,
            "message": "no research closure cycle has been recorded yet",
        }
    return {
        "available": True,
        **latest,
        "paper_only": True,
        "live_execution_authority": False,
    }
