from __future__ import annotations

from inefficiency_engine.dashboard_research_closure import build_research_closure_dashboard_router
from inefficiency_engine.read_api import _latest_payload, _require_store
from inefficiency_engine.read_api_fast import app


RESEARCH_CLOSURE_WORKER_ID = "research-closure-diagnostic-loop"


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
    """Return the latest compact research-closure checkpoint and runtime state.

    This endpoint is read-only. It exposes diagnostic funnels, capability truth,
    provider admission readiness, and forward-research cohort summaries without
    creating strategy, allocation, or execution authority. If summary publication
    has not succeeded yet, the dedicated worker heartbeat makes the failed stage
    visible instead of presenting an unexplained empty state.
    """

    store = _require_store()
    latest = _latest_payload(store, "research_closure_cycle_summaries")
    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(RESEARCH_CLOSURE_WORKER_ID)
    except Exception:
        heartbeat = None
    runtime = heartbeat.model_dump(mode="json") if heartbeat is not None else None

    if latest is None:
        return {
            "available": False,
            "paper_only": True,
            "live_execution_authority": False,
            "runtime": runtime,
            "message": (
                "research closure has not recorded a summary; inspect runtime for the exact stage/error"
                if runtime is not None
                else "no research closure cycle has been recorded yet"
            ),
        }
    return {
        "available": True,
        **latest,
        "runtime": runtime,
        "paper_only": True,
        "live_execution_authority": False,
    }
