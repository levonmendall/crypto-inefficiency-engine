from __future__ import annotations

from inefficiency_engine.dashboard_v5_router import build_v5_dashboard_router
from inefficiency_engine.read_api import _latest_payload, _payload_history, _require_store
from inefficiency_engine.read_api_fast import app
from inefficiency_engine.research_reset_runtime import RESEARCH_RESET_POLICY_VERSION


RESEARCH_CLOSURE_WORKER_ID = "research-closure-diagnostic-loop"
RESEARCH_RESET_WORKER_ID = "research-qualification-reset"


# Replace the inherited HTML presentation routes at the shared research read-plane
# layer. Every higher production deploy app composes from this same FastAPI object,
# so V5 no longer depends on which historical module Render launches.
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in {"/", "/dashboard", "/v3/dashboard/v5-snapshot"}
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None
app.include_router(build_v5_dashboard_router())


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


@app.get("/v3/research/candidate-observatory")
def candidate_observatory_status(limit: int = 50):
    """Expose raw signals, near misses, and diagnostic-only shadow learning.

    Priority scores, near-miss ranks, and diagnostic shadow outcomes are research
    telemetry only; they cannot qualify a strategy or authorize allocation.
    """

    store = _require_store()
    bounded = max(1, min(200, int(limit)))
    latest = _latest_payload(store, "candidate_observatory_snapshots")
    recent_candidates = _payload_history(store, "candidate_observatory_events", limit=bounded)
    recent_shadow_events = _payload_history(store, "candidate_observatory_shadow_events", limit=bounded)
    if latest is None:
        return {
            "available": False,
            "recent_candidates": recent_candidates,
            "recent_diagnostic_shadow_events": recent_shadow_events,
            "qualification_policy_version": RESEARCH_RESET_POLICY_VERSION,
            "qualification_thresholds_unchanged": False,
            "observatory_allocation_authority": False,
            "allocation_authority": False,
            "paper_only": True,
            "live_execution_authority": False,
            "message": "no candidate observatory snapshot has been recorded yet",
        }
    return {
        "available": True,
        **latest,
        "recent_candidates": recent_candidates,
        "recent_diagnostic_shadow_events": recent_shadow_events,
        "qualification_policy_version": RESEARCH_RESET_POLICY_VERSION,
        "qualification_thresholds_unchanged": False,
        "observatory_allocation_authority": False,
        "allocation_authority": False,
        "paper_only": True,
        "live_execution_authority": False,
    }


@app.get("/v3/research/qualification-reset")
def qualification_reset_status():
    """Expose the active research-reset policy and its scientific checkpoint."""

    store = _require_store()
    latest = _latest_payload(store, "research_qualification_reset_snapshots")
    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(RESEARCH_RESET_WORKER_ID)
    except Exception:
        heartbeat = None
    runtime = heartbeat.model_dump(mode="json") if heartbeat is not None else None
    if latest is None:
        return {
            "available": False,
            "policy_version": RESEARCH_RESET_POLICY_VERSION,
            "runtime": runtime,
            "paper_only": True,
            "live_execution_authority": False,
            "message": "no research qualification reset snapshot has been recorded yet",
        }
    return {
        "available": True,
        **latest,
        "runtime": runtime,
        "paper_only": True,
        "live_execution_authority": False,
    }
