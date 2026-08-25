from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine.candidate_observatory import OBSERVATORY_WORKER_ID
from inefficiency_engine.candidate_observatory_historical_replay import (
    read_historical_candidate_replay,
)
from inefficiency_engine.dashboard_v5_router import build_v5_dashboard_router
from inefficiency_engine.read_api import _latest_payload, _payload_history, _require_store
from inefficiency_engine.read_api_fast import app
from inefficiency_engine.research_reset_runtime import RESEARCH_RESET_POLICY_VERSION


RESEARCH_CLOSURE_WORKER_ID = "research-closure-diagnostic-loop"
RESEARCH_RESET_WORKER_ID = "research-qualification-reset"
RESEARCH_CLOSURE_PRESENTATION_STALE_SECONDS = 1_800.0


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


def _payload_freshness(
    payload: dict[str, object],
    *,
    stale_after_seconds: float = RESEARCH_CLOSURE_PRESENTATION_STALE_SECONDS,
    now: datetime | None = None,
) -> dict[str, object]:
    raw = payload.get("observed_at")
    try:
        observed_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return {
            "fresh": False,
            "stale": True,
            "age_seconds": None,
            "freshness_sla_seconds": float(stale_after_seconds),
            "freshness_error": "invalid_observed_at",
        }
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age_seconds = max(
        0.0,
        (current.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
    )
    stale = age_seconds > max(1.0, float(stale_after_seconds))
    return {
        "fresh": not stale,
        "stale": stale,
        "age_seconds": age_seconds,
        "freshness_sla_seconds": float(stale_after_seconds),
    }


@app.get("/v3/operations/research-closure")
def research_closure_status():
    """Return the latest compact research-closure checkpoint and runtime state.

    ``available`` means a durable record exists. Freshness is reported separately so
    an old checkpoint can never be mistaken for current production diagnosis. This
    endpoint is read-only and does not create strategy, allocation, or execution
    authority.
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
            "fresh": False,
            "stale": True,
            "paper_only": True,
            "live_execution_authority": False,
            "runtime": runtime,
            "message": (
                "research closure has not recorded a summary; inspect runtime for the exact stage/error"
                if runtime is not None
                else "no research closure cycle has been recorded yet"
            ),
        }
    freshness = _payload_freshness(latest)
    return {
        "available": True,
        **latest,
        **freshness,
        "runtime": runtime,
        "paper_only": True,
        "live_execution_authority": False,
    }


@app.get("/v3/research/candidate-observatory")
def candidate_observatory_status(limit: int = 50):
    """Expose live observatory truth plus separately labeled historical replay.

    Historical replay is diagnostic-only and comes exclusively from evidence that was
    already persisted before the live observatory existed. It never counts as forward
    evidence and can never qualify a strategy or authorize allocation.
    """

    store = _require_store()
    bounded = max(1, min(200, int(limit)))
    latest = _latest_payload(store, "candidate_observatory_snapshots")
    recent_candidates = _payload_history(store, "candidate_observatory_events", limit=bounded)
    recent_shadow_events = _payload_history(store, "candidate_observatory_shadow_events", limit=bounded)
    historical_replay = read_historical_candidate_replay(store, limit=bounded)
    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(OBSERVATORY_WORKER_ID)
    except Exception:
        heartbeat = None
    runtime = heartbeat.model_dump(mode="json") if heartbeat is not None else None
    if latest is None:
        return {
            "available": False,
            "recent_candidates": recent_candidates,
            "recent_diagnostic_shadow_events": recent_shadow_events,
            "historical_replay": historical_replay,
            "historical_replay_available": bool(historical_replay.get("available")),
            "runtime": runtime,
            "qualification_policy_version": RESEARCH_RESET_POLICY_VERSION,
            "qualification_thresholds_unchanged": True,
            "observatory_allocation_authority": False,
            "allocation_authority": False,
            "paper_only": True,
            "live_execution_authority": False,
            "message": (
                "no live candidate observatory snapshot has been recorded yet; historical replay is exposed separately"
                if historical_replay.get("available")
                else "no candidate observatory snapshot has been recorded yet"
            ),
        }
    return {
        "available": True,
        **latest,
        "recent_candidates": recent_candidates,
        "recent_diagnostic_shadow_events": recent_shadow_events,
        "historical_replay": historical_replay,
        "historical_replay_available": bool(historical_replay.get("available")),
        "runtime": runtime,
        "qualification_policy_version": RESEARCH_RESET_POLICY_VERSION,
        "qualification_thresholds_unchanged": True,
        "observatory_allocation_authority": False,
        "allocation_authority": False,
        "paper_only": True,
        "live_execution_authority": False,
    }


@app.get("/v3/research/candidate-observatory/history")
def candidate_observatory_history(limit: int = 200):
    """Return bounded pre-observatory history without mixing it into live evidence."""

    store = _require_store()
    bounded = max(1, min(500, int(limit)))
    return read_historical_candidate_replay(store, limit=bounded)


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
