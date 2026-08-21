from __future__ import annotations

import os

from fastapi import HTTPException

from inefficiency_engine import read_api_research_deploy as _base_deploy
from inefficiency_engine.active_volume_runtime import (
    read_active_cycle_history_status,
    read_active_volume_universe_status,
)
from inefficiency_engine.lane_readiness import build_lane_executable_readiness
from inefficiency_engine.production_dashboard_fastpath import build_production_dashboard_snapshot
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.source_coverage_catalog import LANES


app = _base_deploy.app

# Replace only the deploy/read-model routes whose payloads need the active volume
# universe. All portfolio, mechanism, and research routes remain unchanged.
_REPLACED_PATHS = {
    "/health",
    "/ready",
    "/v3/dashboard/snapshot",
    "/v3/research/cycle-history",
}
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in _REPLACED_PATHS
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None


def _store():
    return _base_deploy._base.evidence_store  # noqa: SLF001 - deploy-layer composition


def _release_commit() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("CIE_RELEASE_COMMIT")
    return value.strip() if value and value.strip() else None


def _lane_readiness():
    """Detailed diagnostic path only; never called by the dashboard fast path."""
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    core = OpportunityService(settings=_base_deploy._base.settings, evidence_store=store)  # noqa: SLF001
    return build_lane_executable_readiness(core, store)


def _lane_summary_from_payload(payload: dict[str, object]) -> dict[str, object]:
    """Build a non-blocking summary from already-persisted mechanism rows.

    Architecture capability is a code property. Current qualification/profitability
    counts are reported only from the compact worker-published mechanism rows already
    present in the dashboard payload; this function performs no database or provider
    work and cannot delay NAV/cash visibility.
    """

    mechanisms = payload.get("mechanisms")
    rows: list[dict[str, object]] = []
    if isinstance(mechanisms, dict):
        raw_rows = mechanisms.get("mechanisms")
        if isinstance(raw_rows, list):
            rows = [row for row in raw_rows if isinstance(row, dict)]

    def _qualified(row: dict[str, object]) -> bool:
        if bool(row.get("currently_qualified")):
            return True
        if str(row.get("state") or "") in {"certifying", "certified"}:
            return True
        try:
            return int(row.get("current_promoted_count") or 0) > 0
        except (TypeError, ValueError):
            return False

    lane_count = len(LANES)
    return {
        "available": bool(rows),
        "lane_count": lane_count,
        "architecture_executable_count": lane_count,
        "currently_qualified_count": sum(_qualified(row) for row in rows),
        "profitability_certified_count": sum(
            bool(row.get("profitability_certified")) for row in rows
        ),
        "all_lanes_paper_execution_capable": lane_count == 13,
        "live_execution_capable": False,
        "summary_source": "persisted_dashboard_projection",
        "request_time_research_computation": False,
        "detail_endpoint": "/v3/lane-executability",
    }


@app.get("/health")
def deployment_health():
    payload = dict(_base_deploy.deployment_health())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
            "dashboard_critical_path_persisted_only": True,
        }
    )
    return payload


@app.get("/ready")
def deployment_readiness():
    payload = dict(_base_deploy.deployment_readiness())
    payload.update(
        {
            "release_commit": _release_commit(),
            "volume_universe_observability": True,
            "active_cycle_history_membership": True,
            "thirteen_lane_executable_readiness": True,
            "dashboard_critical_path_persisted_only": True,
        }
    )
    return payload


@app.get("/v3/research/volume-universe")
def volume_universe_status():
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_active_volume_universe_status(store)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="active volume universe is temporarily unavailable",
        ) from exc


@app.get("/v3/research/cycle-history")
def cycle_history_status():
    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_active_cycle_history_status(store)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="active cycle-history status is temporarily unavailable",
        ) from exc


@app.get("/v3/lane-executability")
def lane_executability():
    """Detailed diagnostic endpoint; intentionally outside the dashboard deadline."""
    try:
        return _lane_readiness().model_dump(mode="json")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="lane executable readiness is temporarily unavailable",
        ) from exc


@app.get("/v3/lane-executability/{lane_id}")
def lane_executability_detail(lane_id: str):
    snapshot = _lane_readiness()
    for lane in snapshot.lanes:
        if lane.lane_id == lane_id:
            return lane.model_dump(mode="json")
    raise HTTPException(status_code=404, detail="unknown profit-mechanism lane")


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    """Return a bounded persisted-only command-center snapshot.

    The phone refresh deadline must never include research/service construction.
    Normal reads use worker-published compact projections; if the portfolio compact
    row is absent, the server reconstructs the portfolio once from durable tables in
    one bounded read path. Top-40 and history metadata remain optional DB-only
    enrichments and cannot erase the canonical portfolio payload.
    """

    store = _store()
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")

    try:
        payload = build_production_dashboard_snapshot(
            store,
            forward_target=max(
                1,
                int(getattr(_base_deploy._base.settings, "alpha_min_forward_samples", 30)),  # noqa: SLF001
            ),
            settled_target=max(
                5,
                int(
                    getattr(
                        _base_deploy._base.settings,  # noqa: SLF001
                        "operating_certification_min_settled_trials",
                        20,
                    )
                ),
            ),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="persisted dashboard snapshot is temporarily unavailable",
        ) from exc

    try:
        volume = read_active_volume_universe_status(store)
    except Exception:
        volume = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "volume_is_defining_metric": True,
        }
    try:
        cycle_history = read_active_cycle_history_status(store)
    except Exception:
        cycle_history = {
            "available": False,
            "asset_count": 0,
            "assets": [],
            "historical_counts_as_forward": False,
            "live_execution_authority": False,
        }

    payload["volume_universe"] = volume
    payload["cycle_history"] = cycle_history
    payload["lane_executability"] = _lane_summary_from_payload(payload)
    payload["release_commit"] = _release_commit()
    payload["volume_universe_observability"] = True
    payload["active_cycle_history_membership"] = True
    payload["dashboard_critical_path_persisted_only"] = True
    return payload
