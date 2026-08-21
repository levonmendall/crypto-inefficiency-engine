from __future__ import annotations

import json

from fastapi import HTTPException
from sqlalchemy import text

from inefficiency_engine import __version__
from inefficiency_engine import evidence as evidence_module
from inefficiency_engine.cycle_history_runtime import read_cycle_history_status
from inefficiency_engine.operating_state_read import (
    rebuild_live_action_queue,
    reconcile_live_operating_states,
)
from inefficiency_engine.provider_readiness_read import reconcile_provider_readiness
from inefficiency_engine.read_evidence import build_read_only_evidence_store
from inefficiency_engine.strategy_evidence_read import augment_mechanism_payload


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
        "provider_readiness_reconciliation": True,
        "live_operating_state_reconciliation": True,
        "cycle_history_backfill_observability": True,
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
        "provider_readiness_reconciliation": True,
        "live_operating_state_reconciliation": True,
        "cycle_history_backfill_observability": True,
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


def _safe_decode_projection(raw: object | None, *, label: str) -> dict[str, object] | None:
    try:
        return _decode_projection(raw, label=label)
    except HTTPException:
        return None


def _read_mapping(call, fallback: dict[str, object]) -> dict[str, object]:
    """Contain one diagnostic read so presentation fallback never hides canonical data."""
    try:
        payload = call()
    except Exception:
        return dict(fallback)
    return dict(payload) if isinstance(payload, dict) else dict(fallback)


def _durable_portfolio_fallback(*, reason: str) -> dict[str, object]:
    """Reconstruct the dashboard from bounded durable reads when its cache is absent.

    This is presentation-only. It never writes state, creates schema, reruns research,
    or synthesizes portfolio economics. If canonical state itself does not exist the
    portfolio remains explicitly unavailable; research sections degrade independently.
    """

    portfolio = _read_mapping(
        _base.canonical_portfolio,
        {
            "available": False,
            "portfolio_id": _base.CANONICAL_PORTFOLIO_ID,
            "initial_capital_usd": _base.CANONICAL_INITIAL_CAPITAL_USD,
            "paper_only": True,
        },
    )
    performance = _read_mapping(
        _base.canonical_portfolio_performance,
        {
            "available": False,
            "portfolio_id": _base.CANONICAL_PORTFOLIO_ID,
            "initial_capital_usd": _base.CANONICAL_INITIAL_CAPITAL_USD,
            "paper_only": True,
            "live_execution_authority": False,
        },
    )
    runtime = _read_mapping(
        _base.canonical_portfolio_runtime_status,
        {
            "portfolio_id": _base.CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "operational": False,
            "degraded": True,
            "valuation_status": "unavailable",
            "cycle_status": "unavailable",
            "allocation_family_failures": [],
        },
    )
    positions = _read_mapping(
        _base.canonical_portfolio_positions,
        {"portfolio_id": _base.CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": 0, "positions": []},
    )
    trades = _read_mapping(
        lambda: _base.canonical_portfolio_trades(limit=20),
        {"portfolio_id": _base.CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": 0, "trades": []},
    )
    history = _read_mapping(
        lambda: _base.canonical_portfolio_history(limit=250),
        {"portfolio_id": _base.CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": 0, "snapshots": []},
    )
    skips = _read_mapping(
        lambda: _base.canonical_portfolio_skips(limit=20),
        {"portfolio_id": _base.CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": 0, "skips": []},
    )
    attribution = _read_mapping(
        _base.canonical_portfolio_attribution,
        {
            "portfolio_id": _base.CANONICAL_PORTFOLIO_ID,
            "paper_only": True,
            "pnl_by_mechanism_usd": {},
            "pnl_by_strategy_usd": {},
        },
    )
    requirements = {
        "independent_forward_outcomes": max(
            1,
            int(getattr(_base.settings, "alpha_min_forward_samples", 30)),
        ),
        "settled_allocator_outcomes": max(
            5,
            int(getattr(_base.settings, "operating_certification_min_settled_trials", 20)),
        ),
    }
    mechanisms = _read_mapping(
        _base.operating_mechanisms,
        {
            "paper_only": True,
            "count": 0,
            "observed_at": None,
            "requirements": requirements,
            "live_telemetry": {"available": False},
            "mechanisms": [],
        },
    )
    queue = _read_mapping(
        _base.operating_action_queue,
        {"paper_only": True, "count": 0, "actions": []},
    )
    observed_at = portfolio.get("observed_at") or runtime.get("latest_snapshot_observed_at")
    return {
        "projection_version": 1,
        "projection_kind": "portfolio",
        "projection_mode": "durable_portfolio_fallback",
        "presentation_fallback": True,
        "presentation_fallback_reason": reason,
        "observed_at": observed_at,
        "source_portfolio_observed_at": observed_at,
        "portfolio": portfolio,
        "performance": performance,
        "runtime": runtime,
        "positions": positions,
        "trades": trades,
        "history": history,
        "skips": skips,
        "attribution": attribution,
        "mechanisms": mechanisms,
        "queue": queue,
        "paper_only": True,
        "live_execution_authority": False,
    }


def _attributed_sections(
    store,
    mechanisms: dict[str, object],
    queue: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Presentation-only live reconciliation; never changes portfolio authority."""
    try:
        # Reconcile from narrowest/latest evidence outward. Provider admission fixes
        # connectivity first; strategy attribution reconstructs current statistical
        # and allocator evidence; the final operating pass updates the displayed lane
        # label and rebuilds the action queue from that reconciled state.
        provider_reconciled = reconcile_provider_readiness(store, mechanisms)
        attributed = augment_mechanism_payload(store, _base.settings, provider_reconciled)
        operating_reconciled = reconcile_live_operating_states(attributed, _base.settings)
        return operating_reconciled, rebuild_live_action_queue(operating_reconciled)
    except Exception:
        # Dashboard enrichment is fail-contained. The durable operating projection
        # remains authoritative if diagnostics cannot be enriched.
        return mechanisms, queue


@app.get("/v3/research/cycle-history")
def cycle_history_status():
    """Read-only proof of historical backfill coverage and replay readiness."""
    store = _base.evidence_store
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    try:
        return read_cycle_history_status(store)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="cycle-history status is temporarily unavailable",
        ) from exc


@app.get("/v3/dashboard/snapshot")
def dashboard_snapshot():
    """Return portfolio state plus the independently refreshed research-card projection.

    The browser still makes one request. The normal path performs bounded tail reads
    from worker-published compact projections. If that presentation cache is absent,
    invalid, or temporarily unreadable, the API reconstructs only the display payload
    from bounded durable canonical reads. Research/history may degrade independently;
    canonical portfolio visibility does not depend on their publication cadence.
    """
    store = _base.evidence_store
    if store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")

    base_raw = None
    research_raw = None
    compact_read_error: str | None = None
    try:
        with store.engine.begin() as db:
            if store.backend == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
                db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
            if _projection_table_exists(db, store.backend, "dashboard_projection_snapshots"):
                base_raw = db.execute(
                    text(
                        "SELECT payload_json FROM dashboard_projection_snapshots "
                        "ORDER BY id DESC LIMIT 1"
                    )
                ).scalar_one_or_none()
            if _projection_table_exists(db, store.backend, "dashboard_research_projection_snapshots"):
                research_raw = db.execute(
                    text(
                        "SELECT payload_json FROM dashboard_research_projection_snapshots "
                        "ORDER BY id DESC LIMIT 1"
                    )
                ).scalar_one_or_none()
    except Exception as exc:
        compact_read_error = type(exc).__name__

    base = _safe_decode_projection(base_raw, label="portfolio")
    if base is None:
        fallback_reason = compact_read_error or (
            "compact_projection_invalid" if base_raw is not None else "compact_projection_unavailable"
        )
        base = _durable_portfolio_fallback(reason=fallback_reason)
    research = _safe_decode_projection(research_raw, label="research")

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
    try:
        cycle_history = read_cycle_history_status(store)
    except Exception:
        cycle_history = {
            "available": False,
            "assets": [],
            "historical_counts_as_forward": False,
            "full_forward_promotion_gate_unchanged": True,
            "live_execution_authority": False,
        }

    if research is None:
        combined = dict(base)
        combined["mechanisms"] = mechanisms
        combined["queue"] = queue
        combined["cycle_history"] = cycle_history
        combined["strategy_evidence_attribution"] = bool(
            mechanisms.get("strategy_evidence_attribution")
        )
        combined["provider_readiness_reconciliation"] = bool(
            mechanisms.get("provider_readiness_reconciled")
        )
        combined["live_operating_state_reconciliation"] = bool(
            mechanisms.get("live_operating_state_reconciled")
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
        "cycle_history": cycle_history,
        "strategy_evidence_attribution": bool(
            mechanisms.get("strategy_evidence_attribution")
        ),
        "provider_readiness_reconciliation": bool(
            mechanisms.get("provider_readiness_reconciled")
        ),
        "live_operating_state_reconciliation": bool(
            mechanisms.get("live_operating_state_reconciled")
        ),
    })
    return combined
