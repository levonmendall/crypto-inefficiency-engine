from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from inefficiency_engine.dashboard_projection import build_dashboard_projection
from inefficiency_engine.evidence import EvidenceStore


def _decode(raw: object | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _table_exists(db, backend: str, table_name: str) -> bool:
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


def _read_compact_projections(
    store: EvidenceStore,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Read both compact projections in one short transaction.

    This deliberately performs no provider reconciliation, service construction,
    research replay, or schema mutation. A dashboard refresh must remain a bounded
    read even as the top-volume universe and research architecture grow.
    """

    try:
        with store.engine.begin() as db:
            if store.backend == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '1500ms'"))
                db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            base_raw = None
            research_raw = None
            if _table_exists(db, store.backend, "dashboard_projection_snapshots"):
                base_raw = db.execute(
                    text(
                        "SELECT payload_json FROM dashboard_projection_snapshots "
                        "ORDER BY id DESC LIMIT 1"
                    )
                ).scalar_one_or_none()
            if _table_exists(db, store.backend, "dashboard_research_projection_snapshots"):
                research_raw = db.execute(
                    text(
                        "SELECT payload_json FROM dashboard_research_projection_snapshots "
                        "ORDER BY id DESC LIMIT 1"
                    )
                ).scalar_one_or_none()
    except Exception as exc:
        return None, None, type(exc).__name__
    return _decode(base_raw), _decode(research_raw), None


def build_production_dashboard_snapshot(
    store: EvidenceStore,
    *,
    forward_target: int = 30,
    settled_target: int = 20,
) -> dict[str, Any]:
    """Return the production dashboard without request-time research computation.

    Normal operation reads the two worker-published compact projections. If the
    portfolio projection is absent or invalid, reconstruct it once from durable
    canonical tables using ``build_dashboard_projection``; that helper performs a
    single bounded read transaction. The read plane never fans out through the
    diagnostic endpoints and never constructs an OpportunityService here.
    """

    base, research, compact_error = _read_compact_projections(store)
    fallback_reason: str | None = None
    if base is None:
        fallback_reason = compact_error or "compact_projection_unavailable"
        try:
            base = build_dashboard_projection(
                store,
                forward_target=max(1, int(forward_target)),
                settled_target=max(1, int(settled_target)),
            )
        except Exception as exc:
            # Keep HTTP semantics explicit if durable canonical state itself cannot
            # be read. The caller can still return a controlled 503 and the browser
            # can use its last-good session projection.
            raise RuntimeError(
                f"durable dashboard reconstruction failed: {type(exc).__name__}"
            ) from exc
        base = dict(base)
        base["projection_mode"] = "durable_single_read_fallback"
        base["presentation_fallback"] = True
        base["presentation_fallback_reason"] = fallback_reason

    combined = dict(base)
    if research is not None:
        combined.update(
            {
                "projection_version": max(2, int(combined.get("projection_version") or 1)),
                "projection_mode": "portfolio_plus_persisted_research",
                "observed_at": research.get("observed_at") or combined.get("observed_at"),
                "research_projection_observed_at": research.get("observed_at"),
                "source_operating_observed_at": research.get("source_operating_observed_at"),
                "source_research_closure_observed_at": research.get(
                    "source_research_closure_observed_at"
                ),
                "source_research_heartbeat_at": research.get("source_research_heartbeat_at"),
                "mechanisms": research.get("mechanisms") or combined.get("mechanisms") or {},
                "queue": research.get("queue") or combined.get("queue") or {},
            }
        )
    else:
        combined.setdefault("projection_mode", "portfolio_persisted_only")

    combined["critical_path_persisted_only"] = True
    combined["request_time_research_computation"] = False
    combined["paper_only"] = True
    combined["live_execution_authority"] = False
    return combined
