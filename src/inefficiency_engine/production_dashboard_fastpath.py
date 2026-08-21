from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from inefficiency_engine.dashboard_projection import build_dashboard_projection
from inefficiency_engine.evidence import EvidenceStore


DEFAULT_RESEARCH_PROJECTION_STALE_SECONDS = 900.0


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


def _parse_timestamp(value: object | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _research_projection_stale_seconds() -> float:
    try:
        configured = float(
            os.getenv(
                "CIE_RESEARCH_PROJECTION_STALE_SECONDS",
                str(DEFAULT_RESEARCH_PROJECTION_STALE_SECONDS),
            )
        )
    except ValueError:
        configured = DEFAULT_RESEARCH_PROJECTION_STALE_SECONDS
    return max(180.0, configured)


def research_projection_freshness(
    research: dict[str, Any] | None,
    *,
    now: datetime | None = None,
    stale_seconds: float | None = None,
) -> dict[str, Any]:
    """Evaluate a persisted research projection against the actual wall clock.

    A stale research projection remains useful historical evidence, but it must not
    be presented as current operating health. This check is deliberately read-only
    and does not reinterpret any profitability/statistical result.
    """

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    threshold = max(
        1.0,
        float(stale_seconds if stale_seconds is not None else _research_projection_stale_seconds()),
    )
    observed = _parse_timestamp((research or {}).get("observed_at"))
    if observed is None:
        return {
            "available": research is not None,
            "observed_at": None,
            "age_seconds": None,
            "stale_after_seconds": threshold,
            "stale": research is not None,
            "reason": "research projection has no valid observed_at timestamp"
            if research is not None
            else "research projection is unavailable",
        }
    age = max(0.0, (current - observed).total_seconds())
    stale = age > threshold
    return {
        "available": True,
        "observed_at": observed.isoformat(),
        "age_seconds": age,
        "stale_after_seconds": threshold,
        "stale": stale,
        "reason": (
            f"research projection is {age:.1f}s old (limit {threshold:.1f}s)"
            if stale
            else None
        ),
    }


def _stale_research_mechanisms(
    payload: object,
    *,
    freshness: dict[str, Any],
) -> object:
    """Make stale runtime status explicit without rewriting strategy economics."""

    if not isinstance(payload, dict):
        return payload
    result = dict(payload)
    raw_rows = result.get("mechanisms")
    if not isinstance(raw_rows, list):
        result["research_projection_stale"] = True
        result["research_projection_freshness"] = freshness
        return result

    age = freshness.get("age_seconds")
    observed = freshness.get("observed_at")
    prefix = "research runtime projection is stale"
    if isinstance(age, (int, float)):
        prefix += f" ({float(age) / 60.0:.1f} minutes old)"
    if observed:
        prefix += f"; last research projection {observed}"

    rows: list[object] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            rows.append(raw)
            continue
        row = dict(raw)
        reason = str(row.get("primary_reason") or "")
        reason = reason.replace("forward collector healthy", "forward collector degraded")
        if prefix not in reason:
            reason = f"{prefix} · {reason}" if reason else prefix
        next_action = str(row.get("next_action") or "")
        stale_action = "restore successful research publication before interpreting this lane as current"
        if stale_action not in next_action:
            next_action = f"{stale_action}; {next_action}" if next_action else stale_action
        row.update(
            {
                "primary_reason": reason,
                "next_action": next_action,
                "forward_evidence_worker_healthy": False,
                "research_projection_stale": True,
                "research_projection_age_seconds": age,
            }
        )
        rows.append(row)

    result["mechanisms"] = rows
    result["research_projection_stale"] = True
    result["research_projection_freshness"] = freshness
    live = result.get("live_telemetry")
    if isinstance(live, dict):
        live = dict(live)
        live.update(
            {
                "research_projection_stale": True,
                "research_projection_age_seconds": age,
                "research_projection_observed_at": observed,
            }
        )
        result["live_telemetry"] = live
    return result


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

    Persisted research is also evaluated against the actual request-time UTC clock.
    Stale research remains visible as historical evidence, but current-health fields
    are degraded and the payload explicitly identifies the stale projection.
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
    freshness = research_projection_freshness(research)
    if research is not None:
        stale = bool(freshness.get("stale"))
        mechanisms = research.get("mechanisms") or combined.get("mechanisms") or {}
        if stale:
            mechanisms = _stale_research_mechanisms(mechanisms, freshness=freshness)
        combined.update(
            {
                "projection_version": max(2, int(combined.get("projection_version") or 1)),
                "projection_mode": (
                    "portfolio_plus_stale_research"
                    if stale
                    else "portfolio_plus_persisted_research"
                ),
                "observed_at": research.get("observed_at") or combined.get("observed_at"),
                "research_projection_observed_at": research.get("observed_at"),
                "research_projection_stale": stale,
                "research_projection_freshness": freshness,
                "source_operating_observed_at": research.get("source_operating_observed_at"),
                "source_research_closure_observed_at": research.get(
                    "source_research_closure_observed_at"
                ),
                "source_research_heartbeat_at": research.get("source_research_heartbeat_at"),
                "mechanisms": mechanisms,
                "queue": research.get("queue") or combined.get("queue") or {},
            }
        )
    else:
        combined.setdefault("projection_mode", "portfolio_persisted_only")
        combined["research_projection_stale"] = False
        combined["research_projection_freshness"] = freshness

    combined["critical_path_persisted_only"] = True
    combined["request_time_research_computation"] = False
    combined["paper_only"] = True
    combined["live_execution_authority"] = False
    return combined
