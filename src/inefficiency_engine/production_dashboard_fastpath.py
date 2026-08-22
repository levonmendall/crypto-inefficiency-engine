from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from inefficiency_engine.dashboard_projection import build_dashboard_projection
from inefficiency_engine.evidence import EvidenceStore


DEFAULT_RESEARCH_PROJECTION_STALE_SECONDS = 900.0
DEFAULT_OPERATING_PROJECTION_STALE_SECONDS = 1800.0
DEFAULT_COMPACT_READ_TIMEOUT_MS = 1500
RETRY_COMPACT_READ_TIMEOUT_MS = 4500


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
    *,
    statement_timeout_ms: int = DEFAULT_COMPACT_READ_TIMEOUT_MS,
    lock_timeout_ms: int = 500,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Read both compact projections in one short transaction.

    This deliberately performs no provider reconciliation, service construction,
    research replay, or schema mutation. A dashboard refresh must remain a bounded
    read even as the top-volume universe and research architecture grow.
    """

    statement_timeout_ms = max(100, int(statement_timeout_ms))
    lock_timeout_ms = max(50, int(lock_timeout_ms))
    try:
        with store.engine.begin() as db:
            if store.backend == "postgresql":
                db.execute(
                    text(f"SET LOCAL statement_timeout = '{statement_timeout_ms}ms'")
                )
                db.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'"))
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


def _env_stale_seconds(name: str, default: float, *, minimum: float = 180.0) -> float:
    try:
        configured = float(os.getenv(name, str(default)))
    except ValueError:
        configured = default
    return max(minimum, configured)


def _projection_freshness(
    observed_value: object | None,
    *,
    available: bool,
    now: datetime,
    stale_seconds: float,
    label: str,
) -> dict[str, Any]:
    observed = _parse_timestamp(observed_value)
    if observed is None:
        return {
            "available": available,
            "observed_at": None,
            "age_seconds": None,
            "stale_after_seconds": stale_seconds,
            "stale": available,
            "reason": f"{label} has no valid observed_at timestamp" if available else f"{label} is unavailable",
        }
    age = max(0.0, (now - observed).total_seconds())
    stale = age > stale_seconds
    return {
        "available": True,
        "observed_at": observed.isoformat(),
        "age_seconds": age,
        "stale_after_seconds": stale_seconds,
        "stale": stale,
        "reason": f"{label} is {age:.1f}s old (limit {stale_seconds:.1f}s)" if stale else None,
    }


def research_projection_freshness(
    research: dict[str, Any] | None,
    *,
    now: datetime | None = None,
    stale_seconds: float | None = None,
) -> dict[str, Any]:
    """Evaluate a persisted research projection against the actual wall clock."""

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    threshold = max(
        1.0,
        float(
            stale_seconds
            if stale_seconds is not None
            else _env_stale_seconds(
                "CIE_RESEARCH_PROJECTION_STALE_SECONDS",
                DEFAULT_RESEARCH_PROJECTION_STALE_SECONDS,
            )
        ),
    )
    return _projection_freshness(
        (research or {}).get("observed_at"),
        available=research is not None,
        now=current,
        stale_seconds=threshold,
        label="research projection",
    )


def operating_projection_freshness(
    research: dict[str, Any] | None,
    *,
    now: datetime | None = None,
    stale_seconds: float | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    threshold = max(
        1.0,
        float(
            stale_seconds
            if stale_seconds is not None
            else _env_stale_seconds(
                "CIE_OPERATING_PROJECTION_STALE_SECONDS",
                DEFAULT_OPERATING_PROJECTION_STALE_SECONDS,
            )
        ),
    )
    return _projection_freshness(
        (research or {}).get("source_operating_observed_at"),
        available=research is not None,
        now=current,
        stale_seconds=threshold,
        label="operating certification projection",
    )


def reconcile_mechanism_runtime_truth(
    payload: object,
    *,
    now: datetime | None = None,
    research_freshness: dict[str, Any] | None = None,
    operating_freshness: dict[str, Any] | None = None,
) -> object:
    """Re-evaluate runtime/cadence fields at request time without changing economics.

    Statistical failure, poor economics, provider gaps, and certification are durable
    research results. This function never changes those states. It only prevents an
    old reason string from saying a collector is healthy after its own expected
    collection deadline or source operating projection has gone stale.
    """

    if not isinstance(payload, dict):
        return payload
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    research_freshness = research_freshness or {"stale": False}
    operating_freshness = operating_freshness or {"stale": False}

    result = dict(payload)
    raw_rows = result.get("mechanisms")
    if not isinstance(raw_rows, list):
        return result

    rows: list[object] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            rows.append(raw)
            continue
        row = dict(raw)
        reason = str(row.get("primary_reason") or "")
        next_action = str(row.get("next_action") or "")
        worker_state = str(row.get("forward_evidence_worker_state") or "")
        expected_at = _parse_timestamp(row.get("forward_evidence_next_expected_at"))
        expected_interval = 0.0
        try:
            expected_interval = max(0.0, float(row.get("forward_evidence_expected_interval_seconds") or 0.0))
        except (TypeError, ValueError):
            expected_interval = 0.0
        overdue_seconds = max(0.0, (current - expected_at).total_seconds()) if expected_at else 0.0
        overdue_grace = max(180.0, expected_interval)
        overdue = expected_at is not None and overdue_seconds > overdue_grace
        unhealthy_state = worker_state in {"late", "stalled", "failed", "unknown"}
        runtime_stale = bool(research_freshness.get("stale")) or overdue or unhealthy_state

        prefixes: list[str] = []
        if bool(research_freshness.get("stale")):
            age = research_freshness.get("age_seconds")
            if isinstance(age, (int, float)):
                prefixes.append(f"research runtime projection is stale ({float(age) / 60.0:.1f} minutes old)")
            else:
                prefixes.append("research runtime projection is stale")
        if overdue:
            prefixes.append(
                f"forward evidence collection is overdue by {overdue_seconds / 60.0:.1f} minutes"
            )
        elif unhealthy_state:
            prefixes.append(f"forward evidence worker state is {worker_state}")
        if bool(operating_freshness.get("stale")):
            age = operating_freshness.get("age_seconds")
            if isinstance(age, (int, float)):
                prefixes.append(
                    f"operating certification snapshot is stale ({float(age) / 60.0:.1f} minutes old)"
                )
            else:
                prefixes.append("operating certification snapshot is stale")

        if runtime_stale:
            reason = reason.replace("forward collector healthy", "forward collector degraded")
            row["forward_evidence_worker_healthy"] = False
            if worker_state not in {"failed", "stalled"}:
                row["forward_evidence_worker_state"] = "stalled" if overdue else worker_state or "unknown"
            row["forward_evidence_overdue_seconds"] = overdue_seconds if overdue else 0.0
            stale_action = "restore successful current research publication before interpreting this lane as current"
            if stale_action not in next_action:
                next_action = f"{stale_action}; {next_action}" if next_action else stale_action

        if prefixes:
            prefix = "; ".join(prefixes)
            if prefix not in reason:
                reason = f"{prefix} · {reason}" if reason else prefix

        row["primary_reason"] = reason
        row["next_action"] = next_action
        row["research_projection_stale"] = bool(research_freshness.get("stale"))
        row["operating_projection_stale"] = bool(operating_freshness.get("stale"))
        rows.append(row)

    result["mechanisms"] = rows
    result["research_projection_stale"] = bool(research_freshness.get("stale"))
    result["research_projection_freshness"] = research_freshness
    result["operating_projection_stale"] = bool(operating_freshness.get("stale"))
    result["operating_projection_freshness"] = operating_freshness
    live = result.get("live_telemetry")
    if isinstance(live, dict):
        live = dict(live)
        live.update(
            {
                "research_projection_stale": bool(research_freshness.get("stale")),
                "research_projection_age_seconds": research_freshness.get("age_seconds"),
                "operating_projection_stale": bool(operating_freshness.get("stale")),
                "operating_projection_age_seconds": operating_freshness.get("age_seconds"),
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

    Normal operation reads the two worker-published compact projections. A transient
    compact-read failure gets one bounded retry before any durable reconstruction is
    attempted. If the portfolio projection is genuinely absent or invalid, reconstruct
    it once from durable canonical tables using ``build_dashboard_projection``. The
    read plane never fans out through diagnostic endpoints or constructs an
    OpportunityService here.

    Persisted research and each lane's own expected collection deadline are evaluated
    against the actual request-time UTC clock. Old evidence remains visible, but it
    cannot continue claiming current collector health.
    """

    base, research, first_compact_error = _read_compact_projections(store)
    retry_used = False
    retry_error: str | None = None
    if base is None and first_compact_error is not None:
        retry_used = True
        retry_base, retry_research, retry_error = _read_compact_projections(
            store,
            statement_timeout_ms=RETRY_COMPACT_READ_TIMEOUT_MS,
            lock_timeout_ms=1000,
        )
        if retry_base is not None:
            base = retry_base
            research = retry_research
        elif research is None and retry_research is not None:
            research = retry_research

    fallback_reason: str | None = None
    if base is None:
        fallback_reason = retry_error or first_compact_error or "compact_projection_unavailable"
        try:
            base = build_dashboard_projection(
                store,
                forward_target=max(1, int(forward_target)),
                settled_target=max(1, int(settled_target)),
            )
        except Exception as exc:
            compact_context = fallback_reason or "none"
            raise RuntimeError(
                "durable dashboard reconstruction failed after compact read "
                f"({compact_context}): {type(exc).__name__}"
            ) from exc
        base = dict(base)
        base["projection_mode"] = "durable_single_read_fallback"
        base["presentation_fallback"] = True
        base["presentation_fallback_reason"] = fallback_reason

    combined = dict(base)
    combined["compact_projection_read_retry_used"] = retry_used
    combined["compact_projection_read_initial_error_type"] = first_compact_error
    combined["compact_projection_read_retry_error_type"] = retry_error
    current = datetime.now(timezone.utc)
    research_freshness = research_projection_freshness(research, now=current)
    operating_freshness = operating_projection_freshness(research, now=current)
    if research is not None:
        mechanisms = reconcile_mechanism_runtime_truth(
            research.get("mechanisms") or combined.get("mechanisms") or {},
            now=current,
            research_freshness=research_freshness,
            operating_freshness=operating_freshness,
        )
        any_stale = bool(research_freshness.get("stale")) or bool(operating_freshness.get("stale"))
        combined.update(
            {
                "projection_version": max(2, int(combined.get("projection_version") or 1)),
                "projection_mode": (
                    "portfolio_plus_stale_research"
                    if any_stale
                    else "portfolio_plus_persisted_research"
                ),
                "observed_at": research.get("observed_at") or combined.get("observed_at"),
                "research_projection_observed_at": research.get("observed_at"),
                "research_projection_stale": bool(research_freshness.get("stale")),
                "research_projection_freshness": research_freshness,
                "operating_projection_stale": bool(operating_freshness.get("stale")),
                "operating_projection_freshness": operating_freshness,
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
        combined["research_projection_freshness"] = research_freshness
        combined["operating_projection_stale"] = False
        combined["operating_projection_freshness"] = operating_freshness

    combined["critical_path_persisted_only"] = True
    combined["request_time_research_computation"] = False
    combined["paper_only"] = True
    combined["live_execution_authority"] = False
    return combined
