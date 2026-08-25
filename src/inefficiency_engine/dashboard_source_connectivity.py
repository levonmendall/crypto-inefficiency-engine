from __future__ import annotations

from copy import deepcopy
import os
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from sqlalchemy import inspect, text

from inefficiency_engine.dashboard_source_truth import (
    _SAFE_TABLES,
    _candidate,
    _fallback_freshness_seconds,
    _json_payload,
    _table_candidate,
)
from inefficiency_engine.evidence_velocity import evidence_freshness_seconds
from inefficiency_engine.runtime_provider_policy import env_flag
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


_LAST_SUCCESSFUL_BY_STORE: dict[int, dict[str, object]] = {}
_LAST_SUCCESSFUL_LOCK = Lock()


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _read_source_input_history(
    store,
) -> tuple[
    set[str],
    list[dict[str, Any]],
    list[dict[str, object]],
    list[dict[str, Any]],
    dict[str, dict[str, object]],
]:
    """Read bounded source histories in one transaction without provider calls.

    The canonical source resolver needs history, not only the newest attempt. A failed
    refresh is transport telemetry and must not erase a still-fresh successful
    observation. Database work is identical in breadth to the prior reader; this
    function simply keeps the bounded rows instead of collapsing them in Python.
    """

    available = set(inspect(store.engine).get_table_names())
    direct: list[dict[str, Any]] = []
    providers: list[dict[str, object]] = []
    admissions: list[dict[str, Any]] = []
    table_candidates: dict[str, dict[str, object]] = {}

    with store.engine.begin() as db:
        if getattr(store, "backend", "") == "postgresql":
            db.execute(text("SET LOCAL statement_timeout = '1200ms'"))
            db.execute(text("SET LOCAL lock_timeout = '300ms'"))

        if "source_coverage_observations" in available:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM source_coverage_observations "
                        "ORDER BY id DESC LIMIT 3000"
                    )
                ).scalars()
            )
            for raw in raws:
                payload = _json_payload(raw)
                if payload is None:
                    continue
                lane_id = str(payload.get("lane_id") or "")
                source_id = str(payload.get("source_id") or "")
                if lane_id in LANES and source_id:
                    direct.append(payload)

        if "provider_statuses" in available:
            providers = [
                dict(row)
                for row in db.execute(
                    text(
                        "SELECT provider,ok,item_count,error_type,observed_at "
                        "FROM provider_statuses ORDER BY id DESC LIMIT 1000"
                    )
                ).mappings()
            ]

        if "provider_gap_admissions" in available:
            raws = list(
                db.execute(
                    text(
                        "SELECT payload_json FROM provider_gap_admissions "
                        "ORDER BY id DESC LIMIT 1000"
                    )
                ).scalars()
            )
            for raw in raws:
                payload = _json_payload(raw)
                if payload is None:
                    continue
                mechanism_id = str(payload.get("mechanism_id") or "")
                provider = str(payload.get("provider") or "")
                if mechanism_id and provider:
                    admissions.append(payload)

        for spec in SOURCES:
            source_id = str(spec["id"])
            candidate = _table_candidate(db, spec, available)
            if candidate is not None:
                table_candidates[source_id] = candidate

    return available, direct, providers, admissions, table_candidates


def _candidate_time(candidate: dict[str, object]) -> datetime:
    observed_at = candidate.get("observed_at")
    if isinstance(observed_at, datetime):
        return observed_at
    return datetime.min.replace(tzinfo=timezone.utc)


def _candidate_eval(
    candidate: dict[str, object],
    *,
    current: datetime,
    configured_classes: list[str],
    fallback_seconds: float,
) -> tuple[datetime | None, float | None, list[str], float, bool]:
    observed_at = candidate.get("observed_at")
    observed = observed_at if isinstance(observed_at, datetime) else None
    age_seconds = (
        max(0.0, (current - observed).total_seconds()) if observed is not None else None
    )
    classes = [
        str(value) for value in list(candidate.get("classes") or configured_classes)
    ]
    ttl = evidence_freshness_seconds(classes, fallback_seconds=fallback_seconds)
    fresh = bool(age_seconds is not None and age_seconds <= ttl)
    return observed, age_seconds, classes, ttl, fresh


def _source_row(
    spec: dict[str, object],
    *,
    current: datetime,
    direct: list[dict[str, Any]],
    providers: list[dict[str, object]],
    admissions: list[dict[str, Any]],
    table_candidates: dict[str, dict[str, object]],
    fallback_seconds: float,
) -> dict[str, object]:
    source_id = str(spec["id"])
    lane_ids = [str(value) for value in list(spec.get("lanes") or [])]
    configured_classes = [str(value) for value in list(spec.get("classes") or [])]
    credential = spec.get("credential")
    enabled_env = spec.get("enabled_env")
    enabled = env_flag(str(enabled_env), default=True) if enabled_env else True
    tier = str(spec.get("tier") or "unknown")
    base: dict[str, object] = {
        "source_id": source_id,
        "name": str(spec.get("name") or source_id),
        "lane_ids": lane_ids,
        "classes": configured_classes,
        "group": str(spec.get("group") or source_id),
        "tier": tier,
        "authoritative": bool(spec.get("authoritative", True)),
        "active": bool(spec.get("active", True)),
        "enabled_env": str(enabled_env) if enabled_env else None,
        "enabled": enabled,
        "credential_env": str(credential) if credential else None,
        "credential_configured": bool(not credential or os.getenv(str(credential))),
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
    if not enabled:
        return {
            **base,
            "state": "not_applicable",
            "healthy": False,
            "fresh": False,
            "admitted": False,
            "observed_at": None,
            "age_seconds": None,
            "freshness_ttl_seconds": evidence_freshness_seconds(
                configured_classes,
                fallback_seconds=fallback_seconds,
            ),
            "item_count": 0,
            "error_type": None,
            "source_reference": None,
            "status_reason": "disabled_by_runtime_provider_policy",
        }
    if credential and not os.getenv(str(credential)):
        return {
            **base,
            "state": "credential_required",
            "healthy": False,
            "fresh": False,
            "admitted": False,
            "observed_at": None,
            "age_seconds": None,
            "freshness_ttl_seconds": evidence_freshness_seconds(
                configured_classes,
                fallback_seconds=fallback_seconds,
            ),
            "item_count": 0,
            "error_type": None,
            "source_reference": None,
        }

    candidates: list[dict[str, object]] = []
    lane_set = set(lane_ids)
    for direct_row in direct:
        if str(direct_row.get("source_id") or "") != source_id:
            continue
        if str(direct_row.get("lane_id") or "") not in lane_set:
            continue
        candidates.append(
            _candidate(
                observed_at=direct_row.get("observed_at"),
                healthy=direct_row.get("healthy"),
                item_count=direct_row.get("item_count"),
                classes=[
                    str(value)
                    for value in list(
                        direct_row.get("evidence_classes") or spec.get("classes") or []
                    )
                ],
                authoritative=direct_row.get(
                    "authoritative", spec.get("authoritative", True)
                ),
                commercial=direct_row.get("commercial_use_permitted", True),
                point_in_time=direct_row.get("point_in_time", True),
                error_type=direct_row.get("error_type"),
                source_reference=direct_row.get("source_reference"),
            )
        )

    prefixes = [str(value) for value in list(spec.get("provider") or [])]
    if prefixes:
        for admission in admissions:
            if str(admission.get("mechanism_id") or "") not in lane_set:
                continue
            provider = str(admission.get("provider") or "")
            if any(provider.startswith(prefix) for prefix in prefixes):
                candidates.append(
                    _candidate(
                        observed_at=admission.get("observed_at"),
                        healthy=admission.get("healthy"),
                        item_count=admission.get("item_count"),
                        classes=configured_classes,
                        authoritative=admission.get(
                            "authoritative", spec.get("authoritative", True)
                        ),
                        commercial=admission.get("commercial_use_permitted", True),
                        point_in_time=admission.get("point_in_time", True),
                        error_type=admission.get("error_type"),
                        source_reference=admission.get("source_reference")
                        or f"admission:{provider}",
                    )
                )
        for provider_row in providers:
            provider = str(provider_row.get("provider") or "")
            if any(provider.startswith(prefix) for prefix in prefixes):
                candidates.append(
                    _candidate(
                        observed_at=provider_row.get("observed_at"),
                        healthy=provider_row.get("ok"),
                        item_count=provider_row.get("item_count"),
                        classes=configured_classes,
                        authoritative=bool(spec.get("authoritative", True)),
                        commercial=True,
                        point_in_time=True,
                        error_type=provider_row.get("error_type"),
                        source_reference=f"provider_status:{provider}",
                    )
                )

    table_candidate = table_candidates.get(source_id)
    if table_candidate is not None:
        candidates.append(table_candidate)

    if not candidates:
        state = "awaiting_endogenous" if tier == "internal" else "unobserved"
        return {
            **base,
            "state": state,
            "healthy": False,
            "fresh": False,
            "admitted": False,
            "observed_at": None,
            "age_seconds": None,
            "freshness_ttl_seconds": evidence_freshness_seconds(
                configured_classes,
                fallback_seconds=fallback_seconds,
            ),
            "item_count": 0,
            "error_type": None,
            "source_reference": None,
            "status_reason": (
                "generated_only_after_governed_activity" if tier == "internal" else None
            ),
        }

    ordered = sorted(candidates, key=_candidate_time, reverse=True)
    latest = ordered[0]
    latest_observed, latest_age, _, latest_ttl, latest_fresh = _candidate_eval(
        latest,
        current=current,
        configured_classes=configured_classes,
        fallback_seconds=fallback_seconds,
    )
    latest_state = (
        "failed"
        if not latest.get("healthy")
        else "healthy"
        if latest_fresh
        else "stale"
    )

    selected: dict[str, object] | None = None
    selected_eval: tuple[datetime | None, float | None, list[str], float, bool] | None = None
    for candidate in ordered:
        evaluated = _candidate_eval(
            candidate,
            current=current,
            configured_classes=configured_classes,
            fallback_seconds=fallback_seconds,
        )
        if not (
            candidate.get("healthy")
            and evaluated[4]
            and candidate.get("authoritative", True)
            and candidate.get("commercial", True)
            and candidate.get("point_in_time", True)
        ):
            continue
        selected = candidate
        selected_eval = evaluated
        break

    latest_fields = {
        "latest_attempt_state": latest_state,
        "latest_attempt_observed_at": (
            latest_observed.isoformat() if latest_observed is not None else None
        ),
        "latest_attempt_age_seconds": latest_age,
        "latest_attempt_freshness_ttl_seconds": latest_ttl,
        "latest_attempt_error_type": latest.get("error_type"),
        "latest_attempt_source_reference": latest.get("source_reference"),
        "latest_attempt_item_count": max(0, int(latest.get("item_count") or 0)),
    }

    if selected is None or selected_eval is None:
        effective_classes = [
            str(value) for value in list(latest.get("classes") or configured_classes)
        ]
        return {
            **base,
            "state": latest_state,
            "healthy": bool(latest.get("healthy")),
            "fresh": latest_fresh,
            "admitted": False,
            "classes": effective_classes,
            "authoritative": bool(
                latest.get("authoritative", spec.get("authoritative", True))
            ),
            "observed_at": (
                latest_observed.isoformat() if latest_observed is not None else None
            ),
            "age_seconds": latest_age,
            "freshness_ttl_seconds": latest_ttl,
            "freshness_policy": "evidence_class_specific",
            "item_count": max(0, int(latest.get("item_count") or 0)),
            "error_type": latest.get("error_type"),
            "source_reference": latest.get("source_reference"),
            "using_prior_fresh_evidence": False,
            "refresh_degraded": False,
            **latest_fields,
        }

    observed_at, age_seconds, effective_classes, ttl, _ = selected_eval
    using_prior = selected is not latest
    refresh_degraded = bool(using_prior and latest_state != "healthy")
    return {
        **base,
        "state": "healthy",
        "healthy": True,
        "fresh": True,
        "admitted": True,
        "classes": effective_classes,
        "authoritative": bool(
            selected.get("authoritative", spec.get("authoritative", True))
        ),
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
        "age_seconds": age_seconds,
        "freshness_ttl_seconds": ttl,
        "freshness_policy": "evidence_class_specific",
        "item_count": max(0, int(selected.get("item_count") or 0)),
        "error_type": selected.get("error_type"),
        "source_reference": selected.get("source_reference"),
        "using_prior_fresh_evidence": using_prior,
        "refresh_degraded": refresh_degraded,
        "status_reason": (
            "latest_refresh_failed_prior_evidence_still_fresh"
            if refresh_degraded
            else None
        ),
        **latest_fields,
    }


def _summary(rows: list[dict[str, object]]) -> dict[str, int]:
    connectivity_rows = [
        row
        for row in rows
        if row.get("tier") != "internal" and row.get("state") != "not_applicable"
    ]
    return {
        "configured": len(rows),
        "connectivity_configured": len(connectivity_rows),
        "healthy": sum(row.get("state") == "healthy" for row in connectivity_rows),
        "stale": sum(row.get("state") == "stale" for row in connectivity_rows),
        "failed": sum(row.get("state") == "failed" for row in connectivity_rows),
        "unobserved": sum(row.get("state") == "unobserved" for row in connectivity_rows),
        "awaiting_endogenous": sum(
            row.get("state") == "awaiting_endogenous" for row in rows
        ),
        "credential_required": sum(
            row.get("state") == "credential_required" for row in connectivity_rows
        ),
        "not_applicable": sum(row.get("state") == "not_applicable" for row in rows),
        "admitted": sum(bool(row.get("admitted")) for row in connectivity_rows),
        "refresh_degraded": sum(
            bool(row.get("refresh_degraded")) for row in connectivity_rows
        ),
    }


def _cache_success(store, payload: dict[str, object]) -> None:
    with _LAST_SUCCESSFUL_LOCK:
        _LAST_SUCCESSFUL_BY_STORE[id(store)] = deepcopy(payload)


def _cached_after_read_failure(
    store,
    *,
    current: datetime,
    error_type: str,
) -> dict[str, object] | None:
    with _LAST_SUCCESSFUL_LOCK:
        cached = deepcopy(_LAST_SUCCESSFUL_BY_STORE.get(id(store)))
    if not isinstance(cached, dict):
        return None

    rows = [dict(row) for row in list(cached.get("sources") or []) if isinstance(row, dict)]
    for row in rows:
        raw_observed = row.get("observed_at")
        if not raw_observed:
            continue
        try:
            observed_at = datetime.fromisoformat(str(raw_observed).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (current - observed_at.astimezone(timezone.utc)).total_seconds())
        row["age_seconds"] = age_seconds
        try:
            ttl = float(row.get("freshness_ttl_seconds") or 0.0)
        except (TypeError, ValueError):
            ttl = 0.0
        if row.get("state") == "healthy" and ttl > 0.0 and age_seconds > ttl:
            row["state"] = "stale"
            row["healthy"] = False
            row["fresh"] = False
            row["admitted"] = False
            row["cache_expired_during_read_failure"] = True

    last_successful_observed_at = cached.get("observed_at")
    cached.update(
        {
            "available": False,
            "observed_at": current.isoformat(),
            "last_successful_observed_at": last_successful_observed_at,
            "read_error_type": error_type,
            "diagnostic_read_degraded": True,
            "served_last_successful_snapshot": True,
            "summary": _summary(rows),
            "sources": rows,
        }
    )
    return cached


def read_source_connectivity(
    store,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read stable per-source connectivity without provider calls or schema writes.

    Current source state is based on the newest still-fresh usable evidence. The newest
    acquisition attempt remains visible separately, so transient transport failures do
    not erase valid evidence. If the diagnostic database read itself fails, the last
    successful snapshot is retained and aged forward; cached healthy evidence still
    becomes stale at its unchanged TTL.
    """

    current = _utc(now)
    try:
        available, direct, providers, admissions, table_candidates = (
            _read_source_input_history(store)
        )
    except Exception as exc:
        cached = _cached_after_read_failure(
            store,
            current=current,
            error_type=type(exc).__name__,
        )
        if cached is not None:
            return cached
        return {
            "available": False,
            "observed_at": current.isoformat(),
            "read_error_type": type(exc).__name__,
            "diagnostic_read_degraded": True,
            "served_last_successful_snapshot": False,
            "summary": {
                "configured": len(SOURCES),
                "connectivity_configured": 0,
                "healthy": 0,
                "stale": 0,
                "failed": 0,
                "unobserved": 0,
                "awaiting_endogenous": 0,
                "credential_required": 0,
                "not_applicable": 0,
                "admitted": 0,
                "refresh_degraded": 0,
            },
            "sources": [],
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }

    relevant_surfaces = {
        "source_coverage_observations",
        "provider_statuses",
        "provider_gap_admissions",
    } | _SAFE_TABLES
    if not (available & relevant_surfaces):
        return {
            "available": False,
            "observed_at": current.isoformat(),
            "read_error_type": "SourceSurfacesUnavailable",
            "diagnostic_read_degraded": True,
            "served_last_successful_snapshot": False,
            "summary": _summary([]),
            "sources": [],
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }

    fallback = _fallback_freshness_seconds()
    rows = [
        _source_row(
            spec,
            current=current,
            direct=direct,
            providers=providers,
            admissions=admissions,
            table_candidates=table_candidates,
            fallback_seconds=fallback,
        )
        for spec in SOURCES
    ]
    payload: dict[str, object] = {
        "available": True,
        "observed_at": current.isoformat(),
        "read_error_type": None,
        "diagnostic_read_degraded": False,
        "served_last_successful_snapshot": False,
        "summary": _summary(rows),
        "sources": rows,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
    _cache_success(store, payload)
    return payload
