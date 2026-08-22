from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from inefficiency_engine.dashboard_source_truth import (
    _candidate,
    _fallback_freshness_seconds,
    _latest_candidate,
    _read_source_inputs,
)
from inefficiency_engine.evidence_velocity import evidence_freshness_seconds
from inefficiency_engine.source_coverage_catalog import SOURCES


def _utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _source_row(
    spec: dict[str, object],
    *,
    current: datetime,
    direct: dict[tuple[str, str], dict[str, Any]],
    providers: list[dict[str, object]],
    admissions: list[dict[str, Any]],
    table_candidates: dict[str, dict[str, object]],
    fallback_seconds: float,
) -> dict[str, object]:
    source_id = str(spec["id"])
    lane_ids = [str(value) for value in list(spec.get("lanes") or [])]
    configured_classes = [str(value) for value in list(spec.get("classes") or [])]
    credential = spec.get("credential")
    base: dict[str, object] = {
        "source_id": source_id,
        "name": str(spec.get("name") or source_id),
        "lane_ids": lane_ids,
        "classes": configured_classes,
        "group": str(spec.get("group") or source_id),
        "tier": str(spec.get("tier") or "unknown"),
        "authoritative": bool(spec.get("authoritative", True)),
        "active": bool(spec.get("active", True)),
        "credential_env": str(credential) if credential else None,
        "credential_configured": bool(not credential or os.getenv(str(credential))),
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
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
    for lane_id in lane_ids:
        direct_row = direct.get((lane_id, source_id))
        if direct_row is None:
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
                authoritative=direct_row.get("authoritative", spec.get("authoritative", True)),
                commercial=direct_row.get("commercial_use_permitted", True),
                point_in_time=direct_row.get("point_in_time", True),
                error_type=direct_row.get("error_type"),
                source_reference=direct_row.get("source_reference"),
            )
        )

    prefixes = [str(value) for value in list(spec.get("provider") or [])]
    if prefixes:
        lane_set = set(lane_ids)
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
                        source_reference=admission.get("source_reference") or f"admission:{provider}",
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

    latest = _latest_candidate(candidates)
    if latest is None:
        return {
            **base,
            "state": "unobserved",
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

    observed_at = latest.get("observed_at")
    age_seconds = (
        max(0.0, (current - observed_at).total_seconds())
        if isinstance(observed_at, datetime)
        else None
    )
    effective_classes = [str(value) for value in list(latest.get("classes") or configured_classes)]
    ttl = evidence_freshness_seconds(effective_classes, fallback_seconds=fallback_seconds)
    fresh = bool(age_seconds is not None and age_seconds <= ttl)
    healthy = bool(latest.get("healthy"))
    admitted = bool(
        healthy
        and fresh
        and latest.get("authoritative", True)
        and latest.get("commercial", True)
        and latest.get("point_in_time", True)
    )
    state = "failed" if not healthy else "healthy" if fresh else "stale"
    return {
        **base,
        "state": state,
        "healthy": healthy,
        "fresh": fresh,
        "admitted": admitted,
        "classes": effective_classes,
        "authoritative": bool(latest.get("authoritative", spec.get("authoritative", True))),
        "observed_at": observed_at.isoformat() if isinstance(observed_at, datetime) else None,
        "age_seconds": age_seconds,
        "freshness_ttl_seconds": ttl,
        "freshness_policy": "evidence_class_specific",
        "item_count": max(0, int(latest.get("item_count") or 0)),
        "error_type": latest.get("error_type"),
        "source_reference": latest.get("source_reference"),
    }


def read_source_connectivity(
    store,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read per-source production connectivity without provider calls or schema writes."""

    current = _utc(now)
    try:
        available, direct, providers, admissions, table_candidates = _read_source_inputs(store)
    except Exception as exc:
        return {
            "available": False,
            "observed_at": current.isoformat(),
            "read_error_type": type(exc).__name__,
            "summary": {
                "configured": len(SOURCES),
                "healthy": 0,
                "stale": 0,
                "failed": 0,
                "unobserved": 0,
                "credential_required": 0,
                "admitted": 0,
            },
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
    counts = {
        "configured": len(rows),
        "healthy": sum(row.get("state") == "healthy" for row in rows),
        "stale": sum(row.get("state") == "stale" for row in rows),
        "failed": sum(row.get("state") == "failed" for row in rows),
        "unobserved": sum(row.get("state") == "unobserved" for row in rows),
        "credential_required": sum(row.get("state") == "credential_required" for row in rows),
        "admitted": sum(bool(row.get("admitted")) for row in rows),
    }
    return {
        "available": True,
        "observed_at": current.isoformat(),
        "read_error_type": None,
        "summary": counts,
        "sources": rows,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }
