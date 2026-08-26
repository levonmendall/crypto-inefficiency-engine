from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from inefficiency_engine import read_api_active_volume_deploy as read_plane
from inefficiency_engine import read_api_card_history_deploy as cards
from inefficiency_engine import read_api_end_to_end_certification_deploy as inner
from inefficiency_engine.durable_lane_history_projection import (
    DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID,
)
from inefficiency_engine.source_coverage_catalog import LANES


DURABLE_HISTORY_PATH = "/v3/dashboard/durable-lane-history"
PROJECTION_STALE_SECONDS = 180.0
app = inner.app


def _parse_time(value: object | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _empty_lane(lane_id: str) -> dict[str, object]:
    definition = LANES[lane_id]
    required = sorted(str(value) for value in list(definition.get("required") or []))
    return {
        "lane_id": lane_id,
        "lane_name": str(definition.get("name") or lane_id),
        "history_available": False,
        "evidence_class_history_complete": False,
        "required_evidence_class_count": len(required),
        "recovered_evidence_class_count": 0,
        "evidence_class_fill_ratio": 0.0 if required else 1.0,
        "canonical_source_snapshot_count": 0,
        "recovered_source_observations": 0,
        "recovered_operating_snapshots": 0,
        "earliest_recovered_at": None,
        "latest_recovered_at": None,
        "historical_evidence_classes": [],
        "missing_historical_evidence_classes": required,
        "source_ids": [],
        "source_ledgers": [],
        "max_authoritative_observation_count": 0,
        "max_economic_candidate_count": 0,
        "max_forward_signal_count": 0,
        "max_independent_forward_outcome_count": 0,
        "latest_operating_state": None,
        "candidate_level_history_synthesized": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def empty_history_payload(*, reason: str, error_type: str | None = None) -> dict[str, object]:
    lanes = {lane_id: _empty_lane(lane_id) for lane_id in LANES}
    error = {"stage": "durable_history_projection", "error_type": error_type or reason}
    return {
        "endpoint_available": True,
        "history_projection_available": False,
        "history_projection_stale": True,
        "history_projection_reason": reason,
        "history_projection_error_type": error_type,
        "lane_count": len(lanes),
        "lanes_with_durable_history": 0,
        "lanes_without_durable_history": len(lanes),
        "lanes_with_all_required_evidence_classes": 0,
        "lanes": lanes,
        "read_degraded": True,
        "read_errors": [error],
        "raw_history_reconstruction_on_http": False,
        "candidate_level_history_synthesized": False,
        "historical_counts_as_forward": False,
        "qualification_authority": False,
        "allocation_authority": False,
        "live_execution_authority": False,
        "paper_only": True,
    }


def durable_history_projection_payload(store: Any | None = None) -> dict[str, object]:
    """Serve one already-materialized projection; never reconstruct history in HTTP."""

    if store is None:
        store = read_plane._store()  # noqa: SLF001 - deployment read-plane composition
    if store is None:
        return empty_history_payload(reason="evidence_persistence_not_configured")

    try:
        heartbeat = store.latest_worker_heartbeat(DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID)
    except Exception as exc:
        return empty_history_payload(
            reason="projection_heartbeat_read_failed",
            error_type=type(exc).__name__,
        )
    if heartbeat is None:
        return empty_history_payload(reason="projection_not_published_yet")

    detail = getattr(heartbeat, "detail", {})
    history = detail.get("history") if isinstance(detail, dict) else None
    if not isinstance(history, dict) or not isinstance(history.get("lanes"), dict):
        return empty_history_payload(reason="projection_payload_missing")

    # Preserve the last valid projection even when it becomes stale. Historical truth
    # does not become false with age; staleness is surfaced explicitly and never converted
    # into zeros. A fresh child will replace it on the next successful projection cycle.
    observed_at = _parse_time(getattr(heartbeat, "observed_at", None))
    age_seconds = None
    if observed_at is not None:
        age_seconds = max(
            0.0,
            (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
        )
    stale = age_seconds is None or age_seconds > PROJECTION_STALE_SECONDS

    payload = dict(history)
    payload.update(
        {
            "endpoint_available": True,
            "history_projection_available": True,
            "history_projection_stale": stale,
            "history_projection_age_seconds": age_seconds,
            "history_projection_observed_at": (
                observed_at.isoformat() if observed_at is not None else None
            ),
            "history_projection_state": getattr(heartbeat, "state", None),
            "history_projection_error_type": getattr(heartbeat, "error_type", None),
            "history_projection_worker_id": DURABLE_LANE_HISTORY_PROJECTION_WORKER_ID,
            "raw_history_reconstruction_on_http": False,
            "candidate_level_history_synthesized": False,
            "historical_counts_as_forward": False,
            "qualification_authority": False,
            "allocation_authority": False,
            "live_execution_authority": False,
            "paper_only": True,
        }
    )
    return payload


# Replace the older request-time durable-history route with the projection-only route.
app.router.routes[:] = [
    route for route in app.router.routes if getattr(route, "path", None) != DURABLE_HISTORY_PATH
]


@app.get(DURABLE_HISTORY_PATH)
def durable_lane_history_projection():
    return durable_history_projection_payload()


_previous_dashboard_html = cards._dashboard_html


def _replace_once(html: str, old: str, new: str) -> str:
    if old not in html:
        raise RuntimeError(f"durable history UI patch target missing: {old[:80]}")
    return html.replace(old, new, 1)


def history_projection_dashboard_html() -> str:
    """Never render a missing durable-history request as fabricated 0/0 history."""

    html = _previous_dashboard_html()
    html = _replace_once(
        html,
        "    }catch(_e){laneDurableHistory=null}",
        "    }catch(e){if(laneDurableHistory)laneDurableHistory.last_fetch_error=String(e)}",
    )
    html = _replace_once(
        html,
        "    const durableRecovered=+(durable?.recovered_evidence_class_count||0);\n"
        "    const durableRequired=+(durable?.required_evidence_class_count||0);\n"
        "    const durableSourceRecords=+(durable?.recovered_source_observations||0);",
        "    const durableRecovered=+(durable?.recovered_evidence_class_count||0);\n"
        "    const durableRequired=+(durable?.required_evidence_class_count||0);\n"
        "    const durableSourceRecords=+(durable?.recovered_source_observations||0);\n"
        "    const durableEvidenceLabel=durable?`${num(durableRecovered)}/${num(durableRequired)}`:'UNAVAILABLE';\n"
        "    const durableSourceLabel=durable?num(durableSourceRecords):'UNAVAILABLE';",
    )
    html = _replace_once(
        html,
        '<div class="v">${num(durableRecovered)}/${num(durableRequired)}</div>',
        '<div class="v">${durableEvidenceLabel}</div>',
    )
    html = _replace_once(
        html,
        '<div class="v">${num(durableSourceRecords)}</div>',
        '<div class="v">${durableSourceLabel}</div>',
    )
    html = _replace_once(
        html,
        "'No trustworthy persisted lane history has been recovered yet.';",
        "durable?'No trustworthy persisted lane history has been recovered yet.':'Durable history projection is temporarily unavailable; history is not being reported as zero.';",
    )
    html = html.replace(
        "setInterval(refreshLaneDurableHistory,300000);",
        "setInterval(refreshLaneDurableHistory,30000);",
        1,
    )
    return html


cards._dashboard_html = history_projection_dashboard_html


__all__ = [
    "DURABLE_HISTORY_PATH",
    "app",
    "durable_history_projection_payload",
    "durable_lane_history_projection",
    "empty_history_payload",
    "history_projection_dashboard_html",
]
