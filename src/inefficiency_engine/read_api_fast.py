from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from inefficiency_engine.read_api import (
    ALPHA_MECHANISMS,
    RESEARCH_WORKER_ID,
    STATE_PRIORITY,
    _evidence_requirements,
    _latest_payload,
    _max_timestamp,
    _parse_timestamp,
    _require_store,
    app,
    settings,
)
from inefficiency_engine.research_closure import classify_research_worker_state


# The production read plane deliberately avoids table-wide aggregates. Primary-key
# tails are cheap high-water diagnostics only; they are never observation counts.
# Current/historical mechanism counts remain owned by worker-published certification
# state, while source freshness is tracked independently from indexed timestamps.


def _integer_tail(table) -> tuple[int, datetime | None]:
    store = _require_store()
    with store.engine.connect() as db:
        row = db.execute(
            select(table.c.id, table.c.observed_at).order_by(table.c.id.desc()).limit(1)
        ).first()
    if row is None:
        return 0, None
    return int(row[0]), _parse_timestamp(row[1])


def _dex_tail() -> datetime | None:
    store = _require_store()
    with store.engine.connect() as db:
        raw = db.execute(
            select(store.dex_route_quotes.c.observed_at)
            .order_by(store.dex_route_quotes.c.observed_at.desc())
            .limit(1)
        ).scalar_one_or_none()
    return _parse_timestamp(raw)


def _reconciled_blockers(blockers: list[object], capabilities: dict[str, object]) -> list[str]:
    rows = [str(item) for item in blockers]
    if capabilities.get("realized_two_leg_cex_settlement"):
        rows = [
            item for item in rows
            if "allocator-level realized settlement" not in item
            and "multi-leg carry and funding accrual reconstruction" not in item
        ]
    if capabilities.get("perpetual_short_observed_funding_settlement"):
        rows = [
            item for item in rows
            if "exact funding settlement" not in item
            and "realized funding accrual" not in item
        ]
    return rows


def _fast_live_mechanism_overlay(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    store = _require_store()
    now = datetime.now(timezone.utc)

    persistence_healthy = False
    try:
        persistence_healthy = bool(store.ping())
    except Exception:
        pass

    market_high_water, market_at = _integer_tail(store.market_quotes)
    funding_high_water, funding_at = _integer_tail(store.funding_quotes)
    order_book_high_water, order_book_at = _integer_tail(store.order_books)
    dex_at = _dex_tail()

    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(RESEARCH_WORKER_ID)
    except Exception:
        heartbeat = None

    closure = _latest_payload(store, "research_closure_cycle_summaries") or {}
    rejection_funnels = closure.get("rejection_funnels") or {}
    if not isinstance(rejection_funnels, dict):
        rejection_funnels = {}
    capabilities = closure.get("canonical_capabilities") or {}
    if not isinstance(capabilities, dict):
        capabilities = {}
    provider_admission = closure.get("provider_admission") or {}
    if not isinstance(provider_admission, dict):
        provider_admission = {}
    capital_location_forward = closure.get("capital_location_forward") or {}
    if not isinstance(capital_location_forward, dict):
        capital_location_forward = {}
    maker_shadow = closure.get("maker_shadow") or {}
    if not isinstance(maker_shadow, dict):
        maker_shadow = {}

    horizons = tuple(getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,))
    max_horizon = max((float(value) for value in horizons), default=60.0)
    configured_interval = max(1.0, float(getattr(settings, "shadow_cycle_interval_seconds", 30.0)))
    core_expected_interval = max(1.0, max_horizon + configured_interval)
    alpha_every = max(1, int(getattr(settings, "alpha_evidence_every_cycles", 10)))
    alpha_expected_interval = core_expected_interval * alpha_every
    stale_after = max(
        float(getattr(settings, "worker_heartbeat_stale_seconds", 180.0)),
        core_expected_interval * 3.0,
    )

    heartbeat_at = heartbeat.observed_at if heartbeat is not None else None
    heartbeat_state = heartbeat.state if heartbeat is not None else None
    heartbeat_error = heartbeat.error_type if heartbeat is not None else None
    worker_healthy: bool | None = None
    if heartbeat_at is not None and heartbeat is not None:
        age = max(0.0, (now - heartbeat_at).total_seconds())
        worker_healthy = heartbeat.state in {"starting", "running", "success"} and age <= stale_after

    newest_authoritative = _max_timestamp(market_at, funding_at, order_book_at, dex_at)
    core_collection_at = _max_timestamp(newest_authoritative, heartbeat_at)

    live_rows: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = worker_healthy
        row["forward_evidence_persistence_healthy"] = persistence_healthy
        row.setdefault(
            "authoritative_observation_count_semantics",
            "worker_published_mechanism_evidence_count",
        )

        # Indexed table tails prove recent durable activity and provide cheap
        # high-water diagnostics. They must never overwrite a mechanism's persisted
        # evidence count: a primary-key value is neither a row count nor lane-specific.
        high_water: dict[str, int] = {}
        authoritative_at: datetime | None = None
        if mechanism_id == "price_discrepancy":
            high_water["market_quotes"] = market_high_water
            authoritative_at = _max_timestamp(market_at, dex_at)
        elif mechanism_id == "carry":
            high_water["market_quotes"] = market_high_water
            high_water["funding_quotes"] = funding_high_water
            authoritative_at = _max_timestamp(market_at, funding_at)
        elif mechanism_id in {"trend_momentum", "mean_reversion", "cross_sectional_relative_value"}:
            high_water["market_quotes"] = market_high_water
            authoritative_at = market_at
        elif mechanism_id == "microstructure":
            high_water["market_quotes"] = market_high_water
            high_water["order_books"] = order_book_high_water
            authoritative_at = _max_timestamp(market_at, order_book_at)
        elif mechanism_id == "liquidity_provision":
            high_water["order_books"] = order_book_high_water
            authoritative_at = order_book_at

        if high_water:
            row["source_table_high_water_marks"] = high_water
            row["source_table_high_water_marks_display_authority"] = False
        if authoritative_at is not None:
            row["authoritative_observation_last_at"] = authoritative_at.isoformat()

        if mechanism_id not in ALPHA_MECHANISMS and core_collection_at is not None:
            last_cycle = core_collection_at
            expected = core_expected_interval
            row["forward_evidence_last_cycle_at"] = last_cycle.isoformat()
            row["forward_evidence_next_expected_at"] = (last_cycle + timedelta(seconds=expected)).isoformat()
            row["forward_evidence_expected_interval_seconds"] = expected
        else:
            last_cycle = _parse_timestamp(row.get("forward_evidence_last_cycle_at"))
            expected = alpha_expected_interval
            if last_cycle is not None:
                row["forward_evidence_next_expected_at"] = (last_cycle + timedelta(seconds=expected)).isoformat()
                row["forward_evidence_expected_interval_seconds"] = expected

        research_error = heartbeat_error
        if heartbeat is not None and mechanism_id in ALPHA_MECHANISMS:
            detail = heartbeat.detail or {}
            if isinstance(detail, dict):
                research_error = str(detail.get("alpha_forward_evidence_error_type")) if detail.get("alpha_forward_evidence_error_type") else heartbeat_error
        row["forward_evidence_worker_state"] = classify_research_worker_state(
            now=now,
            heartbeat_at=heartbeat_at,
            heartbeat_state=heartbeat_state,
            error_type=research_error,
            last_cycle_at=last_cycle,
            expected_interval_seconds=expected,
            stale_after_seconds=max(stale_after, expected * 3.0),
        )

        funnel = rejection_funnels.get(mechanism_id)
        if isinstance(funnel, dict):
            row["rejection_funnel"] = funnel
            row["raw_candidate_count"] = int(funnel.get("raw_candidate_count") or 0)
            row["emitted_candidate_count"] = int(funnel.get("emitted_candidate_count") or 0)
            row["best_gross_economics"] = funnel.get("best_gross_economics")
            row["best_cost_economics"] = funnel.get("best_cost_economics")
            row["best_net_economics"] = funnel.get("best_net_economics")
            row["required_net_economics"] = funnel.get("required_net_economics")
            row["gap_to_hurdle"] = funnel.get("gap_to_hurdle")
            row["economics_unit"] = funnel.get("economics_unit")
            row["dominant_rejection_gate"] = funnel.get("dominant_rejection_gate")

        if mechanism_id == "capital_location_settlement" and capital_location_forward:
            row["stage"] = "forward_testable"
            row["forward_signal_count"] = int(capital_location_forward.get("trial_count") or 0)
            row["independent_forward_outcome_count"] = int(capital_location_forward.get("outcome_count") or 0)
            row["capital_location_mean_incremental_option_value"] = capital_location_forward.get("mean_incremental_option_value")
            row["capital_location_positive_incremental_rate"] = capital_location_forward.get("positive_incremental_rate")
            if int(capital_location_forward.get("outcome_count") or 0) > 0:
                row["primary_reason"] = (
                    "capital-location recommendations are now being evaluated in independent forward cohorts; "
                    "transfer-cost/latency evidence remains fail-closed"
                )
                row["next_action"] = "continue independent location cohorts and add authoritative transfer/withdrawal cost and latency evidence"

        if mechanism_id == "liquidity_provision" and maker_shadow:
            row["maker_shadow_trial_count"] = int(maker_shadow.get("trial_count") or 0)
            row["maker_shadow_outcome_count"] = int(maker_shadow.get("outcome_count") or 0)
            row["maker_crossed_through_count"] = int(maker_shadow.get("crossed_through_count") or 0)
            row["maker_queue_fill_confirmed_count"] = int(maker_shadow.get("queue_fill_confirmed_count") or 0)
            row["maker_adverse_selection_observation_count"] = int(maker_shadow.get("adverse_selection_observation_count") or 0)
            if int(maker_shadow.get("outcome_count") or 0) > 0:
                row["primary_reason"] = (
                    "shadow maker quote outcomes and post-cross adverse-selection evidence are accumulating; "
                    "aggregated public L2 still cannot prove queue priority or actual maker fills"
                )
                row["next_action"] = "continue conservative maker shadow outcomes and connect order-level/venue fill evidence before estimating maker fill probability"

        admission = provider_admission.get(mechanism_id)
        if isinstance(admission, dict):
            row["provider_admission"] = admission

        row["canonical_capabilities"] = capabilities
        row["blockers"] = _reconciled_blockers(list(row.get("blockers") or []), capabilities)
        live_rows.append(row)

    telemetry = {
        "available": heartbeat_at is not None or newest_authoritative is not None,
        "worker_id": RESEARCH_WORKER_ID,
        "worker_healthy": worker_healthy,
        "persistence_healthy": persistence_healthy,
        "worker_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "latest_collection_at": core_collection_at.isoformat() if core_collection_at is not None else None,
        "latest_authoritative_observation_at": newest_authoritative.isoformat() if newest_authoritative is not None else None,
        "research_closure_observed_at": closure.get("observed_at"),
        "canonical_capabilities": capabilities,
        "durable_high_water_marks": {
            "market_quotes": market_high_water,
            "funding_quotes": funding_high_water,
            "order_books": order_book_high_water,
            "dex_route": None,
        },
        "high_water_marks_are_counts": False,
        "query_mode": "append_only_high_water_plus_compact_closure_summary",
    }
    return live_rows, telemetry


# Replace the expensive/default mechanisms route and action queue with a single
# reconciled live view. Everything else remains the lightweight read plane.
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) in {"/v3/operations/mechanisms", "/v3/operations/action-queue"}
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None


def _mechanism_payload() -> dict[str, Any]:
    store = _require_store()
    latest = _latest_payload(store, "operating_certification_snapshots")
    requirements = _evidence_requirements()
    if latest is None:
        heartbeat = None
        try:
            heartbeat = store.latest_worker_heartbeat(RESEARCH_WORKER_ID)
        except Exception:
            pass
        return {
            "paper_only": True,
            "count": 0,
            "observed_at": None,
            "requirements": requirements,
            "live_telemetry": {
                "available": heartbeat is not None,
                "worker_id": RESEARCH_WORKER_ID,
                "worker_healthy": None,
                "persistence_healthy": bool(store.ping()),
                "high_water_marks_are_counts": False,
                "query_mode": "append_only_high_water_plus_compact_closure_summary",
            },
            "mechanisms": [],
        }
    raw_rows = latest.get("mechanisms") or []
    rows = [dict(row) for row in raw_rows if isinstance(row, dict)]
    mechanisms, live_telemetry = _fast_live_mechanism_overlay(rows)
    return {
        "paper_only": True,
        "count": len(mechanisms),
        "observed_at": latest.get("observed_at"),
        "version": latest.get("version"),
        "requirements": requirements,
        "live_telemetry": live_telemetry,
        "mechanisms": mechanisms,
    }


@app.get("/v3/operations/mechanisms")
def fast_operating_mechanisms():
    return _mechanism_payload()


@app.get("/v3/operations/action-queue")
def fast_operating_action_queue():
    payload = _mechanism_payload()
    rows = [row for row in payload.get("mechanisms", []) if row.get("state") != "certified"]
    rows.sort(key=lambda row: (STATE_PRIORITY.get(str(row.get("state") or ""), 99), str(row.get("mechanism_id") or "")))
    return {
        "paper_only": True,
        "count": len(rows),
        "actions": [
            {
                "mechanism_id": row.get("mechanism_id"),
                "name": row.get("name"),
                "state": row.get("state"),
                "primary_reason": row.get("primary_reason"),
                "next_action": row.get("next_action"),
                "blockers": row.get("blockers") or [],
                "worker_state": row.get("forward_evidence_worker_state"),
                "dominant_rejection_gate": row.get("dominant_rejection_gate"),
            }
            for row in rows
        ],
    }
