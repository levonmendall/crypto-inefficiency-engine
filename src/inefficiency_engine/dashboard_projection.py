from __future__ import annotations

import hashlib
import json
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Column, Index, Integer, MetaData, Table, Text, insert, inspect, text

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CANONICAL_PORTFOLIO_ID,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.research_closure import classify_research_worker_state


DASHBOARD_PROJECTION_WORKER_ID = "dashboard-projection-publisher"
DASHBOARD_RESEARCH_PROJECTION_WORKER_ID = "dashboard-research-projection-publisher"
PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"
RESEARCH_WORKER_ID = "shadow-research-auxiliary"
ALPHA_MECHANISMS = {
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
}
STATE_PRIORITY = {
    "provider_gap": 0,
    "poor_economics": 1,
    "statistical_failure": 2,
    "execution_blocked": 3,
    "settlement_blocked": 4,
    "collecting": 5,
    "certifying": 6,
    "certified": 7,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _max_timestamp(*values: object | None) -> datetime | None:
    parsed = [item for item in (_parse_timestamp(value) for value in values) if item is not None]
    return max(parsed) if parsed else None


def _json_row(raw: object | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _rows(raws: list[object]) -> list[dict[str, Any]]:
    return [payload for raw in raws if (payload := _json_row(raw)) is not None]


def _latest_payload(db, table: str, available: set[str]) -> dict[str, Any] | None:
    if table not in available:
        return None
    raw = db.execute(text(f"SELECT payload_json FROM {table} ORDER BY id DESC LIMIT 1")).scalar_one_or_none()
    return _json_row(raw)


def _history(db, table: str, available: set[str], *, limit: int) -> list[dict[str, Any]]:
    if table not in available:
        return []
    raws = list(db.execute(
        text(f"SELECT payload_json FROM {table} ORDER BY id DESC LIMIT :limit"),
        {"limit": max(1, min(1000, int(limit)))},
    ).scalars())
    return _rows(raws)


def _events(db, event_type: str, available: set[str], *, limit: int) -> list[dict[str, Any]]:
    if "canonical_paper_portfolio_events" not in available:
        return []
    raws = list(db.execute(
        text(
            "SELECT payload_json FROM canonical_paper_portfolio_events "
            "WHERE portfolio_id=:portfolio_id AND event_type=:event_type "
            "ORDER BY id DESC LIMIT :limit"
        ),
        {
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "event_type": event_type,
            "limit": max(1, min(1000, int(limit))),
        },
    ).scalars())
    return _rows(raws)


def _latest_heartbeat(db, worker_id: str, available: set[str]) -> dict[str, Any] | None:
    rows = _heartbeat_history(db, worker_id, available, limit=1)
    return rows[0] if rows else None


def _heartbeat_history(
    db,
    worker_id: str,
    available: set[str],
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if "worker_heartbeats" not in available:
        return []
    raws = list(db.execute(
        text(
            "SELECT payload_json FROM worker_heartbeats "
            "WHERE worker_id=:worker_id ORDER BY id DESC LIMIT :limit"
        ),
        {"worker_id": worker_id, "limit": max(1, min(200, int(limit)))},
    ).scalars())
    return _rows(raws)


def _integer_tail(db, table, available: set[str]) -> tuple[int, datetime | None]:
    if table.name not in available:
        return 0, None
    row = db.execute(
        text(f"SELECT id, observed_at FROM {table.name} ORDER BY id DESC LIMIT 1")
    ).first()
    if row is None:
        return 0, None
    return int(row[0]), _parse_timestamp(row[1])


def _dex_tail(db, store: EvidenceStore, available: set[str]) -> datetime | None:
    table = store.dex_route_quotes
    if table.name not in available:
        return None
    raw = db.execute(
        text(
            f"SELECT observed_at FROM {table.name} "
            "ORDER BY observed_at DESC LIMIT 1"
        )
    ).scalar_one_or_none()
    return _parse_timestamp(raw)


def _performance(latest: dict[str, Any] | None, history: list[dict[str, Any]]) -> dict[str, Any]:
    if latest is None:
        return {
            "available": False,
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
            "paper_only": True,
            "live_execution_authority": False,
        }
    chronological = list(reversed(history[:250]))
    returns: list[float] = []
    for previous, current in zip(chronological, chronological[1:]):
        previous_nav = float(previous.get("nav_usd") or 0.0)
        current_nav = float(current.get("nav_usd") or 0.0)
        if previous_nav > 0:
            returns.append(current_nav / previous_nav - 1.0)
    return {
        "available": True,
        "portfolio_id": latest.get("portfolio_id", CANONICAL_PORTFOLIO_ID),
        "initial_capital_usd": latest.get("initial_capital_usd", CANONICAL_INITIAL_CAPITAL_USD),
        "current_nav_usd": latest.get("nav_usd"),
        "cash_usd": latest.get("cash_usd"),
        "reserved_capital_usd": latest.get("reserved_capital_usd"),
        "realized_pnl_usd": latest.get("realized_pnl_usd"),
        "unrealized_pnl_usd": latest.get("unrealized_pnl_usd"),
        "total_return": latest.get("total_return"),
        "max_drawdown_fraction": latest.get("max_drawdown_fraction"),
        "open_position_count": latest.get("open_position_count", 0),
        "closed_trade_count": latest.get("closed_trade_count", 0),
        "mean_snapshot_return": statistics.fmean(returns) if returns else None,
        "positive_snapshot_rate": sum(value > 0 for value in returns) / len(returns) if returns else None,
        "pnl_by_mechanism_usd": latest.get("pnl_by_mechanism_usd", {}),
        "pnl_by_strategy_usd": latest.get("pnl_by_strategy_usd", {}),
        "paper_only": True,
        "live_execution_authority": False,
    }


def _runtime(
    latest: dict[str, Any] | None,
    integrity: dict[str, Any] | None,
    heartbeat: dict[str, Any] | None,
) -> dict[str, Any]:
    valuation = str((integrity or {}).get("valuation_status") or "unavailable")
    cycle_status = str((integrity or {}).get("cycle_status") or "unavailable")
    worker_state = str((heartbeat or {}).get("state") or "unknown")
    failures = list((integrity or {}).get("allocation_family_failures") or [])
    degraded = (
        worker_state in {"degraded", "error"}
        or cycle_status in {"degraded", "failed"}
        or valuation in {"partial", "stale", "unavailable"}
        or bool(failures)
    )
    return {
        "operational": latest is not None and worker_state not in {"error", "unknown"},
        "degraded": degraded,
        "portfolio_worker_state": worker_state,
        "portfolio_worker_error_type": (heartbeat or {}).get("error_type"),
        "account_snapshot_observed_at": (latest or {}).get("observed_at"),
        "market_evidence_observed_at": (integrity or {}).get("market_evidence_at"),
        "valuation_status": valuation,
        "valuation_fresh": valuation in {"cash_only", "fresh"},
        "cycle_status": cycle_status,
        "fallback_snapshot": bool((integrity or {}).get("fallback_snapshot")),
        "cycle_error_type": (integrity or {}).get("cycle_error_type"),
        "stale_position_count": int((integrity or {}).get("stale_position_count") or 0),
        "settlement_evidence_blocked_count": int(
            (integrity or {}).get("settlement_evidence_blocked_count") or 0
        ),
        "allocation_family_failures": failures,
        "paper_only": True,
        "live_execution_authority": False,
    }


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


def _mechanisms(
    operating: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    research_heartbeat: dict[str, Any] | None,
    *,
    forward_target: int,
    settled_target: int,
) -> dict[str, Any]:
    """Legacy compact mechanism section retained for the portfolio projection."""
    source_rows = list((operating or {}).get("mechanisms") or [])
    funnels = (closure or {}).get("rejection_funnels") or {}
    capabilities = (closure or {}).get("canonical_capabilities") or {}
    provider_admission = (closure or {}).get("provider_admission") or {}
    maker_shadow = (closure or {}).get("maker_shadow") or {}
    capital_location = (closure or {}).get("capital_location_forward") or {}
    research_state = str((research_heartbeat or {}).get("state") or "unknown")
    rows: list[dict[str, Any]] = []
    for source in source_rows:
        if not isinstance(source, dict):
            continue
        row = dict(source)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = research_state in {"starting", "running", "success"}
        row["forward_evidence_persistence_healthy"] = True
        row["forward_evidence_worker_state"] = research_state
        funnel = funnels.get(mechanism_id) if isinstance(funnels, dict) else None
        if isinstance(funnel, dict):
            row["rejection_funnel"] = funnel
            for key in (
                "raw_candidate_count", "emitted_candidate_count", "best_gross_economics",
                "best_cost_economics", "best_net_economics", "required_net_economics",
                "gap_to_hurdle", "economics_unit", "dominant_rejection_gate",
            ):
                if key in funnel:
                    row[key] = funnel[key]
        if isinstance(provider_admission, dict):
            admission = provider_admission.get(mechanism_id)
            if isinstance(admission, dict):
                row["provider_admission"] = admission
        if isinstance(capabilities, dict):
            row["canonical_capabilities"] = capabilities
        if mechanism_id == "liquidity_provision" and isinstance(maker_shadow, dict):
            row["maker_shadow_trial_count"] = int(maker_shadow.get("trial_count") or 0)
            row["maker_shadow_outcome_count"] = int(maker_shadow.get("outcome_count") or 0)
            row["maker_crossed_through_count"] = int(maker_shadow.get("crossed_through_count") or 0)
            row["maker_queue_fill_confirmed_count"] = int(maker_shadow.get("queue_fill_confirmed_count") or 0)
        if mechanism_id == "capital_location_settlement" and isinstance(capital_location, dict):
            row["capital_location_mean_incremental_option_value"] = capital_location.get("mean_incremental_option_value")
            row["capital_location_positive_incremental_rate"] = capital_location.get("positive_incremental_rate")
        rows.append(row)
    return {
        "paper_only": True,
        "count": len(rows),
        "observed_at": (operating or {}).get("observed_at"),
        "version": (operating or {}).get("version"),
        "requirements": {
            "independent_forward_outcomes": max(1, int(forward_target)),
            "settled_allocator_outcomes": max(1, int(settled_target)),
        },
        "live_telemetry": {
            "available": research_heartbeat is not None,
            "worker_id": RESEARCH_WORKER_ID,
            "worker_healthy": research_state in {"starting", "running", "success"},
            "persistence_healthy": True,
            "worker_heartbeat_at": (research_heartbeat or {}).get("observed_at"),
            "research_closure_observed_at": (closure or {}).get("observed_at"),
            "query_mode": "worker_published_dashboard_projection",
        },
        "mechanisms": rows,
    }


def _live_research_mechanisms(
    store: EvidenceStore,
    db,
    available: set[str],
    operating: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    research_heartbeats: list[dict[str, Any]],
    *,
    projection_observed_at: datetime,
    forward_target: int,
    settled_target: int,
    shadow_horizons_seconds: tuple[float, ...],
    shadow_cycle_interval_seconds: float,
    alpha_evidence_every_cycles: int,
    heartbeat_stale_seconds: float,
) -> dict[str, Any]:
    source_rows = list((operating or {}).get("mechanisms") or [])
    funnels = (closure or {}).get("rejection_funnels") or {}
    capabilities = (closure or {}).get("canonical_capabilities") or {}
    provider_admission = (closure or {}).get("provider_admission") or {}
    maker_shadow = (closure or {}).get("maker_shadow") or {}
    capital_location = (closure or {}).get("capital_location_forward") or {}
    if not isinstance(funnels, dict):
        funnels = {}
    if not isinstance(capabilities, dict):
        capabilities = {}
    if not isinstance(provider_admission, dict):
        provider_admission = {}
    if not isinstance(maker_shadow, dict):
        maker_shadow = {}
    if not isinstance(capital_location, dict):
        capital_location = {}

    market_count, market_at = _integer_tail(db, store.market_quotes, available)
    funding_count, funding_at = _integer_tail(db, store.funding_quotes, available)
    order_book_count, order_book_at = _integer_tail(db, store.order_books, available)
    dex_at = _dex_tail(db, store, available)

    heartbeat = research_heartbeats[0] if research_heartbeats else None
    heartbeat_at = _parse_timestamp((heartbeat or {}).get("observed_at"))
    heartbeat_state = str((heartbeat or {}).get("state") or "unknown")
    heartbeat_error = (heartbeat or {}).get("error_type")
    latest_alpha_heartbeat = next(
        (
            row for row in research_heartbeats
            if isinstance(row.get("detail"), dict)
            and (
                row["detail"].get("alpha_forward_evidence_cycle_id")
                or row["detail"].get("alpha_forward_evidence_error_type")
            )
        ),
        None,
    )
    alpha_heartbeat_at = _parse_timestamp((latest_alpha_heartbeat or {}).get("observed_at"))

    max_horizon = max((float(value) for value in shadow_horizons_seconds), default=60.0)
    configured_interval = max(1.0, float(shadow_cycle_interval_seconds))
    core_expected_interval = max(1.0, max_horizon + configured_interval)
    alpha_every = max(1, int(alpha_evidence_every_cycles))
    alpha_expected_interval = core_expected_interval * alpha_every
    stale_after = max(float(heartbeat_stale_seconds), core_expected_interval * 3.0)

    worker_healthy: bool | None = None
    if heartbeat_at is not None:
        age = max(0.0, (projection_observed_at - heartbeat_at).total_seconds())
        worker_healthy = heartbeat_state in {"starting", "running", "success"} and age <= stale_after

    newest_authoritative = _max_timestamp(market_at, funding_at, order_book_at, dex_at)
    core_collection_at = _max_timestamp(newest_authoritative, heartbeat_at)
    rows: list[dict[str, Any]] = []
    for source in source_rows:
        if not isinstance(source, dict):
            continue
        row = dict(source)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = worker_healthy
        row["forward_evidence_persistence_healthy"] = True
        row["research_projection_observed_at"] = projection_observed_at.isoformat()

        existing_count = int(row.get("authoritative_observation_count") or 0)
        live_count: int | None = None
        authoritative_at: datetime | None = None
        if mechanism_id == "price_discrepancy":
            live_count = max(existing_count, market_count)
            authoritative_at = _max_timestamp(market_at, dex_at)
        elif mechanism_id == "carry":
            live_count = max(existing_count, market_count + funding_count)
            authoritative_at = _max_timestamp(market_at, funding_at)
        elif mechanism_id in {"trend_momentum", "mean_reversion", "cross_sectional_relative_value"}:
            live_count = max(existing_count, market_count)
            authoritative_at = market_at
        elif mechanism_id == "microstructure":
            live_count = max(existing_count, market_count + order_book_count)
            authoritative_at = _max_timestamp(market_at, order_book_at)
        elif mechanism_id == "liquidity_provision":
            live_count = max(existing_count, order_book_count)
            authoritative_at = order_book_at
        if live_count is not None:
            row["authoritative_observation_count"] = live_count
        if authoritative_at is not None:
            row["authoritative_observation_last_at"] = authoritative_at.isoformat()

        if mechanism_id in ALPHA_MECHANISMS:
            last_cycle = _max_timestamp(row.get("forward_evidence_last_cycle_at"), alpha_heartbeat_at)
            expected = alpha_expected_interval
            research_error = heartbeat_error
            if latest_alpha_heartbeat is not None:
                detail = latest_alpha_heartbeat.get("detail") or {}
                if isinstance(detail, dict) and detail.get("alpha_forward_evidence_error_type"):
                    research_error = str(detail["alpha_forward_evidence_error_type"])
        else:
            last_cycle = core_collection_at
            expected = core_expected_interval
            research_error = heartbeat_error
        if last_cycle is not None:
            row["forward_evidence_last_cycle_at"] = last_cycle.isoformat()
            row["forward_evidence_next_expected_at"] = (
                last_cycle + timedelta(seconds=expected)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = expected

        row["forward_evidence_worker_state"] = classify_research_worker_state(
            now=projection_observed_at,
            heartbeat_at=heartbeat_at,
            heartbeat_state=heartbeat_state,
            error_type=str(research_error) if research_error else None,
            last_cycle_at=last_cycle,
            expected_interval_seconds=expected,
            stale_after_seconds=max(stale_after, expected * 3.0),
        )

        funnel = funnels.get(mechanism_id)
        if isinstance(funnel, dict):
            row["rejection_funnel"] = funnel
            for key in (
                "raw_candidate_count", "emitted_candidate_count", "best_gross_economics",
                "best_cost_economics", "best_net_economics", "required_net_economics",
                "gap_to_hurdle", "economics_unit", "dominant_rejection_gate",
            ):
                if key in funnel:
                    row[key] = funnel[key]

        if mechanism_id == "capital_location_settlement" and capital_location:
            row["stage"] = "forward_testable"
            row["forward_signal_count"] = int(capital_location.get("trial_count") or 0)
            row["independent_forward_outcome_count"] = int(capital_location.get("outcome_count") or 0)
            row["capital_location_mean_incremental_option_value"] = capital_location.get("mean_incremental_option_value")
            row["capital_location_positive_incremental_rate"] = capital_location.get("positive_incremental_rate")
        if mechanism_id == "liquidity_provision" and maker_shadow:
            row["maker_shadow_trial_count"] = int(maker_shadow.get("trial_count") or 0)
            row["maker_shadow_outcome_count"] = int(maker_shadow.get("outcome_count") or 0)
            row["maker_crossed_through_count"] = int(maker_shadow.get("crossed_through_count") or 0)
            row["maker_queue_fill_confirmed_count"] = int(maker_shadow.get("queue_fill_confirmed_count") or 0)
            row["maker_adverse_selection_observation_count"] = int(
                maker_shadow.get("adverse_selection_observation_count") or 0
            )
        admission = provider_admission.get(mechanism_id)
        if isinstance(admission, dict):
            row["provider_admission"] = admission
        row["canonical_capabilities"] = capabilities
        row["blockers"] = _reconciled_blockers(list(row.get("blockers") or []), capabilities)
        rows.append(row)

    return {
        "paper_only": True,
        "count": len(rows),
        "observed_at": projection_observed_at.isoformat(),
        "source_operating_observed_at": (operating or {}).get("observed_at"),
        "version": (operating or {}).get("version"),
        "requirements": {
            "independent_forward_outcomes": max(1, int(forward_target)),
            "settled_allocator_outcomes": max(1, int(settled_target)),
        },
        "live_telemetry": {
            "available": heartbeat_at is not None or newest_authoritative is not None,
            "worker_id": RESEARCH_WORKER_ID,
            "worker_healthy": worker_healthy,
            "persistence_healthy": True,
            "worker_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
            "latest_collection_at": core_collection_at.isoformat() if core_collection_at is not None else None,
            "latest_authoritative_observation_at": (
                newest_authoritative.isoformat() if newest_authoritative is not None else None
            ),
            "research_closure_observed_at": (closure or {}).get("observed_at"),
            "durable_counts": {
                "market": market_count,
                "funding": funding_count,
                "order_book": order_book_count,
                "dex_route": None,
            },
            "query_mode": "research_worker_projection_primary_key_tails_plus_compact_closure",
        },
        "mechanisms": rows,
    }


def _action_queue(mechanism_payload: dict[str, Any]) -> dict[str, Any]:
    queue_rows = [
        row for row in mechanism_payload.get("mechanisms", [])
        if isinstance(row, dict) and row.get("state") != "certified"
    ]
    queue_rows.sort(key=lambda row: (
        STATE_PRIORITY.get(str(row.get("state") or ""), 99),
        str(row.get("mechanism_id") or ""),
    ))
    return {
        "paper_only": True,
        "count": len(queue_rows),
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
            for row in queue_rows
        ],
    }


def build_dashboard_projection(
    store: EvidenceStore,
    *,
    forward_target: int = 30,
    settled_target: int = 20,
) -> dict[str, Any]:
    """Build the portfolio-led compact command-center snapshot."""
    available = set(inspect(store.engine).get_table_names())
    with store.engine.begin() as db:
        if store.backend == "postgresql":
            db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
            db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
        latest = _latest_payload(db, "canonical_paper_portfolio_snapshots", available)
        history = _history(db, "canonical_paper_portfolio_snapshots", available, limit=500)
        integrity = _latest_payload(db, "canonical_paper_portfolio_integrity", available)
        portfolio_heartbeat = _latest_heartbeat(db, PORTFOLIO_WORKER_ID, available)
        research_heartbeat = _latest_heartbeat(db, RESEARCH_WORKER_ID, available)
        trades = _events(db, "close", available, limit=20)
        skips = _events(db, "skip", available, limit=20)
        operating = _latest_payload(db, "operating_certification_snapshots", available)
        closure = _latest_payload(db, "research_closure_cycle_summaries", available)

    mechanism_payload = _mechanisms(
        operating,
        closure,
        research_heartbeat,
        forward_target=forward_target,
        settled_target=settled_target,
    )
    now = _now().isoformat()
    portfolio = ({**latest, "available": True} if latest is not None else {
        "available": False,
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
        "paper_only": True,
    })
    return {
        "projection_version": 1,
        "projection_kind": "portfolio",
        "observed_at": now,
        "source_portfolio_observed_at": (latest or {}).get("observed_at"),
        "source_integrity_observed_at": (integrity or {}).get("observed_at"),
        "source_operating_observed_at": (operating or {}).get("observed_at"),
        "source_research_closure_observed_at": (closure or {}).get("observed_at"),
        "paper_only": True,
        "live_execution_authority": False,
        "portfolio": portfolio,
        "performance": _performance(latest, history),
        "runtime": _runtime(latest, integrity, portfolio_heartbeat),
        "positions": {"positions": list((latest or {}).get("positions") or [])},
        "trades": {"trades": trades},
        "history": {"count": len(history), "snapshots": history},
        "skips": {"skips": skips},
        "attribution": {
            "pnl_by_mechanism_usd": dict((latest or {}).get("pnl_by_mechanism_usd") or {}),
            "pnl_by_strategy_usd": dict((latest or {}).get("pnl_by_strategy_usd") or {}),
        },
        "mechanisms": mechanism_payload,
        "queue": _action_queue(mechanism_payload),
    }


def build_dashboard_research_projection(
    store: EvidenceStore,
    *,
    forward_target: int = 30,
    settled_target: int = 20,
    shadow_horizons_seconds: tuple[float, ...] = (60.0,),
    shadow_cycle_interval_seconds: float = 30.0,
    alpha_evidence_every_cycles: int = 10,
    heartbeat_stale_seconds: float = 180.0,
) -> dict[str, Any]:
    """Build live research progress independently of the portfolio cadence."""
    available = set(inspect(store.engine).get_table_names())
    observed_at = _now()
    with store.engine.begin() as db:
        if store.backend == "postgresql":
            db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
            db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
        operating = _latest_payload(db, "operating_certification_snapshots", available)
        closure = _latest_payload(db, "research_closure_cycle_summaries", available)
        heartbeats = _heartbeat_history(db, RESEARCH_WORKER_ID, available, limit=50)
        mechanism_payload = _live_research_mechanisms(
            store,
            db,
            available,
            operating,
            closure,
            heartbeats,
            projection_observed_at=observed_at,
            forward_target=forward_target,
            settled_target=settled_target,
            shadow_horizons_seconds=tuple(shadow_horizons_seconds or (60.0,)),
            shadow_cycle_interval_seconds=shadow_cycle_interval_seconds,
            alpha_evidence_every_cycles=alpha_evidence_every_cycles,
            heartbeat_stale_seconds=heartbeat_stale_seconds,
        )
    heartbeat = heartbeats[0] if heartbeats else None
    return {
        "projection_version": 2,
        "projection_kind": "research",
        "observed_at": observed_at.isoformat(),
        "source_operating_observed_at": (operating or {}).get("observed_at"),
        "source_research_closure_observed_at": (closure or {}).get("observed_at"),
        "source_research_heartbeat_at": (heartbeat or {}).get("observed_at"),
        "paper_only": True,
        "live_execution_authority": False,
        "mechanisms": mechanism_payload,
        "queue": _action_queue(mechanism_payload),
    }


class DashboardProjectionLedger:
    """Append-only portfolio-led compact projection owned by the worker process."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "dashboard_projection_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observed_at", Text, nullable=False),
            Column("source_portfolio_observed_at", Text),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", Text, nullable=False),
        )
        Index("ix_dashboard_projection_observed", self.rows.c.observed_at)
        metadata.create_all(store.engine)

    def publish(
        self,
        *,
        forward_target: int = 30,
        settled_target: int = 20,
    ) -> dict[str, Any]:
        payload = build_dashboard_projection(
            self.store,
            forward_target=forward_target,
            settled_target=settled_target,
        )
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self.store.engine.begin() as db:
            if self.store.backend == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
                db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
            db.execute(insert(self.rows), {
                "observed_at": payload["observed_at"],
                "source_portfolio_observed_at": payload.get("source_portfolio_observed_at"),
                "payload_json": raw,
                "lineage_hash": hashlib.sha256(raw.encode()).hexdigest(),
            })
        return payload

    def latest(self) -> dict[str, Any] | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                text("SELECT payload_json FROM dashboard_projection_snapshots ORDER BY id DESC LIMIT 1")
            ).scalar_one_or_none()
        return _json_row(raw)


class ResearchDashboardProjectionLedger:
    """Append-only research-card projection refreshed by every successful research cycle."""

    def __init__(self, store: EvidenceStore):
        self.store = store
        metadata = MetaData()
        self.rows = Table(
            "dashboard_research_projection_snapshots",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("observed_at", Text, nullable=False),
            Column("source_research_heartbeat_at", Text),
            Column("payload_json", Text, nullable=False),
            Column("lineage_hash", Text, nullable=False),
        )
        Index("ix_dashboard_research_projection_observed", self.rows.c.observed_at)
        metadata.create_all(store.engine)

    def publish(
        self,
        *,
        forward_target: int = 30,
        settled_target: int = 20,
        shadow_horizons_seconds: tuple[float, ...] = (60.0,),
        shadow_cycle_interval_seconds: float = 30.0,
        alpha_evidence_every_cycles: int = 10,
        heartbeat_stale_seconds: float = 180.0,
    ) -> dict[str, Any]:
        payload = build_dashboard_research_projection(
            self.store,
            forward_target=forward_target,
            settled_target=settled_target,
            shadow_horizons_seconds=shadow_horizons_seconds,
            shadow_cycle_interval_seconds=shadow_cycle_interval_seconds,
            alpha_evidence_every_cycles=alpha_evidence_every_cycles,
            heartbeat_stale_seconds=heartbeat_stale_seconds,
        )
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self.store.engine.begin() as db:
            if self.store.backend == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '2500ms'"))
                db.execute(text("SET LOCAL lock_timeout = '1000ms'"))
            db.execute(insert(self.rows), {
                "observed_at": payload["observed_at"],
                "source_research_heartbeat_at": payload.get("source_research_heartbeat_at"),
                "payload_json": raw,
                "lineage_hash": hashlib.sha256(raw.encode()).hexdigest(),
            })
        return payload

    def latest(self) -> dict[str, Any] | None:
        with self.store.engine.connect() as db:
            raw = db.execute(
                text(
                    "SELECT payload_json FROM dashboard_research_projection_snapshots "
                    "ORDER BY id DESC LIMIT 1"
                )
            ).scalar_one_or_none()
        return _json_row(raw)
