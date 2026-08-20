from __future__ import annotations

import hashlib
import json
import statistics
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Column, Index, Integer, MetaData, Table, Text, insert, inspect, text

from inefficiency_engine.canonical_paper_portfolio import (
    CANONICAL_INITIAL_CAPITAL_USD,
    CANONICAL_PORTFOLIO_ID,
)
from inefficiency_engine.evidence import EvidenceStore


DASHBOARD_PROJECTION_WORKER_ID = "dashboard-projection-publisher"
PORTFOLIO_WORKER_ID = "canonical-portfolio-operating-loop"
RESEARCH_WORKER_ID = "shadow-research-auxiliary"
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
    if "worker_heartbeats" not in available:
        return None
    raw = db.execute(
        text(
            "SELECT payload_json FROM worker_heartbeats "
            "WHERE worker_id=:worker_id ORDER BY id DESC LIMIT 1"
        ),
        {"worker_id": worker_id},
    ).scalar_one_or_none()
    return _json_row(raw)


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


def _mechanisms(
    operating: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    research_heartbeat: dict[str, Any] | None,
    *,
    forward_target: int,
    settled_target: int,
) -> dict[str, Any]:
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


def build_dashboard_projection(
    store: EvidenceStore,
    *,
    forward_target: int = 30,
    settled_target: int = 20,
) -> dict[str, Any]:
    """Build one compact, point-in-time dashboard view on the worker side."""
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
    queue_rows = [
        row for row in mechanism_payload["mechanisms"]
        if row.get("state") != "certified"
    ]
    queue_rows.sort(key=lambda row: (
        STATE_PRIORITY.get(str(row.get("state") or ""), 99),
        str(row.get("mechanism_id") or ""),
    ))
    queue = {
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
    now = _now().isoformat()
    portfolio = ({**latest, "available": True} if latest is not None else {
        "available": False,
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
        "paper_only": True,
    })
    return {
        "projection_version": 1,
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
        "queue": queue,
    }


class DashboardProjectionLedger:
    """Append-only compact projection owned exclusively by the worker process."""

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
