from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from sqlalchemy import func, inspect, select, text

from inefficiency_engine import __version__
from inefficiency_engine.config import Settings
from inefficiency_engine.dashboard_resilience import build_dashboard_router
from inefficiency_engine.evidence import EvidenceStore, build_evidence_store


CANONICAL_PORTFOLIO_ID = "crypto-opportunity-engine-paper-portfolio"
CANONICAL_INITIAL_CAPITAL_USD = 250_000.0
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
ALPHA_MECHANISMS = {
    "trend_momentum",
    "mean_reversion",
    "fundamental_onchain",
    "cross_sectional_relative_value",
    "event_driven",
    "microstructure",
}

settings = Settings.from_env()
evidence_store = build_evidence_store(settings.evidence_db_path)
app = FastAPI(title="Crypto Inefficiency Engine Read Plane", version=__version__)
app.include_router(build_dashboard_router())


def _require_store() -> EvidenceStore:
    if evidence_store is None:
        raise HTTPException(status_code=503, detail="evidence persistence is not configured")
    return evidence_store


def _has_table(store: EvidenceStore, name: str) -> bool:
    try:
        return bool(inspect(store.engine).has_table(name))
    except Exception:
        return False


def _json_row(raw: object | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _latest_payload(store: EvidenceStore, table: str) -> dict[str, Any] | None:
    if not _has_table(store, table):
        return None
    with store.engine.connect() as db:
        raw = db.execute(text(f"SELECT payload_json FROM {table} ORDER BY id DESC LIMIT 1")).scalar_one_or_none()
    return _json_row(raw)


def _payload_history(store: EvidenceStore, table: str, *, limit: int) -> list[dict[str, Any]]:
    if not _has_table(store, table):
        return []
    bounded = max(1, min(1000, int(limit)))
    with store.engine.connect() as db:
        raws = list(db.execute(
            text(f"SELECT payload_json FROM {table} ORDER BY id DESC LIMIT :limit"),
            {"limit": bounded},
        ).scalars())
    return [payload for raw in raws if (payload := _json_row(raw)) is not None]


def _portfolio_events(store: EvidenceStore, event_type: str, *, limit: int) -> list[dict[str, Any]]:
    table = "canonical_paper_portfolio_events"
    if not _has_table(store, table):
        return []
    bounded = max(1, min(1000, int(limit)))
    with store.engine.connect() as db:
        raws = list(db.execute(
            text(
                "SELECT payload_json FROM canonical_paper_portfolio_events "
                "WHERE portfolio_id=:portfolio_id AND event_type=:event_type "
                "ORDER BY id DESC LIMIT :limit"
            ),
            {
                "portfolio_id": CANONICAL_PORTFOLIO_ID,
                "event_type": event_type,
                "limit": bounded,
            },
        ).scalars())
    return [payload for raw in raws if (payload := _json_row(raw)) is not None]


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


def _evidence_requirements() -> dict[str, int]:
    return {
        "independent_forward_outcomes": max(1, int(getattr(settings, "alpha_min_forward_samples", 30))),
        "settled_allocator_outcomes": max(
            5,
            int(getattr(settings, "operating_certification_min_settled_trials", 20)),
        ),
    }


def _live_mechanism_overlay(
    store: EvidenceStore,
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = datetime.now(timezone.utc)
    persistence_healthy = False
    try:
        persistence_healthy = bool(store.ping())
    except Exception:
        pass

    counts = {"market": 0, "funding": 0, "order_book": 0, "dex_route": 0}
    latest: dict[str, datetime | None] = {
        "market": None,
        "funding": None,
        "order_book": None,
        "dex_route": None,
        "scan": None,
    }
    heartbeat_payloads: list[str] = []
    try:
        with store.engine.connect() as db:
            counts["market"] = int(db.execute(select(func.count()).select_from(store.market_quotes)).scalar_one())
            counts["funding"] = int(db.execute(select(func.count()).select_from(store.funding_quotes)).scalar_one())
            counts["order_book"] = int(db.execute(select(func.count()).select_from(store.order_books)).scalar_one())
            counts["dex_route"] = int(db.execute(select(func.count()).select_from(store.dex_route_quotes)).scalar_one())
            latest["market"] = _parse_timestamp(db.execute(select(func.max(store.market_quotes.c.observed_at))).scalar_one_or_none())
            latest["funding"] = _parse_timestamp(db.execute(select(func.max(store.funding_quotes.c.observed_at))).scalar_one_or_none())
            latest["order_book"] = _parse_timestamp(db.execute(select(func.max(store.order_books.c.observed_at))).scalar_one_or_none())
            latest["dex_route"] = _parse_timestamp(db.execute(select(func.max(store.dex_route_quotes.c.observed_at))).scalar_one_or_none())
            latest["scan"] = _parse_timestamp(db.execute(select(func.max(store.scans.c.completed_at))).scalar_one_or_none())
            heartbeat_payloads = list(db.execute(
                select(store.worker_heartbeats.c.payload_json)
                .where(store.worker_heartbeats.c.worker_id == RESEARCH_WORKER_ID)
                .order_by(store.worker_heartbeats.c.id.desc())
                .limit(200)
            ).scalars())
    except Exception:
        heartbeat_payloads = []

    latest_heartbeat: dict[str, Any] | None = None
    latest_success: dict[str, Any] | None = None
    latest_alpha_cycle: dict[str, Any] | None = None
    for raw in heartbeat_payloads:
        payload = _json_row(raw)
        if payload is None:
            continue
        if latest_heartbeat is None:
            latest_heartbeat = payload
        if latest_success is None and payload.get("state") == "success":
            latest_success = payload
        detail = payload.get("detail") or {}
        if latest_alpha_cycle is None and isinstance(detail, dict) and "alpha_forward_evidence_cycle_id" in detail:
            latest_alpha_cycle = payload
        if latest_heartbeat is not None and latest_success is not None and latest_alpha_cycle is not None:
            break

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

    heartbeat_at = _parse_timestamp((latest_heartbeat or {}).get("observed_at"))
    heartbeat_state = str((latest_heartbeat or {}).get("state") or "")
    worker_healthy: bool | None = None
    if heartbeat_at is not None:
        worker_healthy = (
            heartbeat_state in {"starting", "running", "success"}
            and max(0.0, (now - heartbeat_at).total_seconds()) <= stale_after
        )

    last_success_at = _parse_timestamp((latest_success or {}).get("observed_at"))
    core_collection_at = _max_timestamp(latest["scan"], last_success_at)
    alpha_cycle_at = _parse_timestamp((latest_alpha_cycle or {}).get("observed_at"))

    live_rows: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = worker_healthy
        row["forward_evidence_persistence_healthy"] = persistence_healthy

        authoritative_count: int | None = None
        authoritative_at: datetime | None = None
        if mechanism_id == "price_discrepancy":
            authoritative_count = counts["market"] + counts["dex_route"]
            authoritative_at = _max_timestamp(latest["market"], latest["dex_route"])
        elif mechanism_id == "carry":
            authoritative_count = counts["market"] + counts["funding"]
            authoritative_at = _max_timestamp(latest["market"], latest["funding"])
        elif mechanism_id in {"trend_momentum", "mean_reversion", "cross_sectional_relative_value"}:
            authoritative_count = counts["market"]
            authoritative_at = latest["market"]
        elif mechanism_id == "microstructure":
            authoritative_count = counts["market"] + counts["order_book"]
            authoritative_at = _max_timestamp(latest["market"], latest["order_book"])
        elif mechanism_id == "liquidity_provision":
            authoritative_count = counts["order_book"]
            authoritative_at = latest["order_book"]

        if authoritative_count is not None:
            row["authoritative_observation_count"] = max(
                int(row.get("authoritative_observation_count") or 0),
                authoritative_count,
            )
        if authoritative_at is not None:
            row["authoritative_observation_last_at"] = authoritative_at.isoformat()

        if mechanism_id in ALPHA_MECHANISMS and alpha_cycle_at is not None:
            row["forward_evidence_last_cycle_at"] = alpha_cycle_at.isoformat()
            row["forward_evidence_next_expected_at"] = (
                alpha_cycle_at + timedelta(seconds=alpha_expected_interval)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = alpha_expected_interval
        elif core_collection_at is not None:
            row["forward_evidence_last_cycle_at"] = core_collection_at.isoformat()
            row["forward_evidence_next_expected_at"] = (
                core_collection_at + timedelta(seconds=core_expected_interval)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = core_expected_interval
        live_rows.append(row)

    newest_authoritative = _max_timestamp(
        latest["market"], latest["funding"], latest["order_book"], latest["dex_route"]
    )
    telemetry = {
        "available": heartbeat_at is not None or newest_authoritative is not None,
        "worker_id": RESEARCH_WORKER_ID,
        "worker_healthy": worker_healthy,
        "persistence_healthy": persistence_healthy,
        "worker_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "latest_collection_at": core_collection_at.isoformat() if core_collection_at is not None else None,
        "latest_authoritative_observation_at": (
            newest_authoritative.isoformat() if newest_authoritative is not None else None
        ),
        "durable_counts": counts,
    }
    return live_rows, telemetry


@app.get("/health")
def health():
    payload: dict[str, Any] = {
        "status": "ok",
        "version": __version__,
        "paper_only": True,
        "read_plane": True,
        "live_execution": False,
        "evidence_persistence": evidence_store is not None,
    }
    if evidence_store is not None:
        payload["evidence_backend"] = evidence_store.backend
        payload["database_ok"] = evidence_store.ping()
    return payload


@app.get("/v3/portfolio/canonical")
def canonical_portfolio():
    store = _require_store()
    latest = _latest_payload(store, "canonical_paper_portfolio_snapshots")
    if latest is None:
        return {
            "available": False,
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
            "paper_only": True,
            "message": "canonical paper portfolio is awaiting its first worker cycle",
        }
    return {**latest, "available": True}


@app.get("/v3/portfolio/performance")
def canonical_portfolio_performance():
    store = _require_store()
    latest = _latest_payload(store, "canonical_paper_portfolio_snapshots")
    if latest is None:
        return {
            "available": False,
            "portfolio_id": CANONICAL_PORTFOLIO_ID,
            "initial_capital_usd": CANONICAL_INITIAL_CAPITAL_USD,
            "paper_only": True,
        }
    history = list(reversed(_payload_history(store, "canonical_paper_portfolio_snapshots", limit=250)))
    returns: list[float] = []
    for previous, current in zip(history, history[1:]):
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


@app.get("/v3/portfolio/runtime-status")
def canonical_portfolio_runtime_status():
    store = _require_store()
    now = datetime.now(timezone.utc)
    account = _latest_payload(store, "canonical_paper_portfolio_snapshots")
    integrity = _latest_payload(store, "canonical_paper_portfolio_integrity")
    heartbeat = store.latest_worker_heartbeat(PORTFOLIO_WORKER_ID)
    expected_interval = max(60.0, float(getattr(settings, "shadow_cycle_interval_seconds", 30.0)) * 10.0)
    stale_after = max(600.0, expected_interval * 2.5)

    account_at = _parse_timestamp((account or {}).get("observed_at"))
    market_at = _parse_timestamp((integrity or {}).get("market_evidence_at"))
    account_age = max(0.0, (now - account_at).total_seconds()) if account_at is not None else None
    market_age = max(0.0, (now - market_at).total_seconds()) if market_at is not None else None
    heartbeat_age = (
        max(0.0, (now - heartbeat.observed_at).total_seconds()) if heartbeat is not None else None
    )
    heartbeat_recent = bool(heartbeat is not None and heartbeat_age is not None and heartbeat_age <= stale_after)
    accounting_fresh = bool(account is not None and account_age is not None and account_age <= stale_after)
    valuation_status = str((integrity or {}).get("valuation_status") or "unavailable")
    valuation_fresh = bool(
        integrity is not None
        and (
            valuation_status == "cash_only"
            or (valuation_status == "fresh" and market_age is not None and market_age <= stale_after)
        )
    )
    cycle_status = (integrity or {}).get("cycle_status")
    cycle_failed = cycle_status == "failed"
    operational = bool(
        heartbeat_recent
        and heartbeat is not None
        and heartbeat.state not in {"error", "stopped"}
        and accounting_fresh
        and valuation_fresh
        and not cycle_failed
    )
    degraded = bool(
        (heartbeat is not None and heartbeat.state == "degraded")
        or cycle_status == "degraded"
        or (account is not None and not valuation_fresh)
    )
    return {
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "paper_only": True,
        "operational": operational,
        "degraded": degraded,
        "expected_cycle_interval_seconds": expected_interval,
        "stale_after_seconds": stale_after,
        "snapshot_fresh": accounting_fresh,
        "accounting_snapshot_fresh": accounting_fresh,
        "snapshot_age_seconds": account_age,
        "latest_snapshot_observed_at": (account or {}).get("observed_at"),
        "valuation_status": valuation_status,
        "valuation_fresh": valuation_fresh,
        "market_evidence_observed_at": (integrity or {}).get("market_evidence_at"),
        "market_evidence_age_seconds": market_age,
        "cycle_status": cycle_status,
        "fallback_snapshot": bool((integrity or {}).get("fallback_snapshot", False)),
        "cycle_error_type": (integrity or {}).get("cycle_error_type"),
        "stale_position_count": (integrity or {}).get("stale_position_count"),
        "settlement_evidence_blocked_count": (integrity or {}).get("settlement_evidence_blocked_count", 0),
        "allocation_family_failures": (integrity or {}).get("allocation_family_failures", []),
        "market_snapshot_id": (integrity or {}).get("market_snapshot_id"),
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat": heartbeat.model_dump(mode="json") if heartbeat is not None else None,
    }


@app.get("/v3/portfolio/positions")
def canonical_portfolio_positions():
    latest = _latest_payload(_require_store(), "canonical_paper_portfolio_snapshots")
    positions = list((latest or {}).get("positions") or [])
    return {"portfolio_id": CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": len(positions), "positions": positions}


@app.get("/v3/portfolio/trades")
def canonical_portfolio_trades(limit: int = 100):
    rows = _portfolio_events(_require_store(), "close", limit=limit)
    return {"portfolio_id": CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": len(rows), "trades": rows}


@app.get("/v3/portfolio/skips")
def canonical_portfolio_skips(limit: int = 100):
    rows = _portfolio_events(_require_store(), "skip", limit=limit)
    return {"portfolio_id": CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": len(rows), "skips": rows}


@app.get("/v3/portfolio/history")
def canonical_portfolio_history(limit: int = 100):
    rows = _payload_history(_require_store(), "canonical_paper_portfolio_snapshots", limit=limit)
    return {"portfolio_id": CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": len(rows), "snapshots": rows}


@app.get("/v3/portfolio/integrity/history")
def canonical_portfolio_integrity_history(limit: int = 100):
    rows = _payload_history(_require_store(), "canonical_paper_portfolio_integrity", limit=limit)
    return {"portfolio_id": CANONICAL_PORTFOLIO_ID, "paper_only": True, "count": len(rows), "integrity": rows}


@app.get("/v3/portfolio/attribution")
def canonical_portfolio_attribution():
    latest = _latest_payload(_require_store(), "canonical_paper_portfolio_snapshots")
    return {
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "paper_only": True,
        "pnl_by_mechanism_usd": (latest or {}).get("pnl_by_mechanism_usd", {}),
        "pnl_by_strategy_usd": (latest or {}).get("pnl_by_strategy_usd", {}),
    }


@app.get("/v3/operations/certification/latest")
def operating_certification_latest():
    latest = _latest_payload(_require_store(), "operating_certification_snapshots")
    if latest is None:
        return {"available": False, "paper_only": True, "message": "no operating certification cycle has been recorded yet"}
    return {**latest, "available": True}


@app.get("/v3/operations/certification/history")
def operating_certification_history(limit: int = 50):
    rows = _payload_history(_require_store(), "operating_certification_snapshots", limit=limit)
    return {"paper_only": True, "count": len(rows), "snapshots": rows}


@app.get("/v3/operations/certification/summary")
def operating_certification_summary():
    store = _require_store()
    rows = _payload_history(store, "operating_certification_snapshots", limit=500)
    return {
        "snapshot_count": len(rows),
        "latest": rows[0] if rows else None,
        "paper_only": True,
        "live_execution_authority": False,
    }


@app.get("/v3/operations/mechanisms")
def operating_mechanisms():
    store = _require_store()
    latest = _latest_payload(store, "operating_certification_snapshots")
    requirements = _evidence_requirements()
    if latest is None:
        telemetry = {
            "available": False,
            "worker_id": RESEARCH_WORKER_ID,
            "worker_healthy": None,
            "persistence_healthy": bool(store.ping()),
        }
        return {
            "paper_only": True,
            "count": 0,
            "observed_at": None,
            "requirements": requirements,
            "live_telemetry": telemetry,
            "mechanisms": [],
        }
    rows = [dict(row) for row in list(latest.get("mechanisms") or []) if isinstance(row, dict)]
    mechanisms, telemetry = _live_mechanism_overlay(store, rows)
    return {
        "paper_only": True,
        "count": len(mechanisms),
        "observed_at": latest.get("observed_at"),
        "version": latest.get("version"),
        "requirements": requirements,
        "live_telemetry": telemetry,
        "mechanisms": mechanisms,
    }


@app.get("/v3/operations/action-queue")
def operating_action_queue():
    latest = _latest_payload(_require_store(), "operating_certification_snapshots")
    if latest is None:
        return {"paper_only": True, "count": 0, "actions": []}
    rows = [dict(row) for row in list(latest.get("mechanisms") or []) if isinstance(row, dict)]
    rows = [row for row in rows if row.get("state") != "certified"]
    rows.sort(key=lambda row: (STATE_PRIORITY.get(str(row.get("state")), 99), str(row.get("mechanism_id") or "")))
    actions = [
        {
            "mechanism_id": row.get("mechanism_id"),
            "name": row.get("name"),
            "state": row.get("state"),
            "primary_reason": row.get("primary_reason"),
            "next_action": row.get("next_action"),
            "blockers": row.get("blockers", []),
        }
        for row in rows
    ]
    return {"paper_only": True, "count": len(actions), "actions": actions}


@app.get("/v1/worker/health")
def worker_health():
    store = _require_store()
    return store.worker_health(stale_after_seconds=float(getattr(settings, "worker_heartbeat_stale_seconds", 180.0)))


@app.get("/v1/evidence/counts")
def evidence_counts():
    return _require_store().counts().__dict__
