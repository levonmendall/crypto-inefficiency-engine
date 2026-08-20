from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from inefficiency_engine.read_api import (
    ALPHA_MECHANISMS,
    RESEARCH_WORKER_ID,
    _evidence_requirements,
    _latest_payload,
    _max_timestamp,
    _parse_timestamp,
    _require_store,
    app,
    settings,
)


# v3.5.24 made the production web process read-only, but its mechanism-card overlay
# still executed COUNT(*) and MAX() across the growing evidence tables every 30s.
# This replacement reads only append-only primary-key tails plus one indexed DEX
# timestamp and the latest worker heartbeat. Query cost therefore stays essentially
# constant as historical evidence grows.


def _integer_tail(table) -> tuple[int, datetime | None]:
    """Return exact append-only row count and latest timestamp without a table scan.

    The evidence ledgers never delete rows and their integer primary keys are
    autoincrementing from 1, so the largest id is the exact persisted row count.
    """

    store = _require_store()
    with store.engine.connect() as db:
        row = db.execute(
            select(table.c.id, table.c.observed_at).order_by(table.c.id.desc()).limit(1)
        ).first()
    if row is None:
        return 0, None
    return int(row[0]), _parse_timestamp(row[1])


def _dex_tail() -> datetime | None:
    """Read only the indexed newest DEX observation; never COUNT the route ledger."""

    store = _require_store()
    with store.engine.connect() as db:
        raw = db.execute(
            select(store.dex_route_quotes.c.observed_at)
            .order_by(store.dex_route_quotes.c.observed_at.desc())
            .limit(1)
        ).scalar_one_or_none()
    return _parse_timestamp(raw)


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

    # Exact cumulative counts for append-only integer-id ledgers are O(1)/index-tail
    # reads. DEX route records use UUID primary keys, so we deliberately avoid a full
    # COUNT scan; the last persisted certification count remains the monotonic floor
    # while the current indexed DEX timestamp proves collection freshness.
    market_count, market_at = _integer_tail(store.market_quotes)
    funding_count, funding_at = _integer_tail(store.funding_quotes)
    order_book_count, order_book_at = _integer_tail(store.order_books)
    dex_at = _dex_tail()

    heartbeat = None
    try:
        heartbeat = store.latest_worker_heartbeat(RESEARCH_WORKER_ID)
    except Exception:
        heartbeat = None

    horizons = tuple(getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,))
    max_horizon = max((float(value) for value in horizons), default=60.0)
    configured_interval = max(
        1.0,
        float(getattr(settings, "shadow_cycle_interval_seconds", 30.0)),
    )
    core_expected_interval = max(1.0, max_horizon + configured_interval)
    alpha_every = max(1, int(getattr(settings, "alpha_evidence_every_cycles", 10)))
    alpha_expected_interval = core_expected_interval * alpha_every
    stale_after = max(
        float(getattr(settings, "worker_heartbeat_stale_seconds", 180.0)),
        core_expected_interval * 3.0,
    )

    heartbeat_at = heartbeat.observed_at if heartbeat is not None else None
    worker_healthy: bool | None = None
    if heartbeat_at is not None and heartbeat is not None:
        age = max(0.0, (now - heartbeat_at).total_seconds())
        worker_healthy = (
            heartbeat.state in {"starting", "running", "success"}
            and age <= stale_after
        )

    newest_authoritative = _max_timestamp(
        market_at,
        funding_at,
        order_book_at,
        dex_at,
    )
    core_collection_at = _max_timestamp(newest_authoritative, heartbeat_at)

    live_rows: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        mechanism_id = str(row.get("mechanism_id") or "")
        row["forward_evidence_worker_healthy"] = worker_healthy
        row["forward_evidence_persistence_healthy"] = persistence_healthy

        existing_count = int(row.get("authoritative_observation_count") or 0)
        live_count: int | None = None
        authoritative_at: datetime | None = None

        if mechanism_id == "price_discrepancy":
            # The certification snapshot may already include DEX route observations.
            # Keep that exact historical total as a floor and let the exact market
            # ledger tail advance it without ever scanning the UUID DEX table.
            live_count = max(existing_count, market_count)
            authoritative_at = _max_timestamp(market_at, dex_at)
        elif mechanism_id == "carry":
            live_count = max(existing_count, market_count + funding_count)
            authoritative_at = _max_timestamp(market_at, funding_at)
        elif mechanism_id in {
            "trend_momentum",
            "mean_reversion",
            "cross_sectional_relative_value",
        }:
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

        # Core structural mechanisms can use the current collection heartbeat. Alpha
        # mechanisms preserve their point-in-time forward-cycle timestamps from the
        # certification snapshot; only their authoritative input freshness is overlaid.
        if mechanism_id not in ALPHA_MECHANISMS and core_collection_at is not None:
            row["forward_evidence_last_cycle_at"] = core_collection_at.isoformat()
            row["forward_evidence_next_expected_at"] = (
                core_collection_at + timedelta(seconds=core_expected_interval)
            ).isoformat()
            row["forward_evidence_expected_interval_seconds"] = core_expected_interval
        elif mechanism_id in ALPHA_MECHANISMS:
            last_cycle = _parse_timestamp(row.get("forward_evidence_last_cycle_at"))
            if last_cycle is not None:
                row["forward_evidence_next_expected_at"] = (
                    last_cycle + timedelta(seconds=alpha_expected_interval)
                ).isoformat()
                row["forward_evidence_expected_interval_seconds"] = alpha_expected_interval

        live_rows.append(row)

    telemetry = {
        "available": heartbeat_at is not None or newest_authoritative is not None,
        "worker_id": RESEARCH_WORKER_ID,
        "worker_healthy": worker_healthy,
        "persistence_healthy": persistence_healthy,
        "worker_heartbeat_at": heartbeat_at.isoformat() if heartbeat_at is not None else None,
        "latest_collection_at": (
            core_collection_at.isoformat() if core_collection_at is not None else None
        ),
        "latest_authoritative_observation_at": (
            newest_authoritative.isoformat() if newest_authoritative is not None else None
        ),
        "durable_counts": {
            "market": market_count,
            "funding": funding_count,
            "order_book": order_book_count,
            # Intentionally omitted from cumulative live arithmetic: UUID route rows
            # have no constant-time exact cardinality. Their indexed latest timestamp
            # is still included in freshness and the certification count is preserved.
            "dex_route": None,
        },
        "query_mode": "append_only_primary_key_tail",
    }
    return live_rows, telemetry


# Replace only the expensive GET route installed by read_api. Everything else remains
# the v3.5.24 lightweight read plane.
app.router.routes[:] = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/v3/operations/mechanisms"
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]
app.openapi_schema = None


@app.get("/v3/operations/mechanisms")
def fast_operating_mechanisms():
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
                "query_mode": "append_only_primary_key_tail",
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
