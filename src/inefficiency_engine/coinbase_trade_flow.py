from __future__ import annotations

import asyncio
import hashlib
import math
from datetime import datetime, timezone

import httpx
from sqlalchemy import insert, select

from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation


COINBASE_EXCHANGE_URL = "https://api.exchange.coinbase.com"
COINBASE_TRADE_FLOW_SOURCE_ID = "public-trade-flow"
DEFAULT_TRADE_PRODUCTS = ("BTC-USD", "ETH-USD", "SOL-USD")
_BULK_EVENT_CHUNK_SIZE = 500


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:48]


def _number(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def parse_coinbase_product_trades(
    payload: object,
    *,
    product_id: str,
    observed_at: datetime,
) -> list[dict[str, object]]:
    """Normalize Coinbase Exchange trades into taker/aggressor flow.

    Coinbase Exchange documents the REST trade ``side`` as the *maker* order side.
    Aggressor flow is therefore the opposite side. Keeping that distinction explicit
    prevents the microstructure feature from silently reversing its sign.
    """

    if not isinstance(payload, list):
        raise ValueError("Coinbase product trades response must be a list")
    asset = product_id.split("-", 1)[0].upper()
    rows: list[dict[str, object]] = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        trade_id = str(raw.get("trade_id") or "").strip()
        maker_side = str(raw.get("side") or "").lower().strip()
        price = _number(raw.get("price"))
        size = _number(raw.get("size"))
        raw_time = raw.get("time")
        if not trade_id or maker_side not in {"buy", "sell"} or price is None or size is None:
            continue
        if price <= 0 or size <= 0 or raw_time in (None, ""):
            continue
        try:
            event_at = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
        except ValueError:
            continue
        if event_at.tzinfo is None:
            event_at = event_at.replace(tzinfo=timezone.utc)
        aggressor = "sell" if maker_side == "buy" else "buy"
        rows.append(
            {
                "trade_id": trade_id,
                "asset": asset,
                "symbol": product_id,
                "event_at": event_at.astimezone(timezone.utc),
                "observed_at": observed_at,
                "maker_side": maker_side,
                "aggressor_side": aggressor,
                "price": price,
                "size": size,
                "notional_usd": price * size,
            }
        )
    return rows


def _event_rows(observations: list[SourceEventObservation]) -> list[dict[str, object]]:
    return [
        {
            "event_id": row.event_id,
            "lane_id": row.lane_id,
            "source_id": row.source_id,
            "event_type": row.event_type,
            "observed_at": row.observed_at.isoformat(),
            "event_at": row.event_at.isoformat(),
            "payload_json": row.model_dump_json(),
        }
        for row in observations
    ]


def _persist_trade_events_bulk(
    coverage: SourceCoveragePlane,
    observations: list[SourceEventObservation],
) -> int:
    """Persist a bounded event batch without one transaction per trade/lane row."""

    if not observations:
        return 0
    table = coverage.events.rows
    rows = _event_rows(observations)
    event_ids = [str(row["event_id"]) for row in rows]
    existing: set[str] = set()
    with coverage.store.engine.begin() as db:
        for offset in range(0, len(event_ids), _BULK_EVENT_CHUNK_SIZE):
            chunk = event_ids[offset : offset + _BULK_EVENT_CHUNK_SIZE]
            existing.update(
                str(value)
                for value in db.execute(
                    select(table.c.event_id).where(table.c.event_id.in_(chunk))
                ).scalars()
            )
        pending = [row for row in rows if str(row["event_id"]) not in existing]
        for offset in range(0, len(pending), _BULK_EVENT_CHUNK_SIZE):
            batch = pending[offset : offset + _BULK_EVENT_CHUNK_SIZE]
            if batch:
                db.execute(insert(table), batch)
    return len(pending)


async def collect_coinbase_trade_flow(
    coverage: SourceCoveragePlane,
    *,
    products: tuple[str, ...] = DEFAULT_TRADE_PRODUCTS,
    limit: int = 100,
) -> SourceProbeResult:
    """Capture bounded first-party public trade flow without credentials or websockets."""

    observed_at = _now()
    trades: list[dict[str, object]] = []
    source_reference = f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades"
    async with httpx.AsyncClient(
        timeout=8.0,
        headers={
            "User-Agent": "crypto-inefficiency-engine/source-trade-flow",
            "Cache-Control": "no-cache",
        },
    ) as client:
        for product_id in products:
            response = await client.get(
                f"{COINBASE_EXCHANGE_URL}/products/{product_id}/trades",
                params={"limit": max(1, min(1000, int(limit)))},
            )
            response.raise_for_status()
            trades.extend(
                parse_coinbase_product_trades(
                    response.json(),
                    product_id=product_id,
                    observed_at=observed_at,
                )
            )

    if not trades:
        raise ValueError("Coinbase public trades returned no usable trade flow")

    observations: list[SourceEventObservation] = []
    for trade in trades:
        event_at = trade["event_at"]
        assert isinstance(event_at, datetime)
        payload = {
            "venue": "Coinbase",
            "symbol": str(trade["symbol"]),
            "maker_side": str(trade["maker_side"]),
            "aggressor_side": str(trade["aggressor_side"]),
            "price": float(trade["price"]),
            "size": float(trade["size"]),
            "notional_usd": float(trade["notional_usd"]),
        }
        for lane_id in ("microstructure", "liquidity_provision"):
            observations.append(
                SourceEventObservation(
                    event_id=_stable_id(
                        "coinbase-trade",
                        trade["trade_id"],
                        lane_id,
                    ),
                    lane_id=lane_id,
                    source_id=COINBASE_TRADE_FLOW_SOURCE_ID,
                    event_type="public_trade",
                    event_at=event_at,
                    observed_at=observed_at,
                    asset=str(trade["asset"]),
                    source_reference=source_reference,
                    payload=payload,
                )
            )

    inserted_event_rows = await asyncio.to_thread(
        _persist_trade_events_bulk,
        coverage,
        observations,
    )

    return SourceProbeResult(
        source_id=COINBASE_TRADE_FLOW_SOURCE_ID,
        item_count=len(trades),
        source_reference=source_reference,
        evidence_by_lane={
            "microstructure": ["trade_flow"],
            "liquidity_provision": ["trade_flow"],
        },
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=True,
        forward_testable_evidence=True,
        detail={
            "venue": "Coinbase",
            "products": list(products),
            "trade_count": len(trades),
            "event_row_count": len(observations),
            "inserted_event_row_count": inserted_event_rows,
            "bulk_event_persistence": True,
            "side_semantics": "coinbase_maker_side_inverted_to_aggressor",
            "maker_fill_assumed": False,
            "credential_required": False,
            "allocation_authority": False,
            "paper_only": True,
        },
    )
