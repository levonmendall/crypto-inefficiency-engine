from __future__ import annotations

import asyncio
import hashlib
import json
import math
import statistics
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, insert
from websockets.asyncio.client import connect as websocket_connect

from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation


BYBIT_TRADE_WS = "wss://stream.bybit.com/v5/public/linear"
OKX_TRADE_WS = "wss://ws.okx.com:8443/ws/v5/public"
TRADE_FLOW_SOURCE_ID = "public-trade-flow"
MAX_EVENTS_PER_VENUE_BATCH = 2500


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stable(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:48]


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _event_time(value: object) -> datetime | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    if number > 10_000_000_000:
        number /= 1000.0
    return datetime.fromtimestamp(number, tz=timezone.utc)


class EventStreamIntegrityLedger:
    """Bounded append-only telemetry proving event timing and stream continuity."""

    def __init__(self, coverage: SourceCoveragePlane):
        self.store = coverage.store
        metadata = MetaData()
        self.rows = Table(
            "event_stream_integrity",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("batch_id", String(64), nullable=False, unique=True),
            Column("source_id", Text, nullable=False),
            Column("venue", Text, nullable=False),
            Column("observed_at", Text, nullable=False),
            Column("payload_json", Text, nullable=False),
        )
        Index("ix_event_stream_integrity_source", self.rows.c.source_id, self.rows.c.id)
        Index("ix_event_stream_integrity_venue", self.rows.c.venue, self.rows.c.id)
        metadata.create_all(self.store.engine)

    def record(self, *, venue: str, observed_at: datetime, payload: dict[str, object]) -> str:
        batch_id = uuid.uuid4().hex
        with self.store.engine.begin() as db:
            db.execute(
                insert(self.rows),
                {
                    "batch_id": batch_id,
                    "source_id": TRADE_FLOW_SOURCE_ID,
                    "venue": venue,
                    "observed_at": observed_at.isoformat(),
                    "payload_json": json.dumps(
                        {
                            **payload,
                            "paper_only": True,
                            "allocation_authority": False,
                            "live_execution_authority": False,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            )
        return batch_id


def _integrity_summary(events: list[dict[str, object]]) -> dict[str, object]:
    if not events:
        return {
            "event_count": 0,
            "duplicate_event_count": 0,
            "event_time_regression_count": 0,
            "sequence_supported": False,
            "sequence_gap_count": 0,
            "max_receive_latency_ms": None,
            "median_receive_latency_ms": None,
            "integrity_degraded": True,
        }
    seen: set[str] = set()
    duplicates = 0
    regressions = 0
    last_event_by_symbol: dict[str, datetime] = {}
    last_sequence_by_symbol: dict[str, int] = {}
    sequence_gap_count = 0
    sequence_supported = False
    latencies: list[float] = []
    for row in events:
        event_id = str(row.get("exchange_event_id") or row.get("event_id") or "")
        if event_id:
            if event_id in seen:
                duplicates += 1
            seen.add(event_id)
        symbol = str(row.get("symbol") or "")
        event_at = row.get("event_at")
        received_at = row.get("received_at")
        if isinstance(event_at, datetime) and isinstance(received_at, datetime):
            latencies.append(max(0.0, (received_at - event_at).total_seconds() * 1000.0))
            prior_event = last_event_by_symbol.get(symbol)
            if prior_event is not None and event_at < prior_event:
                regressions += 1
            last_event_by_symbol[symbol] = max(prior_event or event_at, event_at)
        sequence_raw = row.get("sequence")
        try:
            sequence = int(sequence_raw) if sequence_raw is not None else None
        except (TypeError, ValueError):
            sequence = None
        if sequence is not None:
            sequence_supported = True
            prior = last_sequence_by_symbol.get(symbol)
            if prior is not None and sequence > prior + 1:
                sequence_gap_count += sequence - prior - 1
            if prior is None or sequence > prior:
                last_sequence_by_symbol[symbol] = sequence
    return {
        "event_count": len(events),
        "duplicate_event_count": duplicates,
        "event_time_regression_count": regressions,
        "sequence_supported": sequence_supported,
        "sequence_gap_count": sequence_gap_count,
        "max_receive_latency_ms": max(latencies) if latencies else None,
        "median_receive_latency_ms": statistics.median(latencies) if latencies else None,
        "integrity_degraded": bool(duplicates or regressions or sequence_gap_count),
    }


def _record_trade_event(
    coverage: SourceCoveragePlane,
    *,
    venue: str,
    symbol: str,
    asset: str,
    exchange_event_id: str,
    sequence: int | None,
    event_at: datetime,
    received_at: datetime,
    side: str,
    price: float,
    size: float,
    source_reference: str,
) -> dict[str, object]:
    event_id = _stable(venue, symbol, exchange_event_id, event_at.isoformat(), side, price, size)
    payload = {
        "venue": venue,
        "symbol": symbol,
        "exchange_event_id": exchange_event_id,
        "sequence": sequence,
        "exchange_event_at": event_at.isoformat(),
        "local_receive_at": received_at.isoformat(),
        "aggressor_side": side,
        "price": price,
        "size": size,
        "notional_usd": price * size,
        "event_integrity_version": 1,
    }
    for lane_id in ("microstructure", "liquidity_provision"):
        coverage.record_event(
            SourceEventObservation(
                event_id=_stable(event_id, lane_id),
                lane_id=lane_id,
                source_id=TRADE_FLOW_SOURCE_ID,
                event_type="public_trade",
                event_at=event_at,
                observed_at=received_at,
                asset=asset.upper(),
                source_reference=source_reference,
                payload=payload,
            )
        )
    return {
        "event_id": event_id,
        "exchange_event_id": exchange_event_id,
        "sequence": sequence,
        "symbol": symbol,
        "event_at": event_at,
        "received_at": received_at,
    }


async def _capture_bybit(
    coverage: SourceCoveragePlane,
    *,
    listen_seconds: float = 1.25,
) -> tuple[int, dict[str, object]]:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    topics = [f"publicTrade.{symbol}" for symbol in symbols]
    events: list[dict[str, object]] = []
    acknowledged = False
    async with websocket_connect(
        BYBIT_TRADE_WS,
        open_timeout=5,
        close_timeout=2,
        ping_interval=20,
    ) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))
        deadline = asyncio.get_running_loop().time() + max(0.25, listen_seconds)
        while asyncio.get_running_loop().time() < deadline and len(events) < MAX_EVENTS_PER_VENUE_BATCH:
            timeout = max(0.05, deadline - asyncio.get_running_loop().time())
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except TimeoutError:
                break
            received_at = _now()
            payload: Any = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            if isinstance(payload, dict) and payload.get("success") is True:
                acknowledged = True
            if not isinstance(payload, dict) or not str(payload.get("topic") or "").startswith("publicTrade."):
                continue
            rows = payload.get("data")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if len(events) >= MAX_EVENTS_PER_VENUE_BATCH or not isinstance(row, dict):
                    break
                symbol = str(row.get("s") or "")
                price = _number(row.get("p"))
                size = _number(row.get("v"))
                event_at = _event_time(row.get("T")) or received_at
                side_raw = str(row.get("S") or "").lower()
                side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else None
                if not symbol or price is None or size is None or price <= 0 or size <= 0 or side is None:
                    continue
                acknowledged = True
                asset = symbol[:-4] if symbol.endswith("USDT") else symbol
                exchange_id = str(row.get("i") or _stable(symbol, event_at.isoformat(), price, size, side))
                sequence_raw = row.get("seq")
                try:
                    sequence = int(sequence_raw) if sequence_raw is not None else None
                except (TypeError, ValueError):
                    sequence = None
                events.append(
                    _record_trade_event(
                        coverage,
                        venue="Bybit",
                        symbol=symbol,
                        asset=asset,
                        exchange_event_id=exchange_id,
                        sequence=sequence,
                        event_at=event_at,
                        received_at=received_at,
                        side=side,
                        price=price,
                        size=size,
                        source_reference=BYBIT_TRADE_WS,
                    )
                )
    if not acknowledged:
        raise ValueError("Bybit trade stream subscription was not acknowledged")
    summary = _integrity_summary(events)
    EventStreamIntegrityLedger(coverage).record(
        venue="Bybit",
        observed_at=_now(),
        payload={"source_reference": BYBIT_TRADE_WS, **summary},
    )
    return len(events), summary


async def _capture_okx(
    coverage: SourceCoveragePlane,
    *,
    listen_seconds: float = 1.25,
) -> tuple[int, dict[str, object]]:
    instruments = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
    args = [{"channel": "trades", "instId": instrument} for instrument in instruments]
    events: list[dict[str, object]] = []
    acknowledged = False
    async with websocket_connect(
        OKX_TRADE_WS,
        open_timeout=5,
        close_timeout=2,
        ping_interval=20,
    ) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": args}))
        deadline = asyncio.get_running_loop().time() + max(0.25, listen_seconds)
        while asyncio.get_running_loop().time() < deadline and len(events) < MAX_EVENTS_PER_VENUE_BATCH:
            timeout = max(0.05, deadline - asyncio.get_running_loop().time())
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except TimeoutError:
                break
            received_at = _now()
            payload: Any = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            if isinstance(payload, dict) and payload.get("event") == "subscribe":
                acknowledged = True
                continue
            if not isinstance(payload, dict):
                continue
            arg = payload.get("arg")
            if not isinstance(arg, dict) or arg.get("channel") != "trades":
                continue
            rows = payload.get("data")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if len(events) >= MAX_EVENTS_PER_VENUE_BATCH or not isinstance(row, dict):
                    break
                symbol = str(row.get("instId") or arg.get("instId") or "")
                price = _number(row.get("px"))
                size = _number(row.get("sz"))
                event_at = _event_time(row.get("ts")) or received_at
                side_raw = str(row.get("side") or "").lower()
                side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else None
                if not symbol or price is None or size is None or price <= 0 or size <= 0 or side is None:
                    continue
                acknowledged = True
                asset = symbol.split("-")[0]
                exchange_id = str(row.get("tradeId") or _stable(symbol, event_at.isoformat(), price, size, side))
                sequence_raw = row.get("seqId")
                try:
                    sequence = int(sequence_raw) if sequence_raw is not None else None
                except (TypeError, ValueError):
                    sequence = None
                events.append(
                    _record_trade_event(
                        coverage,
                        venue="OKX",
                        symbol=symbol,
                        asset=asset,
                        exchange_event_id=exchange_id,
                        sequence=sequence,
                        event_at=event_at,
                        received_at=received_at,
                        side=side,
                        price=price,
                        size=size,
                        source_reference=OKX_TRADE_WS,
                    )
                )
    if not acknowledged:
        raise ValueError("OKX trade stream subscription was not acknowledged")
    summary = _integrity_summary(events)
    EventStreamIntegrityLedger(coverage).record(
        venue="OKX",
        observed_at=_now(),
        payload={"source_reference": OKX_TRADE_WS, **summary},
    )
    return len(events), summary


async def collect_multi_venue_trade_flow(
    coverage: SourceCoveragePlane,
) -> SourceProbeResult:
    """Bounded rolling first-party flow with multi-venue timing/gap integrity."""

    successes: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}
    total = 0
    for venue, collector in (("Bybit", _capture_bybit), ("OKX", _capture_okx)):
        try:
            count, integrity = await collector(coverage)
            if count > 0:
                total += count
                successes[venue] = integrity
            else:
                errors[venue] = "zero point-in-time trades"
        except Exception as exc:
            errors[venue] = f"{type(exc).__name__}: {str(exc)[:180]}"
    if total <= 0:
        raise ValueError(f"all first-party trade-flow streams failed: {errors}")
    return SourceProbeResult(
        source_id=TRADE_FLOW_SOURCE_ID,
        item_count=total,
        source_reference=f"{BYBIT_TRADE_WS};{OKX_TRADE_WS}",
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
            "venues_healthy": sorted(successes),
            "venue_count": len(successes),
            "venue_errors": errors,
            "stream_integrity": successes,
            "bounded_event_count": total,
            "bounded_rolling_capture": True,
            "memory_unbounded_stream": False,
            "maker_fill_assumed": False,
        },
    )
