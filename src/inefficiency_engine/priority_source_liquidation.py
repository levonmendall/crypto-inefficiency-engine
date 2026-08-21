from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx
from websockets.asyncio.client import connect as websocket_connect

from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    AAVE_V3_ETHEREUM_POOL,
    parse_aave_liquidation_log,
    parse_bybit_liquidation_message,
    stable_id,
)
from inefficiency_engine.provider_gap_collection import DEFAULT_ETHEREUM_RPC_URL, _safe_reference
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation

BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def collect_bybit_liquidations(coverage: SourceCoveragePlane) -> SourceProbeResult:
    topics = [f"allLiquidation.{symbol}" for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")]
    events: list[dict[str, object]] = []
    acknowledged = False
    async with websocket_connect(BYBIT_LINEAR_WS, open_timeout=5, close_timeout=2, ping_interval=20) as websocket:
        await websocket.send(json.dumps({"op":"subscribe","args":topics}))
        deadline = asyncio.get_running_loop().time() + 1.75
        while asyncio.get_running_loop().time() < deadline:
            timeout = max(0.05, deadline - asyncio.get_running_loop().time())
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except TimeoutError:
                break
            payload = json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
            if isinstance(payload, dict) and payload.get("success") is True:
                acknowledged = True
            parsed = parse_bybit_liquidation_message(payload)
            if parsed:
                acknowledged = True
                events.extend(parsed)
    if not acknowledged:
        raise ValueError("Bybit liquidation subscription was not acknowledged")
    observed_at = _now()
    for event in events:
        event_at = event["event_at"]
        assert isinstance(event_at, datetime)
        coverage.record_event(SourceEventObservation(
            event_id=stable_id("bybit-liquidations",event["symbol"],event_at.isoformat(),event["side"],event["quantity"],event["price"]),
            lane_id="liquidation_distress", source_id="bybit-liquidations", event_type="exchange_liquidation",
            event_at=event_at, observed_at=observed_at, asset=str(event["symbol"]).replace("USDT",""),
            source_reference=BYBIT_LINEAR_WS, payload={key:value for key,value in event.items() if key != "event_at"},
        ))
    return SourceProbeResult(
        source_id="bybit-liquidations", item_count=len(events), source_reference=BYBIT_LINEAR_WS,
        evidence_by_lane={"liquidation_distress":["liquidation_events"],"microstructure":["liquidation_pressure"]},
        detail={"topics":topics,"subscription_acknowledged":True,"economic_opportunity_complete":False},
    )


async def collect_aave_liquidations(coverage: SourceCoveragePlane) -> SourceProbeResult:
    url = os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
    source = _safe_reference(url)
    async with httpx.AsyncClient(timeout=8.0, headers={"Cache-Control":"no-cache"}) as client:
        async def rpc(method: str, params: list[object]) -> object:
            response = await client.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params})
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload,dict) or payload.get("error"):
                raise ValueError(f"Ethereum RPC {method} failed")
            return payload.get("result")
        latest_raw = await rpc("eth_blockNumber", [])
        latest = int(str(latest_raw),16)
        logs = await rpc("eth_getLogs", [{"address":AAVE_V3_ETHEREUM_POOL,"fromBlock":hex(max(0,latest-512)),"toBlock":"latest","topics":[AAVE_LIQUIDATION_TOPIC]}])
    if not isinstance(logs,list):
        raise ValueError("Aave liquidation log query returned invalid data")
    observed_at, parsed_count = _now(), 0
    for raw in logs:
        row = parse_aave_liquidation_log(raw)
        if row is None:
            continue
        parsed_count += 1
        coverage.record_event(SourceEventObservation(
            event_id=stable_id("aave-liquidations",row.get("transaction_hash"),row.get("log_index")),
            lane_id="liquidation_distress", source_id="aave-liquidations", event_type="protocol_liquidation",
            event_at=observed_at, observed_at=observed_at, asset=str(row.get("debt_asset") or ""),
            source_reference=source, payload=row,
        ))
    return SourceProbeResult(
        source_id="aave-liquidations", item_count=parsed_count, source_reference=source,
        evidence_by_lane={"liquidation_distress":["liquidation_events"]},
        detail={"pool":AAVE_V3_ETHEREUM_POOL,"lookback_blocks":512,"economic_opportunity_complete":False},
    )
