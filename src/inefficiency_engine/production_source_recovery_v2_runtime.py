from __future__ import annotations

import asyncio
import gc
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from inefficiency_engine.coinbase_trade_flow import (
    COINBASE_EXCHANGE_URL,
    COINBASE_TRADE_FLOW_SOURCE_ID,
    DEFAULT_TRADE_PRODUCTS,
    _persist_trade_events_bulk,
    _stable_id,
    parse_coinbase_product_trades,
)
from inefficiency_engine.critical_source_cadence import critical_source_interval_seconds
from inefficiency_engine.option_capacity import (
    DERIBIT_BASE_URL as DERIBIT_CAPACITY_BASE_URL,
    collect_deribit_option_capacity,
)
from inefficiency_engine.permanent_source_plane import PermanentSourcePlane
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_options import OKX_BASE_URL, collect_okx_options
from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    AAVE_V3_ETHEREUM_POOL,
    parse_aave_liquidation_log,
    stable_id,
)
from inefficiency_engine.production_source_recovery_runtime import (
    LIDO_APR_URL,
    LIDO_PREFLIGHT_REFRESH_SECONDS,
    LIDO_SOURCE_ID,
    aave_rpc_candidates,
    collect_lido_provider_resilient,
    collect_lido_yield_resilient,
    install_lido_provider_recovery,
)
from inefficiency_engine.provider_gap_collection import DEFAULT_ETHEREUM_RPC_URL, _safe_reference
from inefficiency_engine.provider_gap_resilience import HYPERLIQUID_INFO_URL
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation
from inefficiency_engine.source_lane_repair_runtime import (
    AAVE_PREFLIGHT_REFRESH_SECONDS,
    HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
    TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
    RemainingSourceLaneRepairService,
    collect_hyperliquid_distress_resilient,
)


CRITICAL_SOURCE_CADENCE_WORKER_ID = "critical-source-freshness-plane"
AAVE_ADAPTIVE_LOOKBACK_WINDOWS = (128, 32, 8, 2, 0)
AAVE_TRANSPORT_BUDGET_SECONDS = 6.0
TRADE_FLOW_ATTEMPTS_PER_PRODUCT = 3
TRADE_FLOW_REQUEST_TIMEOUT_SECONDS = 4.0
TRADE_FLOW_PREFLIGHT_TIMEOUT_SECONDS = 16.0
OKX_OPTIONS_PREFLIGHT_REFRESH_SECONDS = 300.0
DERIBIT_CAPACITY_PREFLIGHT_REFRESH_SECONDS = 300.0
OPTION_PREFLIGHT_TIMEOUT_SECONDS = 24.0


class AaveAllTransportsFailed(RuntimeError):
    """All configured transports failed the unchanged Aave log query."""


class CoinbaseTradeFlowUnavailable(RuntimeError):
    """Coinbase first-party trade-flow requests did not complete within the bounded retries."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _rpc_call(
    client: httpx.AsyncClient,
    *,
    url: str,
    method: str,
    params: list[object],
) -> object:
    response = await client.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("error"):
        error = payload.get("error") if isinstance(payload, dict) else None
        raise ValueError(f"Ethereum RPC {method} failed: {str(error)[:160]}")
    return payload.get("result")


async def _collect_aave_from_rpc_adaptive(
    coverage: SourceCoveragePlane,
    *,
    url: str,
) -> SourceProbeResult:
    """Query the same Aave V3 event with progressively smaller exact block ranges.

    The final block-hash query is an RPC compatibility fallback for providers that
    reject even tiny range-form ``eth_getLogs`` requests. It does not substitute a
    different evidence source or relax the Aave contract/topic requirement.
    """

    source = _safe_reference(url)
    timeout = httpx.Timeout(3.0, connect=1.5)
    failures: list[dict[str, object]] = []
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={
            "Cache-Control": "no-cache",
            "User-Agent": "crypto-inefficiency-engine/source-recovery-v2",
        },
    ) as client:
        latest_raw = await _rpc_call(client, url=url, method="eth_blockNumber", params=[])
        latest = int(str(latest_raw), 16)
        latest_hex = hex(latest)
        logs: list[object] | None = None
        selected_mode: str | None = None
        selected_lookback: int | None = None

        for lookback in AAVE_ADAPTIVE_LOOKBACK_WINDOWS:
            from_block = latest_hex if lookback == 0 else hex(max(0, latest - lookback))
            try:
                result = await _rpc_call(
                    client,
                    url=url,
                    method="eth_getLogs",
                    params=[
                        {
                            "address": AAVE_V3_ETHEREUM_POOL,
                            "fromBlock": from_block,
                            "toBlock": latest_hex,
                            "topics": [AAVE_LIQUIDATION_TOPIC],
                        }
                    ],
                )
                if not isinstance(result, list):
                    raise ValueError("Aave liquidation range query returned invalid data")
                logs = result
                selected_mode = "range"
                selected_lookback = lookback
                break
            except (httpx.HTTPError, ValueError) as exc:
                failures.append(
                    {
                        "transport": source,
                        "mode": "range",
                        "lookback_blocks": lookback,
                        "error_type": type(exc).__name__,
                        "message": str(exc)[:160],
                    }
                )

        if logs is None:
            try:
                block = await _rpc_call(
                    client,
                    url=url,
                    method="eth_getBlockByNumber",
                    params=[latest_hex, False],
                )
                block_hash = str(block.get("hash") or "") if isinstance(block, dict) else ""
                if not block_hash.startswith("0x"):
                    raise ValueError("latest Ethereum block hash unavailable")
                result = await _rpc_call(
                    client,
                    url=url,
                    method="eth_getLogs",
                    params=[
                        {
                            "blockHash": block_hash,
                            "address": AAVE_V3_ETHEREUM_POOL,
                            "topics": [AAVE_LIQUIDATION_TOPIC],
                        }
                    ],
                )
                if not isinstance(result, list):
                    raise ValueError("Aave liquidation block-hash query returned invalid data")
                logs = result
                selected_mode = "block_hash"
                selected_lookback = 0
            except (httpx.HTTPError, ValueError) as exc:
                failures.append(
                    {
                        "transport": source,
                        "mode": "block_hash",
                        "lookback_blocks": 0,
                        "error_type": type(exc).__name__,
                        "message": str(exc)[:160],
                    }
                )

    if logs is None:
        types = ",".join(str(row["error_type"]) for row in failures) or "unknown"
        raise AaveAllTransportsFailed(f"{source} Aave log query failed: {types}")

    observed_at = _now()
    parsed_count = 0
    for raw in logs:
        row = parse_aave_liquidation_log(raw)
        if row is None:
            continue
        parsed_count += 1
        coverage.record_event(
            SourceEventObservation(
                event_id=stable_id(
                    "aave-liquidations",
                    row.get("transaction_hash"),
                    row.get("log_index"),
                ),
                lane_id="liquidation_distress",
                source_id="aave-liquidations",
                event_type="protocol_liquidation",
                event_at=observed_at,
                observed_at=observed_at,
                asset=str(row.get("debt_asset") or ""),
                source_reference=source,
                payload=row,
            )
        )

    return SourceProbeResult(
        source_id="aave-liquidations",
        item_count=parsed_count,
        source_reference=source,
        evidence_by_lane={"liquidation_distress": ["liquidation_events"]},
        detail={
            "pool": AAVE_V3_ETHEREUM_POOL,
            "latest_block": latest,
            "query_mode": selected_mode,
            "lookback_blocks": selected_lookback,
            "query_failures": failures,
            "same_aave_contract_and_topic": True,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
    )


async def collect_aave_liquidations_resilient_v2(
    coverage: SourceCoveragePlane,
) -> SourceProbeResult:
    """Try each existing Ethereum RPC transport and preserve exact failure detail."""

    failures: list[dict[str, object]] = []
    candidates = aave_rpc_candidates()
    for index, url in enumerate(candidates):
        try:
            probe = await asyncio.wait_for(
                _collect_aave_from_rpc_adaptive(coverage, url=url),
                timeout=AAVE_TRANSPORT_BUDGET_SECONDS,
            )
            probe.detail["rpc_candidate_count"] = len(candidates)
            probe.detail["rpc_transport_fallback_used"] = index > 0
            probe.detail["transport_failures"] = failures
            return probe
        except Exception as exc:
            failures.append(
                {
                    "source_reference": _safe_reference(url),
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:220],
                }
            )

    summary = "; ".join(
        f"{row['source_reference']}={row['error_type']}:{row['message']}"
        for row in failures
    )
    raise AaveAllTransportsFailed(summary[:900] or "no Aave RPC transport succeeded")


async def _fetch_coinbase_product_trades(
    *,
    product_id: str,
    limit: int,
    observed_at: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Retry one Coinbase first-party product with a fresh transport each time."""

    url = f"{COINBASE_EXCHANGE_URL}/products/{product_id}/trades"
    failures: list[dict[str, object]] = []
    for attempt in range(1, TRADE_FLOW_ATTEMPTS_PER_PRODUCT + 1):
        try:
            timeout = httpx.Timeout(TRADE_FLOW_REQUEST_TIMEOUT_SECONDS, connect=2.0)
            async with httpx.AsyncClient(
                timeout=timeout,
                headers={
                    "User-Agent": "crypto-inefficiency-engine/source-trade-flow-v2",
                    "Cache-Control": "no-cache",
                },
            ) as client:
                response = await client.get(
                    url,
                    params={"limit": max(1, min(1000, int(limit)))},
                )
                response.raise_for_status()
                rows = parse_coinbase_product_trades(
                    response.json(),
                    product_id=product_id,
                    observed_at=observed_at,
                )
            if not rows:
                raise ValueError(f"Coinbase {product_id} returned no usable trades")
            return rows, failures
        except (httpx.HTTPError, ValueError) as exc:
            failures.append(
                {
                    "product_id": product_id,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:160],
                }
            )
            if attempt < TRADE_FLOW_ATTEMPTS_PER_PRODUCT:
                await asyncio.sleep(0.15 * attempt)
    return [], failures


async def collect_coinbase_trade_flow_resilient(
    coverage: SourceCoveragePlane,
    *,
    products: tuple[str, ...] = DEFAULT_TRADE_PRODUCTS,
    limit: int = 100,
) -> SourceProbeResult:
    """Capture the unchanged Coinbase trade-flow surface with bounded fresh-client retries.

    Every configured product must still succeed. This repairs transient transport
    failures without weakening the breadth of the source evidence that was required
    by the original collector.
    """

    observed_at = _now()
    results = await asyncio.gather(
        *(
            _fetch_coinbase_product_trades(
                product_id=product_id,
                limit=limit,
                observed_at=observed_at,
            )
            for product_id in products
        )
    )
    trades: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    failed_products: list[str] = []
    for product_id, (rows, product_failures) in zip(products, results, strict=True):
        failures.extend(product_failures)
        if not rows:
            failed_products.append(product_id)
        trades.extend(rows)

    if failed_products:
        summary = ",".join(
            f"{row['product_id']}:{row['error_type']}" for row in failures[-9:]
        )
        raise CoinbaseTradeFlowUnavailable(
            f"Coinbase trade-flow failed products={failed_products}; attempts={summary}"
        )
    if not trades:
        raise CoinbaseTradeFlowUnavailable("Coinbase public trades returned no usable trade flow")

    source_reference = f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades"
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
                    event_id=_stable_id("coinbase-trade", trade["trade_id"], lane_id),
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

    inserted = await asyncio.to_thread(_persist_trade_events_bulk, coverage, observations)
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
            "inserted_event_row_count": inserted,
            "transport_retry_failures": failures,
            "all_configured_products_required": True,
            "fresh_client_per_retry": True,
            "side_semantics": "coinbase_maker_side_inverted_to_aggressor",
            "qualification_thresholds_unchanged": True,
            "allocation_authority": False,
            "paper_only": True,
        },
    )


async def run_critical_source_refresh_once(
    service: RemainingSourceLaneRepairService,
) -> dict[str, object]:
    """Refresh short-TTL and volatility evidence independently from the slow tail."""

    latest = await asyncio.to_thread(service.source_coverage.ledger.latest)
    gc.collect()

    trade = await service._preflight(
        source_id="public-trade-flow",
        lane_ids=["liquidity_provision", "microstructure"],
        source_reference=f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades",
        refresh_seconds=TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
        timeout_seconds=TRADE_FLOW_PREFLIGHT_TIMEOUT_SECONDS,
        collector=lambda: collect_coinbase_trade_flow_resilient(service.source_coverage),
        latest=latest,
    )

    latest = await asyncio.to_thread(service.source_coverage.ledger.latest)
    gc.collect()
    tasks = (
        asyncio.create_task(
            service._preflight(
                source_id="aave-liquidations",
                lane_ids=["liquidation_distress"],
                source_reference=_safe_reference(
                    os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
                ),
                refresh_seconds=AAVE_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=15.0,
                collector=lambda: collect_aave_liquidations_resilient_v2(
                    service.source_coverage
                ),
                latest=latest,
            )
        ),
        asyncio.create_task(
            service._preflight(
                source_id="hyperliquid-distress",
                lane_ids=["liquidation_distress"],
                source_reference=HYPERLIQUID_INFO_URL,
                refresh_seconds=HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=15.0,
                collector=collect_hyperliquid_distress_resilient,
                latest=latest,
            )
        ),
        asyncio.create_task(
            service._preflight(
                source_id=LIDO_SOURCE_ID,
                lane_ids=["yield"],
                source_reference=LIDO_APR_URL,
                refresh_seconds=LIDO_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=12.0,
                collector=collect_lido_yield_resilient,
                latest=latest,
            )
        ),
        asyncio.create_task(
            service._preflight(
                source_id="okx-options",
                lane_ids=["volatility"],
                source_reference=f"{OKX_BASE_URL}/api/v5/public/opt-summary",
                refresh_seconds=OKX_OPTIONS_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=OPTION_PREFLIGHT_TIMEOUT_SECONDS,
                collector=lambda: collect_okx_options(service.volatility_service),
                latest=latest,
            )
        ),
        asyncio.create_task(
            service._preflight(
                source_id="deribit-option-capacity",
                lane_ids=["volatility"],
                source_reference=f"{DERIBIT_CAPACITY_BASE_URL}/public/get_order_book",
                refresh_seconds=DERIBIT_CAPACITY_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=OPTION_PREFLIGHT_TIMEOUT_SECONDS,
                collector=lambda: collect_deribit_option_capacity(service.store),
                latest=latest,
            )
        ),
    )
    aave, hyperliquid, lido, okx_options, deribit_capacity = await asyncio.gather(*tasks)
    transfer = await asyncio.to_thread(service.capital_transfer_evidence.status)
    gc.collect()
    return {
        "trade_flow": trade,
        "aave": aave,
        "hyperliquid": hyperliquid,
        "lido": lido,
        "okx_options": okx_options,
        "deribit_option_capacity": deribit_capacity,
        "capital_transfer_evidence": transfer,
        "qualification_thresholds_unchanged": True,
        "source_freshness_thresholds_unchanged": True,
        "paper_only": True,
        "allocation_authority": False,
        "live_execution_authority": False,
    }


async def critical_source_refresh_loop(
    store: Any,
    *,
    stop_event: asyncio.Event,
) -> None:
    """Run source recovery on a start-to-start cadence without overlapping cycles."""

    plane: PermanentSourcePlane | None = None
    sequence = 0
    while not stop_event.is_set():
        sequence += 1
        started = time.monotonic()
        try:
            if plane is None:
                plane = PermanentSourcePlane(store)
            service = plane.priority
            if not isinstance(service, RemainingSourceLaneRepairService):
                raise RuntimeError("critical source cadence requires repaired priority service")
            result = await run_critical_source_refresh_once(service)
            states = {
                str(row.get("state"))
                for row in (
                    result["trade_flow"],
                    result["aave"],
                    result["hyperliquid"],
                    result["lido"],
                    result["okx_options"],
                    result["deribit_option_capacity"],
                )
                if isinstance(row, dict)
            }
            degraded = bool(states & {"provider_failed", "memory_deferred"})
            await asyncio.to_thread(
                store.record_worker_heartbeat,
                worker_id=CRITICAL_SOURCE_CADENCE_WORKER_ID,
                state="degraded" if degraded else "success",
                detail={
                    "sequence": sequence,
                    "stage": "critical_source_refresh_complete",
                    "cycle_runtime_seconds": max(0.0, time.monotonic() - started),
                    "start_to_start_interval_seconds": critical_source_interval_seconds(),
                    "result": result,
                    "independent_from_priority_source_tail": True,
                    "volatility_freshness_recovery": True,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                },
            )
        except Exception as exc:
            plane = None
            try:
                await asyncio.to_thread(
                    store.record_worker_heartbeat,
                    worker_id=CRITICAL_SOURCE_CADENCE_WORKER_ID,
                    state="degraded",
                    error_type=type(exc).__name__,
                    detail={
                        "sequence": sequence,
                        "stage": "critical_source_refresh_failed",
                        "message": str(exc)[:900],
                        "retrying": True,
                        "independent_from_priority_source_tail": True,
                        "qualification_thresholds_unchanged": True,
                        "paper_only": True,
                        "allocation_authority": False,
                        "live_execution_authority": False,
                    },
                )
            except Exception:
                pass

        delay = max(
            0.0,
            critical_source_interval_seconds() - max(0.0, time.monotonic() - started),
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except TimeoutError:
            continue


__all__ = [
    "AaveAllTransportsFailed",
    "CoinbaseTradeFlowUnavailable",
    "collect_aave_liquidations_resilient_v2",
    "collect_coinbase_trade_flow_resilient",
    "collect_lido_provider_resilient",
    "critical_source_refresh_loop",
    "install_lido_provider_recovery",
]
