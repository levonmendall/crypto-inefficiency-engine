from __future__ import annotations

import asyncio
import gc
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from inefficiency_engine.coinbase_trade_flow import COINBASE_EXCHANGE_URL, collect_coinbase_trade_flow
from inefficiency_engine.permanent_source_plane import PermanentSourcePlane
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    AAVE_V3_ETHEREUM_POOL,
    parse_aave_liquidation_log,
    stable_id,
)
from inefficiency_engine.provider_gap_collection import DEFAULT_ETHEREUM_RPC_URL, _safe_reference
from inefficiency_engine.provider_gap_resilience import HYPERLIQUID_INFO_URL
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation
from inefficiency_engine.source_lane_repair_runtime import (
    AAVE_LOOKBACK_WINDOWS,
    AAVE_PREFLIGHT_REFRESH_SECONDS,
    HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
    TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
    RemainingSourceLaneRepairService,
    collect_aave_liquidations_resilient,
    collect_hyperliquid_distress_resilient,
)


CRITICAL_SOURCE_CADENCE_WORKER_ID = "critical-source-freshness-plane"
DEFAULT_CRITICAL_SOURCE_INTERVAL_SECONDS = 30.0
AAVE_RPC_FALLBACK_URLS = ("https://cloudflare-eth.com",)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def critical_source_interval_seconds() -> float:
    try:
        value = float(
            os.getenv(
                "CIE_CRITICAL_SOURCE_INTERVAL_SECONDS",
                str(DEFAULT_CRITICAL_SOURCE_INTERVAL_SECONDS),
            )
        )
    except ValueError:
        value = DEFAULT_CRITICAL_SOURCE_INTERVAL_SECONDS
    return max(15.0, min(60.0, value))


def _aave_rpc_candidates() -> tuple[str, ...]:
    primary = os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
    ordered: list[str] = []
    for value in (primary, *AAVE_RPC_FALLBACK_URLS):
        candidate = str(value or "").strip()
        if candidate and candidate not in ordered:
            ordered.append(candidate)
    return tuple(ordered)


async def _collect_aave_from_rpc(
    coverage: SourceCoveragePlane,
    *,
    url: str,
) -> SourceProbeResult:
    source = _safe_reference(url)
    timeout = httpx.Timeout(4.0, connect=3.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={
            "Cache-Control": "no-cache",
            "User-Agent": "crypto-inefficiency-engine/critical-source-cadence",
        },
    ) as client:
        async def rpc(method: str, params: list[object]) -> object:
            response = await client.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("error"):
                raise ValueError(f"Ethereum RPC {method} failed")
            return payload.get("result")

        latest_raw = await rpc("eth_blockNumber", [])
        latest = int(str(latest_raw), 16)
        logs: list[object] | None = None
        selected_lookback: int | None = None
        last_error: Exception | None = None
        for lookback in AAVE_LOOKBACK_WINDOWS:
            try:
                result = await rpc(
                    "eth_getLogs",
                    [
                        {
                            "address": AAVE_V3_ETHEREUM_POOL,
                            "fromBlock": hex(max(0, latest - lookback)),
                            "toBlock": "latest",
                            "topics": [AAVE_LIQUIDATION_TOPIC],
                        }
                    ],
                )
                if not isinstance(result, list):
                    raise ValueError("Aave liquidation log query returned invalid data")
                logs = result
                selected_lookback = lookback
                break
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                await asyncio.sleep(0.05)
        if logs is None:
            assert last_error is not None
            raise last_error

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
            "lookback_blocks": selected_lookback,
            "rpc_transport_fallback": True,
            "qualification_thresholds_unchanged": True,
        },
    )


async def collect_aave_liquidations_with_transport_fallback(
    coverage: SourceCoveragePlane,
) -> SourceProbeResult:
    """Try the configured RPC first, then one independent public RPC transport.

    Both transports query the same Aave V3 contract/log topic on Ethereum. This is a
    transport-availability repair only; it does not create a second authoritative
    source group or weaken the liquidation evidence requirement.
    """

    failures: list[dict[str, object]] = []
    candidates = _aave_rpc_candidates()
    for index, url in enumerate(candidates):
        try:
            if index == 0:
                probe = await collect_aave_liquidations_resilient(coverage)
            else:
                probe = await _collect_aave_from_rpc(coverage, url=url)
            probe.detail["rpc_candidate_count"] = len(candidates)
            probe.detail["rpc_transport_fallback_used"] = index > 0
            probe.detail["transport_failures"] = failures
            return probe
        except Exception as exc:
            failures.append(
                {
                    "source_reference": _safe_reference(url),
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:180],
                }
            )
    last = failures[-1] if failures else {"error_type": "ProviderUnavailable"}
    raise RuntimeError(
        f"all Aave Ethereum RPC transports failed: {last.get('error_type')}"
    )


async def run_critical_source_refresh_once(
    service: RemainingSourceLaneRepairService,
) -> dict[str, object]:
    """Refresh the shortest-TTL lane evidence independently of the slow source tail."""

    latest = await asyncio.to_thread(service.source_coverage.ledger.latest)
    gc.collect()

    # Trade flow repairs two lanes and has the shortest validity window, so it always
    # gets first access to the bounded cadence.
    trade = await service._preflight(
        source_id="public-trade-flow",
        lane_ids=["liquidity_provision", "microstructure"],
        source_reference=f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades",
        refresh_seconds=TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
        timeout_seconds=12.0,
        collector=lambda: collect_coinbase_trade_flow(service.source_coverage),
        latest=latest,
    )

    latest = await asyncio.to_thread(service.source_coverage.ledger.latest)
    gc.collect()
    aave_task = asyncio.create_task(
        service._preflight(
            source_id="aave-liquidations",
            lane_ids=["liquidation_distress"],
            source_reference=_safe_reference(
                os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
            ),
            refresh_seconds=AAVE_PREFLIGHT_REFRESH_SECONDS,
            timeout_seconds=15.0,
            collector=lambda: collect_aave_liquidations_with_transport_fallback(
                service.source_coverage
            ),
            latest=latest,
        )
    )
    hyperliquid_task = asyncio.create_task(
        service._preflight(
            source_id="hyperliquid-distress",
            lane_ids=["liquidation_distress"],
            source_reference=HYPERLIQUID_INFO_URL,
            refresh_seconds=HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
            timeout_seconds=15.0,
            collector=collect_hyperliquid_distress_resilient,
            latest=latest,
        )
    )
    aave, hyperliquid = await asyncio.gather(aave_task, hyperliquid_task)
    transfer = await asyncio.to_thread(service.capital_transfer_evidence.status)
    gc.collect()
    return {
        "trade_flow": trade,
        "aave": aave,
        "hyperliquid": hyperliquid,
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
    """Run critical source freshness on its own start-to-start cadence.

    The general priority-source cycle may legitimately spend minutes in slower
    provider/database work. This loop prevents that long tail from starving the
    120-second trade-flow contract or the liquidation/distress refreshes.
    """

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
                for row in (result["trade_flow"], result["aave"], result["hyperliquid"])
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
                        "message": str(exc)[:500],
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
