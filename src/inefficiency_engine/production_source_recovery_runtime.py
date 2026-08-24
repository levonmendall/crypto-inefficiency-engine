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
    collect_coinbase_trade_flow,
)
from inefficiency_engine.critical_source_cadence import critical_source_interval_seconds
from inefficiency_engine.permanent_source_plane import PermanentSourcePlane
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    AAVE_V3_ETHEREUM_POOL,
    parse_aave_liquidation_log,
    stable_id,
)
from inefficiency_engine.provider_gap_collection import (
    DEFAULT_ETHEREUM_RPC_URL,
    LIDO_APR_URL,
    ProviderProbeResult,
    _safe_reference,
)
from inefficiency_engine.provider_gap_resilience import (
    HYPERLIQUID_INFO_URL,
    ResilientProviderGapCollectionService,
)
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation
from inefficiency_engine.source_lane_repair_runtime import (
    AAVE_LOOKBACK_WINDOWS,
    AAVE_PREFLIGHT_REFRESH_SECONDS,
    HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
    TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
    RemainingSourceLaneRepairService,
    collect_hyperliquid_distress_resilient,
)


CRITICAL_SOURCE_CADENCE_WORKER_ID = "critical-source-freshness-plane"
AAVE_RPC_FALLBACK_URLS = ("https://eth.llamarpc.com",)
AAVE_TRANSPORT_BUDGET_SECONDS = 4.5
LIDO_LAST_APR_URL = "https://eth-api.lido.fi/v1/protocol/steth/apr/last"
LIDO_SOURCE_ID = "lido-yield"
LIDO_PREFLIGHT_REFRESH_SECONDS = 600.0
LIDO_REQUEST_TIMEOUT_SECONDS = 4.0
LIDO_ATTEMPTS_PER_ENDPOINT = 2


def _now() -> datetime:
    return datetime.now(timezone.utc)


def aave_rpc_candidates() -> tuple[str, ...]:
    """Return bounded Ethereum RPC transports for the same canonical Aave log query."""

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
    """Collect one point-in-time Aave liquidation surface from one Ethereum RPC."""

    source = _safe_reference(url)
    timeout = httpx.Timeout(2.5, connect=1.5)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={
            "Cache-Control": "no-cache",
            "User-Agent": "crypto-inefficiency-engine/production-source-recovery",
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
        latest_hex = hex(latest)
        logs: list[object] | None = None
        selected_lookback: int | None = None
        failures: list[dict[str, object]] = []
        last_error: Exception | None = None
        for lookback in AAVE_LOOKBACK_WINDOWS:
            try:
                result = await rpc(
                    "eth_getLogs",
                    [
                        {
                            "address": AAVE_V3_ETHEREUM_POOL,
                            "fromBlock": hex(max(0, latest - lookback)),
                            "toBlock": latest_hex,
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
                failures.append(
                    {
                        "lookback_blocks": lookback,
                        "error_type": type(exc).__name__,
                    }
                )
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
            "latest_block": latest,
            "lookback_blocks": selected_lookback,
            "range_failures": failures,
            "exact_to_block": True,
            "qualification_thresholds_unchanged": True,
            "paper_only": True,
        },
    )


async def collect_aave_liquidations_production_resilient(
    coverage: SourceCoveragePlane,
) -> SourceProbeResult:
    """Use bounded alternate RPC transports without changing the Aave evidence source."""

    failures: list[dict[str, object]] = []
    candidates = aave_rpc_candidates()
    for index, url in enumerate(candidates):
        try:
            probe = await asyncio.wait_for(
                _collect_aave_from_rpc(coverage, url=url),
                timeout=AAVE_TRANSPORT_BUDGET_SECONDS,
            )
            probe.detail["rpc_candidate_count"] = len(candidates)
            probe.detail["rpc_transport_fallback_used"] = index > 0
            probe.detail["transport_failures"] = failures
            probe.detail["same_aave_contract_and_topic"] = True
            return probe
        except Exception as exc:
            failures.append(
                {
                    "source_reference": _safe_reference(url),
                    "error_type": type(exc).__name__,
                    "message": str(exc)[:160],
                }
            )

    failure_types = ",".join(str(row["error_type"]) for row in failures) or "unknown"
    raise RuntimeError(f"all Aave Ethereum RPC transports failed: {failure_types}")


def _extract_lido_apr(payload: object) -> float:
    values: list[float] = []

    def walk(value: object, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                walk(child, str(child_key))
            return
        if isinstance(value, list):
            for child in value:
                walk(child, key)
            return
        if "apr" not in key.lower():
            return
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return
        if parsed >= 0.0:
            values.append(parsed)

    walk(payload)
    if not values:
        raise ValueError("Lido APR response did not contain a numeric APR")
    return values[0]


async def collect_lido_yield_resilient() -> SourceProbeResult:
    """Retry Lido's first-party APR API and fall back from SMA to latest APR."""

    endpoints = (LIDO_APR_URL, LIDO_LAST_APR_URL)
    failures: list[dict[str, object]] = []
    timeout = httpx.Timeout(LIDO_REQUEST_TIMEOUT_SECONDS, connect=2.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={
            "Cache-Control": "no-cache",
            "User-Agent": "crypto-inefficiency-engine/production-source-recovery",
        },
    ) as client:
        for endpoint_index, endpoint in enumerate(endpoints):
            for attempt in range(1, LIDO_ATTEMPTS_PER_ENDPOINT + 1):
                try:
                    response = await client.get(endpoint)
                    response.raise_for_status()
                    apr = _extract_lido_apr(response.json())
                    return SourceProbeResult(
                        source_id=LIDO_SOURCE_ID,
                        item_count=1,
                        source_reference=endpoint,
                        evidence_by_lane={"yield": ["yield_rate"]},
                        authoritative=True,
                        commercial_use_permitted=True,
                        point_in_time=True,
                        economic_fields_complete=False,
                        forward_testable_evidence=False,
                        detail={
                            "observed_apr": apr,
                            "metric": "sma_apr" if endpoint_index == 0 else "latest_apr",
                            "attempt": attempt,
                            "first_party_lido_api": True,
                            "endpoint_fallback_used": endpoint_index > 0,
                            "transport_failures": failures,
                            "economic_observation_complete": False,
                            "remaining_required_fields": [
                                "executable capacity",
                                "exit liquidity",
                                "protocol-loss calibration",
                            ],
                            "qualification_thresholds_unchanged": True,
                            "paper_only": True,
                        },
                    )
                except (httpx.HTTPError, ValueError) as exc:
                    failures.append(
                        {
                            "source_reference": endpoint,
                            "error_type": type(exc).__name__,
                        }
                    )
                    if attempt < LIDO_ATTEMPTS_PER_ENDPOINT:
                        await asyncio.sleep(0.15 * attempt)

    failure_types = ",".join(str(row["error_type"]) for row in failures) or "unknown"
    raise RuntimeError(f"all Lido APR attempts failed: {failure_types}")


async def collect_lido_provider_resilient(
    self: ResilientProviderGapCollectionService,
) -> ProviderProbeResult:
    """Expose the same resilient Lido read through the provider-admission plane."""

    probe = await collect_lido_yield_resilient()
    return ProviderProbeResult(
        mechanism_id="yield",
        provider=self.LIDO_PROVIDER,
        item_count=probe.item_count,
        source_reference=probe.source_reference,
        detail={
            **probe.detail,
            "provider_gap_runtime_repair": True,
            "provider_policy_unchanged": True,
        },
    )


async def run_critical_source_refresh_once(
    service: RemainingSourceLaneRepairService,
) -> dict[str, object]:
    """Refresh the short-lived production sources independently from the slow tail."""

    latest = await asyncio.to_thread(service.source_coverage.ledger.latest)
    gc.collect()

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
            timeout_seconds=12.0,
            collector=lambda: collect_aave_liquidations_production_resilient(
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
    lido_task = asyncio.create_task(
        service._preflight(
            source_id=LIDO_SOURCE_ID,
            lane_ids=["yield"],
            source_reference=LIDO_APR_URL,
            refresh_seconds=LIDO_PREFLIGHT_REFRESH_SECONDS,
            timeout_seconds=12.0,
            collector=collect_lido_yield_resilient,
            latest=latest,
        )
    )
    aave, hyperliquid, lido = await asyncio.gather(
        aave_task,
        hyperliquid_task,
        lido_task,
    )
    transfer = await asyncio.to_thread(service.capital_transfer_evidence.status)
    gc.collect()
    return {
        "trade_flow": trade,
        "aave": aave,
        "hyperliquid": hyperliquid,
        "lido": lido,
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
    """Keep trade-flow, liquidation/distress, and Lido recovery on an independent cadence."""

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


def install_lido_provider_recovery() -> None:
    """Patch only the Lido transport implementation; admission semantics are unchanged."""

    ResilientProviderGapCollectionService._collect_lido_yield_surface = (
        collect_lido_provider_resilient
    )
