from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Awaitable, Callable

import httpx

from inefficiency_engine.capital_transfer_evidence import CapitalTransferEvidenceLedger
from inefficiency_engine.coinbase_trade_flow import COINBASE_EXCHANGE_URL, collect_coinbase_trade_flow
from inefficiency_engine.memory_budget import memory_budget_exceeded
from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_parsers import (
    AAVE_LIQUIDATION_TOPIC,
    AAVE_V3_ETHEREUM_POOL,
    parse_aave_liquidation_log,
    stable_id,
)
from inefficiency_engine.provider_gap_collection import DEFAULT_ETHEREUM_RPC_URL, _safe_reference
from inefficiency_engine.provider_gap_resilience import HYPERLIQUID_INFO_URL, _hyperliquid_context_rows
from inefficiency_engine.source_coverage import SourceCoveragePlane, SourceEventObservation


# These are collection cadences, not source-validity or qualification thresholds.
# They deliberately leave headroom inside the unchanged evidence freshness windows:
# liquidation_events=300s, distress_state=900s, trade_flow=120s.
AAVE_PREFLIGHT_REFRESH_SECONDS = 120.0
HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS = 300.0
TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS = 45.0
AAVE_LOOKBACK_WINDOWS = (512, 128, 32)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def collect_aave_liquidations_resilient(
    coverage: SourceCoveragePlane,
) -> SourceProbeResult:
    """Collect recent Aave LiquidationCall logs with a bounded range fallback.

    Some public Ethereum RPCs reject a broad ``eth_getLogs`` window even while
    ordinary finalized-state reads remain healthy. A smaller recent block window is
    still authoritative point-in-time liquidation evidence; shrinking the query is a
    transport compatibility fallback, not a relaxed evidence threshold.
    """

    url = os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
    source = _safe_reference(url)
    timeout = httpx.Timeout(4.0, connect=3.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Cache-Control": "no-cache", "User-Agent": "crypto-inefficiency-engine/source-lane-repair"},
    ) as client:
        async def rpc(method: str, params: list[object]) -> object:
            last_error: Exception | None = None
            attempts = 2 if method == "eth_blockNumber" else 1
            for attempt in range(attempts):
                try:
                    response = await client.post(
                        url,
                        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict) or payload.get("error"):
                        raise ValueError(f"Ethereum RPC {method} failed")
                    return payload.get("result")
                except (httpx.HTTPError, ValueError) as exc:
                    last_error = exc
                    if attempt + 1 < attempts:
                        await asyncio.sleep(0.15)
            assert last_error is not None
            raise last_error

        latest_raw = await rpc("eth_blockNumber", [])
        latest = int(str(latest_raw), 16)
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
                failures.append(
                    {
                        "lookback_blocks": lookback,
                        "error_type": type(exc).__name__,
                        "message": str(exc)[:160],
                    }
                )
                await asyncio.sleep(0.1)
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
            "requested_lookback_blocks": AAVE_LOOKBACK_WINDOWS[0],
            "lookback_blocks": selected_lookback,
            "range_fallback_used": selected_lookback != AAVE_LOOKBACK_WINDOWS[0],
            "range_failures": failures,
            "economic_opportunity_complete": False,
            "qualification_thresholds_unchanged": True,
        },
    )


async def collect_hyperliquid_distress_resilient() -> SourceProbeResult:
    """Retry the existing Hyperliquid distress surface within a bounded budget."""

    last_error: Exception | None = None
    timeout = httpx.Timeout(5.0, connect=2.5)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Cache-Control": "no-cache", "User-Agent": "crypto-inefficiency-engine/source-lane-repair"},
    ) as client:
        for attempt in range(1, 4):
            try:
                response = await client.post(HYPERLIQUID_INFO_URL, json={"type": "metaAndAssetCtxs"})
                response.raise_for_status()
                rows = _hyperliquid_context_rows(response.json())
                if not rows:
                    raise ValueError("Hyperliquid distress surface returned no usable contexts")
                return SourceProbeResult(
                    source_id="hyperliquid-distress",
                    item_count=len(rows),
                    source_reference=HYPERLIQUID_INFO_URL,
                    evidence_by_lane={"liquidation_distress": ["distress_state"]},
                    detail={
                        "attempt": attempt,
                        "bounded_transport_retries": True,
                        "economic_opportunity_complete": False,
                        "qualification_thresholds_unchanged": True,
                    },
                )
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.2 * attempt)
    assert last_error is not None
    raise last_error


class RemainingSourceLaneRepairService(PrioritySourceCollectionService):
    """Repair the four current source gaps without changing any qualification gate.

    Repair order:
      1. liquidation/distress transport resilience;
      2. trade-flow freshness for liquidity-provision and microstructure;
      3. initialize the strict verified-transfer evidence sink for capital location.

    The first two repairs collect genuine existing provider evidence. The capital
    location repair deliberately creates no synthetic transfer observation: that lane
    remains fail-closed until a verified external transfer is actually recorded.
    """

    def __init__(self, *, source_coverage: SourceCoveragePlane, yield_service, **kwargs):
        store = kwargs.get("store")
        if store is None:
            raise ValueError("remaining source-lane repair requires a durable evidence store")
        super().__init__(source_coverage=source_coverage, yield_service=yield_service, **kwargs)
        self.capital_transfer_evidence = CapitalTransferEvidenceLedger(store)

    async def _preflight(
        self,
        *,
        source_id: str,
        lane_ids: list[str],
        source_reference: str,
        refresh_seconds: float,
        timeout_seconds: float,
        collector: Callable[[], Awaitable[SourceProbeResult]],
        latest: dict[tuple[str, str], object],
    ) -> dict[str, object]:
        if self._source_is_fresh(
            source_id,
            lane_ids,
            refresh_seconds,
            latest=latest,
        ):
            return {
                "source_id": source_id,
                "state": "fresh_cached",
                "refresh_seconds": refresh_seconds,
            }
        if memory_budget_exceeded(self.memory_soft_limit_mb):
            return {
                "source_id": source_id,
                "state": "memory_deferred",
                "refresh_seconds": refresh_seconds,
                "preserved_previous_source_observation": True,
            }
        try:
            probe = await asyncio.wait_for(collector(), timeout=max(1.0, timeout_seconds))
            await asyncio.to_thread(self._record_probe, probe)
            return {
                "source_id": source_id,
                "state": "refreshed",
                "item_count": probe.item_count,
                "source_reference": probe.source_reference,
                "refresh_seconds": refresh_seconds,
            }
        except Exception as exc:
            await asyncio.to_thread(
                self._record_failure,
                source_id,
                lane_ids,
                source_reference,
                exc,
            )
            return {
                "source_id": source_id,
                "state": "provider_failed",
                "error_type": type(exc).__name__,
                "refresh_seconds": refresh_seconds,
            }

    async def run_cycle(self) -> dict[str, object]:
        latest = await asyncio.to_thread(self.source_coverage.ledger.latest)
        ethereum_source = _safe_reference(
            os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL)
        )

        # Highest-priority repair: restore both independent liquidation/distress
        # evidence classes before slower event/yield/options work can consume cadence.
        aave_result, hyperliquid_result = await asyncio.gather(
            self._preflight(
                source_id="aave-liquidations",
                lane_ids=["liquidation_distress"],
                source_reference=ethereum_source,
                refresh_seconds=AAVE_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=15.0,
                collector=lambda: collect_aave_liquidations_resilient(self.source_coverage),
                latest=latest,
            ),
            self._preflight(
                source_id="hyperliquid-distress",
                lane_ids=["liquidation_distress"],
                source_reference=HYPERLIQUID_INFO_URL,
                refresh_seconds=HYPERLIQUID_DISTRESS_PREFLIGHT_REFRESH_SECONDS,
                timeout_seconds=18.0,
                collector=collect_hyperliquid_distress_resilient,
                latest=latest,
            ),
        )

        # Second repair: refresh trade flow at the front of every priority cycle with
        # enough headroom that its unchanged 120-second evidence TTL is not crossed.
        latest = await asyncio.to_thread(self.source_coverage.ledger.latest)
        trade_result = await self._preflight(
            source_id="public-trade-flow",
            lane_ids=["liquidity_provision", "microstructure"],
            source_reference=f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades",
            refresh_seconds=TRADE_FLOW_PREFLIGHT_REFRESH_SECONDS,
            timeout_seconds=15.0,
            collector=lambda: collect_coinbase_trade_flow(self.source_coverage),
            latest=latest,
        )

        # Third repair is architectural and intentionally fail-closed: the durable
        # verified-transfer sink now exists, but no observation is invented.
        transfer_status = await asyncio.to_thread(self.capital_transfer_evidence.status)

        result = await super().run_cycle()
        result["remaining_source_lane_repair"] = {
            "repair_order": [
                "liquidation_distress",
                "liquidity_provision_and_microstructure",
                "capital_location_settlement",
            ],
            "liquidation_distress": {
                "aave": aave_result,
                "hyperliquid": hyperliquid_result,
            },
            "trade_flow": trade_result,
            "capital_transfer_evidence": transfer_status,
            "provider_policy_unchanged": True,
            "source_freshness_thresholds_unchanged": True,
            "qualification_thresholds_unchanged": True,
            "synthetic_evidence_created": False,
            "paper_only": True,
            "allocation_authority": False,
            "live_execution_authority": False,
        }
        return result
