from __future__ import annotations

import asyncio

import httpx

from inefficiency_engine import priority_source_collection as priority_sources
from inefficiency_engine import priority_source_event_yield as event_yield
from inefficiency_engine import production_source_recovery_runtime as recovery_v1
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine import source_lane_repair_runtime as lane_repair
from inefficiency_engine.priority_source_models import SourceProbeResult


AAVE_DOCUMENTED_PUBLIC_RPC_FALLBACKS = (
    "https://eth.drpc.org",
    "https://public.1rpc.io/eth",
)
AAVE_SOURCE_PREFLIGHT_TIMEOUT_SECONDS = 30.0
TRADE_FLOW_SOURCE_PREFLIGHT_TIMEOUT_SECONDS = 20.0
DEFILLAMA_REQUEST_TIMEOUT_SECONDS = 5.0
DEFILLAMA_CONNECT_TIMEOUT_SECONDS = 3.0
DEFILLAMA_ATTEMPTS = 3

_PREFLIGHT_PATCH_MARKER = "_remaining_transport_preflight_installed"
_ORIGINAL_PREFLIGHT = lane_repair.RemainingSourceLaneRepairService._preflight


def _transport_preflight_timeout(source_id: str, requested: float) -> float:
    """Give only proven transport-failure sources enough bounded acquisition time.

    These are collection budgets, not source-validity or qualification thresholds.
    Both remain comfortably inside the unchanged evidence freshness windows.
    """

    value = max(1.0, float(requested))
    if source_id == "aave-liquidations":
        return max(value, AAVE_SOURCE_PREFLIGHT_TIMEOUT_SECONDS)
    if source_id == "public-trade-flow":
        return max(value, TRADE_FLOW_SOURCE_PREFLIGHT_TIMEOUT_SECONDS)
    return value


async def _preflight_with_remaining_transport_budget(self, **kwargs):
    source_id = str(kwargs.get("source_id") or "")
    kwargs["timeout_seconds"] = _transport_preflight_timeout(
        source_id,
        float(kwargs.get("timeout_seconds") or 1.0),
    )
    return await _ORIGINAL_PREFLIGHT(self, **kwargs)


def _retryable_http_error(exc: httpx.HTTPStatusError) -> bool:
    response = getattr(exc, "response", None)
    status = int(getattr(response, "status_code", 0) or 0)
    return status in {408, 425, 429} or status >= 500


async def collect_defillama_protocols_resilient() -> SourceProbeResult:
    """Retry the unchanged DefiLlama protocols endpoint with fresh bounded transports."""

    failures: list[dict[str, object]] = []
    last_error: Exception | None = None
    for attempt in range(1, DEFILLAMA_ATTEMPTS + 1):
        try:
            timeout = httpx.Timeout(
                DEFILLAMA_REQUEST_TIMEOUT_SECONDS,
                connect=DEFILLAMA_CONNECT_TIMEOUT_SECONDS,
            )
            async with httpx.AsyncClient(
                timeout=timeout,
                headers={
                    "Cache-Control": "no-cache",
                    "User-Agent": "crypto-inefficiency-engine/source-coverage-resilient",
                },
            ) as client:
                response = await client.get(event_yield.DEFILLAMA_PROTOCOLS_URL)
                response.raise_for_status()
                payload = response.json()
            rows = (
                [
                    row
                    for row in payload
                    if isinstance(row, dict)
                    and row.get("name")
                    and row.get("tvl") is not None
                ]
                if isinstance(payload, list)
                else []
            )
            if not rows:
                raise ValueError("DefiLlama protocols returned no usable protocol metrics")
            return SourceProbeResult(
                source_id="defillama-protocols",
                item_count=len(rows),
                source_reference=event_yield.DEFILLAMA_PROTOCOLS_URL,
                evidence_by_lane={"fundamental_onchain": ["protocol_fundamentals"]},
                authoritative=False,
                economic_fields_complete=False,
                detail={
                    "secondary_discovery_only": True,
                    "alpha_authority": False,
                    "transport_attempt": attempt,
                    "transport_retry_failures": failures,
                    "same_defillama_endpoint": True,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                },
            )
        except httpx.HTTPStatusError as exc:
            last_error = exc
            failures.append(
                {
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "status_code": int(exc.response.status_code),
                }
            )
            if not _retryable_http_error(exc):
                raise
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            failures.append(
                {
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                }
            )
        if attempt < DEFILLAMA_ATTEMPTS:
            await asyncio.sleep(0.2 * attempt)

    assert last_error is not None
    raise last_error


def _append_aave_public_rpc_fallbacks() -> None:
    ordered: list[str] = []
    for value in (
        *tuple(recovery_v1.AAVE_RPC_FALLBACK_URLS),
        *AAVE_DOCUMENTED_PUBLIC_RPC_FALLBACKS,
    ):
        endpoint = str(value or "").strip()
        if endpoint and endpoint not in ordered:
            ordered.append(endpoint)
    recovery_v1.AAVE_RPC_FALLBACK_URLS = tuple(ordered)


def install_remaining_source_transport_repairs() -> None:
    """Install transport-only repairs for the three currently failed source surfaces."""

    _append_aave_public_rpc_fallbacks()

    # The normal priority path previously used older one-shot Aave/Coinbase collectors,
    # which could overwrite a successful critical-cadence observation with a newer
    # transport failure. Route both owners through the already-tested resilient
    # collectors so the evidence source and required product breadth remain identical.
    lane_repair.collect_aave_liquidations_resilient = (
        recovery_v2.collect_aave_liquidations_resilient_v2
    )
    lane_repair.collect_coinbase_trade_flow = recovery_v2.collect_coinbase_trade_flow_resilient

    # The priority source module imported this function by name, so patch both the
    # defining module and its runtime binding. The endpoint and evidence classification
    # are unchanged; only fresh-client transport retries are added.
    event_yield.collect_defillama_protocols = collect_defillama_protocols_resilient
    priority_sources.collect_defillama_protocols = collect_defillama_protocols_resilient

    service = lane_repair.RemainingSourceLaneRepairService
    if not bool(getattr(service, _PREFLIGHT_PATCH_MARKER, False)):
        service._preflight = _preflight_with_remaining_transport_budget
        setattr(service, _PREFLIGHT_PATCH_MARKER, True)
