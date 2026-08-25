from __future__ import annotations

import asyncio

from inefficiency_engine import permanent_source_plane as source_plane
from inefficiency_engine import permanent_source_worker as base
from inefficiency_engine import production_source_recovery_runtime as recovery_v1
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine.production_source_recovery_v2_runtime import (
    critical_source_refresh_loop,
    install_lido_provider_recovery,
)
from inefficiency_engine.provider_gap_collection import ProviderProbeResult
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService
from inefficiency_engine.source_lane_repair_runtime import (
    RemainingSourceLaneRepairService,
    collect_hyperliquid_distress_resilient,
)


_ORIGINAL_RUN_PERMANENT_SOURCE_WORKER = base.run_permanent_source_worker
_RUNTIME_PATCH_MARKER = "_critical_source_cadence_installed"
AAVE_RPC_FALLBACK_URLS = (
    "https://eth.llamarpc.com",
    "https://cloudflare-eth.com/v1/mainnet",
)
AAVE_TRANSPORT_BUDGET_SECONDS = 4.0


async def _collect_hyperliquid_distress_with_retries(
    self: ResilientProviderGapCollectionService,
) -> ProviderProbeResult:
    """Use the same first-party distress surface with bounded transport retries."""

    probe = await collect_hyperliquid_distress_resilient()
    return ProviderProbeResult(
        mechanism_id="liquidation_distress",
        provider=self.HYPERLIQUID_DISTRESS_PROVIDER,
        item_count=probe.item_count,
        source_reference=probe.source_reference,
        detail={
            **probe.detail,
            "provider_gap_runtime_repair": True,
            "provider_policy_unchanged": True,
        },
    )


async def _run_permanent_source_worker_with_critical_cadence(
    store,
    *,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Run shortest-TTL source recovery independently from the slow priority tail."""

    stop = stop_event or asyncio.Event()
    critical_task = asyncio.create_task(
        critical_source_refresh_loop(store, stop_event=stop),
        name="critical-source-freshness",
    )
    try:
        return await _ORIGINAL_RUN_PERMANENT_SOURCE_WORKER(store, stop_event=stop)
    finally:
        stop.set()
        critical_task.cancel()
        await asyncio.gather(critical_task, return_exceptions=True)


def install_remaining_source_lane_repairs() -> None:
    """Install source-only repairs before the permanent source plane is built."""

    source_plane.PrioritySourceCollectionService = RemainingSourceLaneRepairService
    ResilientProviderGapCollectionService._collect_hyperliquid_distress_surface = (
        _collect_hyperliquid_distress_with_retries
    )
    install_lido_provider_recovery()

    # Preserve the exact Aave V3 Ethereum Pool + LiquidationCall query while adding
    # one independent documented Ethereum JSON-RPC transport. Keep the total fallback
    # sequence inside the existing 15-second preflight boundary.
    recovery_v1.AAVE_RPC_FALLBACK_URLS = AAVE_RPC_FALLBACK_URLS
    recovery_v1.AAVE_TRANSPORT_BUDGET_SECONDS = AAVE_TRANSPORT_BUDGET_SECONDS
    recovery_v2.AAVE_TRANSPORT_BUDGET_SECONDS = AAVE_TRANSPORT_BUDGET_SECONDS

    if not bool(getattr(base, _RUNTIME_PATCH_MARKER, False)):
        base.run_permanent_source_worker = _run_permanent_source_worker_with_critical_cadence
        setattr(base, _RUNTIME_PATCH_MARKER, True)


def main() -> int:
    install_remaining_source_lane_repairs()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
