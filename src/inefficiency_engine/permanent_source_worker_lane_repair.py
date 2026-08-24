from __future__ import annotations

from inefficiency_engine import permanent_source_plane as source_plane
from inefficiency_engine import permanent_source_worker as base
from inefficiency_engine.provider_gap_collection import ProviderProbeResult
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService
from inefficiency_engine.source_lane_repair_runtime import (
    RemainingSourceLaneRepairService,
    collect_hyperliquid_distress_resilient,
)


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


def install_remaining_source_lane_repairs() -> None:
    """Install source-only repairs before the permanent source plane is built."""

    source_plane.PrioritySourceCollectionService = RemainingSourceLaneRepairService
    ResilientProviderGapCollectionService._collect_hyperliquid_distress_surface = (
        _collect_hyperliquid_distress_with_retries
    )


def main() -> int:
    install_remaining_source_lane_repairs()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
