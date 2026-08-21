from __future__ import annotations

import gc

from inefficiency_engine.evidence_velocity import stagnation_diagnostics
from inefficiency_engine.memory_budget import memory_budget_exceeded
from inefficiency_engine.priority_source_collection import (
    PrioritySourceCollectionService,
    SourceCoverageAwareOperatingCertificationService,
)
from inefficiency_engine.trade_flow_integrity import (
    BYBIT_TRADE_WS,
    OKX_TRADE_WS,
    collect_multi_venue_trade_flow,
)


PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS = 30.0


class ExecutablePrioritySourceCollectionService(PrioritySourceCollectionService):
    """Priority collection plus bounded multi-venue public taker-flow evidence."""

    async def run_cycle(self) -> dict[str, object]:
        result = await super().run_cycle()
        source_id = "public-trade-flow"
        lanes = ["microstructure", "liquidity_provision"]

        if self._source_is_fresh(source_id, lanes, PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS):
            trade_flow = {
                "healthy": True,
                "refresh_state": "fresh_cached",
                "refresh_ttl_seconds": PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS,
            }
        elif memory_budget_exceeded(self.memory_soft_limit_mb):
            trade_flow = {
                "healthy": None,
                "item_count": 0,
                "error_type": "MemoryBudgetDeferred",
                "memory_deferred": True,
                "refresh_state": "memory_deferred",
                "preserved_previous_source_observation": True,
                "refresh_ttl_seconds": PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS,
            }
            gc.collect()
            self._record_memory("public_trade_flow_deferred", source_id=source_id)
        else:
            try:
                probe = await collect_multi_venue_trade_flow(self.source_coverage)
                if probe.item_count <= 0:
                    raise ValueError(
                        "public trade-flow streams produced no point-in-time trades"
                    )
                self._record_probe(probe)
                trade_flow = {
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "authoritative": probe.authoritative,
                    "refresh_state": "refreshed",
                    "refresh_ttl_seconds": PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS,
                    "stream_integrity": probe.detail.get("stream_integrity"),
                    "venues_healthy": probe.detail.get("venues_healthy"),
                    "venue_errors": probe.detail.get("venue_errors"),
                }
            except Exception as exc:
                self._record_failure(
                    source_id,
                    lanes,
                    f"{BYBIT_TRADE_WS};{OKX_TRADE_WS}",
                    exc,
                )
                trade_flow = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                    "refresh_state": "provider_failed",
                    "refresh_ttl_seconds": PUBLIC_TRADE_FLOW_REFRESH_TTL_SECONDS,
                }
            finally:
                gc.collect()
                self._record_memory("public_trade_flow_complete", source_id=source_id)

        priority = dict(result.get("priority_sources") or {})
        priority[source_id] = trade_flow
        refresh = dict(result.get("source_refresh") or {})
        refresh["public_trade_flow"] = trade_flow
        coverage = self.source_coverage.snapshot()
        stagnation = stagnation_diagnostics(
            self.store,
            lane_ids=[row.lane_id for row in coverage.lanes],
        )
        refresh["dynamic_lane_priority_order"] = coverage.priority_order
        result["priority_sources"] = priority
        result["source_refresh"] = refresh
        result["source_coverage"] = {
            "lane_count": coverage.lane_count,
            "sufficient_lane_count": coverage.sufficient_lane_count,
            "insufficient_lane_count": coverage.insufficient_lane_count,
            "research_eligible_lane_count": coverage.research_eligible_lane_count,
            "forward_test_eligible_lane_count": coverage.forward_test_eligible_lane_count,
            "allocation_source_qualified_lane_count": (
                coverage.allocation_source_qualified_lane_count
            ),
            "priority_order": coverage.priority_order,
        }
        result["stagnation_control"] = {
            "window_snapshots": 50,
            "automatic_priority_only": True,
            "qualification_thresholds_unchanged": True,
            "lanes": {
                lane_id: diagnostic.as_dict()
                for lane_id, diagnostic in stagnation.items()
            },
        }
        return result


class ExecutableSourceCoverageAwareOperatingCertificationService(
    SourceCoverageAwareOperatingCertificationService
):
    """Source-aware certification with event-integrity trade flow wired in."""

    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(
            core,
            store,
            alpha_factory,
            allocation_certification,
            version=version,
        )
        self.provider_gap_collection = ExecutablePrioritySourceCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
            yield_service=self.yield_service,
            source_coverage=self.source_coverage,
        )
