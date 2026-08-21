from __future__ import annotations

import gc

from inefficiency_engine.memory_budget import MemoryBudgetDeferred, memory_budget_exceeded
from inefficiency_engine.priority_source_collection import (
    PrioritySourceCollectionService,
    SourceCoverageAwareOperatingCertificationService,
)
from inefficiency_engine.trade_flow import BYBIT_LINEAR_WS, collect_bybit_trade_flow


class ExecutablePrioritySourceCollectionService(PrioritySourceCollectionService):
    """Priority source collection plus empirical public taker-flow evidence."""

    async def run_cycle(self) -> dict[str, object]:
        result = await super().run_cycle()
        source_id = "public-trade-flow"
        lanes = ["microstructure", "liquidity_provision"]
        if memory_budget_exceeded(self.memory_soft_limit_mb):
            exc = MemoryBudgetDeferred(
                f"public trade-flow probe deferred above {self.memory_soft_limit_mb:.0f} MiB RSS soft limit"
            )
            self._record_failure(source_id, lanes, BYBIT_LINEAR_WS, exc)
            trade_flow = {
                "healthy": False,
                "item_count": 0,
                "error_type": "MemoryBudgetDeferred",
                "memory_deferred": True,
            }
        else:
            try:
                probe = await collect_bybit_trade_flow(self.source_coverage)
                if probe.item_count <= 0:
                    raise ValueError("public trade-flow subscription produced no point-in-time trades")
                self._record_probe(probe)
                trade_flow = {
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "authoritative": probe.authoritative,
                }
            except Exception as exc:
                self._record_failure(source_id, lanes, BYBIT_LINEAR_WS, exc)
                trade_flow = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                }
            finally:
                gc.collect()
                self._record_memory("public_trade_flow_complete", source_id=source_id)

        priority = dict(result.get("priority_sources") or {})
        priority[source_id] = trade_flow
        coverage = self.source_coverage.snapshot()
        result["priority_sources"] = priority
        result["source_coverage"] = {
            "lane_count": coverage.lane_count,
            "sufficient_lane_count": coverage.sufficient_lane_count,
            "insufficient_lane_count": coverage.insufficient_lane_count,
            "priority_order": coverage.priority_order,
        }
        return result


class ExecutableSourceCoverageAwareOperatingCertificationService(
    SourceCoverageAwareOperatingCertificationService
):
    """Source-aware certification with real public trade flow wired in."""

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
