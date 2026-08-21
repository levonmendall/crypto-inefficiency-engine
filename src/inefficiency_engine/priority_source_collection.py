from __future__ import annotations

import gc
import os

from inefficiency_engine.memory_budget import (
    DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB,
    MemoryBudgetDeferred,
    memory_budget_exceeded,
    memory_snapshot,
)
from inefficiency_engine.priority_source_event_yield import (
    DEFILLAMA_PROTOCOLS_URL,
    MORPHO_GRAPHQL_URL,
    SNAPSHOT_GRAPHQL_URL,
    collect_defillama_protocols,
    collect_morpho_markets,
    collect_snapshot_governance,
)
from inefficiency_engine.priority_source_liquidation import (
    BYBIT_LINEAR_WS,
    collect_aave_liquidations,
    collect_bybit_liquidations,
)
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.priority_source_options import OKX_BASE_URL, collect_bybit_options, collect_okx_options
from inefficiency_engine.provider_gap_collection import DEFAULT_ETHEREUM_RPC_URL, _safe_reference
from inefficiency_engine.provider_gap_resilience import (
    BYBIT_BASE_URLS,
    ResilientProviderGapAwareOperatingCertificationService,
    ResilientProviderGapCollectionService,
)
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane


class PrioritySourceCollectionService(ResilientProviderGapCollectionService):
    """Collect the five priority evidence families under a process memory budget.

    Every source remains isolated and sequential. Once current RSS reaches the
    configured soft limit, optional priority probes are deferred and recorded as
    unavailable rather than allowing research collection to trigger a Render OOM.
    This can reduce source coverage for that cycle, but it cannot create allocation
    authority or weaken any downstream profitability, statistical or settlement gate.
    """

    def __init__(self, *, source_coverage: SourceCoveragePlane, yield_service, **kwargs):
        super().__init__(**kwargs)
        self.source_coverage = source_coverage
        self.yield_service = yield_service
        self.memory_soft_limit_mb = max(
            128.0,
            float(os.getenv(
                "CIE_RESEARCH_MEMORY_SOFT_LIMIT_MB",
                str(DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB),
            )),
        )

    def _record_probe(self, probe: SourceProbeResult) -> None:
        for lane_id, classes in probe.evidence_by_lane.items():
            self.source_coverage.record(SourceCoverageObservation(
                source_id=probe.source_id,
                lane_id=lane_id,
                healthy=True,
                item_count=probe.item_count,
                evidence_classes=classes,
                authoritative=probe.authoritative,
                commercial_use_permitted=probe.commercial_use_permitted,
                point_in_time=probe.point_in_time,
                source_reference=probe.source_reference,
                economic_fields_complete=probe.economic_fields_complete,
                forward_testable_evidence=probe.forward_testable_evidence,
                detail=probe.detail,
            ))

    def _record_failure(self, source_id: str, lane_ids: list[str], source_reference: str, exc: Exception) -> None:
        for lane_id in lane_ids:
            self.source_coverage.record(SourceCoverageObservation(
                source_id=source_id,
                lane_id=lane_id,
                healthy=False,
                item_count=0,
                source_reference=source_reference,
                error_type=type(exc).__name__,
                detail={"message": str(exc)[:300]},
            ))

    def _record_memory(self, stage: str, **detail: object) -> dict[str, float | None]:
        snapshot = memory_snapshot()
        rss = snapshot.get("rss_mb")
        over_budget = bool(rss is not None and rss >= self.memory_soft_limit_mb)
        try:
            self.store.record_worker_heartbeat(
                worker_id="source-coverage-memory-budget",
                state="degraded" if over_budget else "running",
                detail={
                    "stage": stage,
                    **snapshot,
                    "soft_limit_mb": self.memory_soft_limit_mb,
                    "memory_budget_exceeded": over_budget,
                    "paper_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    **detail,
                },
            )
        except Exception:
            pass
        return snapshot

    async def run_cycle(self) -> dict[str, object]:
        base = await super().run_cycle()
        self._record_memory("base_provider_gap_complete")
        eth_source = _safe_reference(os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL))
        # Ordered deliberately by marginal expected value: liquidation -> event -> yield -> options -> fundamentals.
        probes = (
            ("bybit-liquidations", ["liquidation_distress", "microstructure"], BYBIT_LINEAR_WS, lambda: collect_bybit_liquidations(self.source_coverage)),
            ("aave-liquidations", ["liquidation_distress"], eth_source, lambda: collect_aave_liquidations(self.source_coverage)),
            ("snapshot-governance", ["event_driven"], SNAPSHOT_GRAPHQL_URL, lambda: collect_snapshot_governance(self.source_coverage, self.alpha_factory)),
            ("morpho-markets", ["yield", "fundamental_onchain"], MORPHO_GRAPHQL_URL, lambda: collect_morpho_markets(self.source_coverage, self.yield_service)),
            ("bybit-options", ["volatility"], f"{BYBIT_BASE_URLS[0]}/v5/market/tickers", lambda: collect_bybit_options(self.volatility_service)),
            ("okx-options", ["volatility"], f"{OKX_BASE_URL}/api/v5/public/opt-summary", lambda: collect_okx_options(self.volatility_service)),
            ("defillama-protocols", ["fundamental_onchain"], DEFILLAMA_PROTOCOLS_URL, collect_defillama_protocols),
        )
        priority: dict[str, object] = {}
        memory_by_source: dict[str, dict[str, float | None]] = {}
        for source_id, lane_ids, reference, collector in probes:
            if memory_budget_exceeded(self.memory_soft_limit_mb):
                exc = MemoryBudgetDeferred(
                    f"optional source probe deferred above {self.memory_soft_limit_mb:.0f} MiB RSS soft limit"
                )
                self._record_failure(source_id, lane_ids, reference, exc)
                priority[source_id] = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": "MemoryBudgetDeferred",
                    "memory_deferred": True,
                }
                gc.collect()
                memory_by_source[source_id] = self._record_memory(
                    "priority_source_deferred",
                    source_id=source_id,
                )
                continue
            try:
                probe = await collector()
                self._record_probe(probe)
                priority[source_id] = {
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "authoritative": probe.authoritative,
                }
                del probe
            except Exception as exc:
                self._record_failure(source_id, lane_ids, reference, exc)
                priority[source_id] = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                }
            finally:
                gc.collect()
                memory_by_source[source_id] = self._record_memory(
                    "priority_source_complete",
                    source_id=source_id,
                )
        coverage = self.source_coverage.snapshot()
        final_memory = self._record_memory(
            "source_coverage_snapshot_complete",
            sufficient_lane_count=coverage.sufficient_lane_count,
        )
        return {
            **base,
            "priority_sources": priority,
            "source_coverage": {
                "lane_count": coverage.lane_count,
                "sufficient_lane_count": coverage.sufficient_lane_count,
                "insufficient_lane_count": coverage.insufficient_lane_count,
                "priority_order": coverage.priority_order,
            },
            "memory_budget": {
                "soft_limit_mb": self.memory_soft_limit_mb,
                "by_source": memory_by_source,
                "final": final_memory,
            },
            "paper_only": True,
            "live_execution_authority": False,
        }


class SourceCoverageAwareOperatingCertificationService(ResilientProviderGapAwareOperatingCertificationService):
    """Existing operating certification plus the non-authoritative source plane."""

    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(core, store, alpha_factory, allocation_certification, version=version)
        self.source_coverage = SourceCoveragePlane(store)
        self.provider_gap_collection = PrioritySourceCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
            yield_service=self.yield_service,
            source_coverage=self.source_coverage,
        )
