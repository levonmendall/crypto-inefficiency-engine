from __future__ import annotations

import gc
import os
from datetime import datetime, timezone

from inefficiency_engine.evidence_velocity import prioritize_source_probes, stagnation_diagnostics
from inefficiency_engine.memory_budget import (
    DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB,
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


SOURCE_REFRESH_WORKER_ID = "priority-source-refresh-plane"
# Collection TTLs remain deliberately faster than the source-coverage validity
# windows. They control refresh effort, not qualification authority.
SOURCE_REFRESH_TTL_SECONDS: dict[str, float] = {
    "bybit-liquidations": 90.0,
    "aave-liquidations": 180.0,
    "snapshot-governance": 600.0,
    "morpho-markets": 300.0,
    "bybit-options": 120.0,
    "okx-options": 120.0,
    "defillama-protocols": 900.0,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PrioritySourceCollectionService(ResilientProviderGapCollectionService):
    """Refresh priority evidence independently, dynamically, and fail closed.

    Memory pressure is an internal scheduling condition, not provider-health evidence.
    Source ordering is now driven by distance to the next evidence gate and durable
    stagnation signals. Poor economics/statistical failure never trigger threshold
    relaxation; they deliberately receive no automatic repair priority boost.
    """

    def __init__(self, *, source_coverage: SourceCoveragePlane, yield_service, **kwargs):
        super().__init__(**kwargs)
        self.source_coverage = source_coverage
        self.yield_service = yield_service
        self.memory_soft_limit_mb = max(
            128.0,
            float(
                os.getenv(
                    "CIE_RESEARCH_MEMORY_SOFT_LIMIT_MB",
                    str(DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB),
                )
            ),
        )

    def _record_probe(self, probe: SourceProbeResult) -> None:
        for lane_id, classes in probe.evidence_by_lane.items():
            self.source_coverage.record(
                SourceCoverageObservation(
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
                )
            )

    def _record_failure(
        self,
        source_id: str,
        lane_ids: list[str],
        source_reference: str,
        exc: Exception,
    ) -> None:
        for lane_id in lane_ids:
            self.source_coverage.record(
                SourceCoverageObservation(
                    source_id=source_id,
                    lane_id=lane_id,
                    healthy=False,
                    item_count=0,
                    source_reference=source_reference,
                    error_type=type(exc).__name__,
                    detail={"message": str(exc)[:300]},
                )
            )

    def _source_is_fresh(
        self,
        source_id: str,
        lane_ids: list[str],
        ttl_seconds: float,
    ) -> bool:
        latest = self.source_coverage.ledger.latest()
        now = _now()
        for lane_id in lane_ids:
            row = latest.get((source_id, lane_id))
            if row is None or not row.healthy:
                return False
            age = max(0.0, (now - row.observed_at).total_seconds())
            if age > max(1.0, ttl_seconds):
                return False
        return True

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

    def _record_refresh_heartbeat(self, *, state: str, **detail: object) -> None:
        try:
            self.store.record_worker_heartbeat(
                worker_id=SOURCE_REFRESH_WORKER_ID,
                state=state,
                detail={
                    "independent_source_refresh": True,
                    "dynamic_distance_to_gate_scheduler": True,
                    "automatic_stagnation_remediation": True,
                    "memory_deferral_preserves_last_truthful_observation": True,
                    "qualification_thresholds_unchanged": True,
                    "paper_only": True,
                    "allocation_authority": False,
                    "live_execution_authority": False,
                    **detail,
                },
            )
        except Exception:
            pass

    async def run_cycle(self) -> dict[str, object]:
        self._record_refresh_heartbeat(state="running", stage="provider_admission")
        base = await super().run_cycle()
        self._record_memory("base_provider_gap_complete")
        eth_source = _safe_reference(os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL))
        probes = (
            (
                "bybit-liquidations",
                ["liquidation_distress", "microstructure"],
                BYBIT_LINEAR_WS,
                lambda: collect_bybit_liquidations(self.source_coverage),
            ),
            (
                "aave-liquidations",
                ["liquidation_distress"],
                eth_source,
                lambda: collect_aave_liquidations(self.source_coverage),
            ),
            (
                "snapshot-governance",
                ["event_driven"],
                SNAPSHOT_GRAPHQL_URL,
                lambda: collect_snapshot_governance(self.source_coverage, self.alpha_factory),
            ),
            (
                "morpho-markets",
                ["yield", "fundamental_onchain"],
                MORPHO_GRAPHQL_URL,
                lambda: collect_morpho_markets(self.source_coverage, self.yield_service),
            ),
            (
                "bybit-options",
                ["volatility"],
                f"{BYBIT_BASE_URLS[0]}/v5/market/tickers",
                lambda: collect_bybit_options(self.volatility_service),
            ),
            (
                "okx-options",
                ["volatility"],
                f"{OKX_BASE_URL}/api/v5/public/opt-summary",
                lambda: collect_okx_options(self.volatility_service),
            ),
            (
                "defillama-protocols",
                ["fundamental_onchain"],
                DEFILLAMA_PROTOCOLS_URL,
                collect_defillama_protocols,
            ),
        )

        coverage_before = self.source_coverage.snapshot()
        ordered_probes = prioritize_source_probes(
            self.store,
            coverage_before.lanes,
            probes,
        )
        priority: dict[str, object] = {}
        memory_by_source: dict[str, dict[str, float | None]] = {}
        deferred_sources: list[str] = []
        cached_sources: list[str] = []
        failed_sources: list[str] = []
        refreshed_sources: list[str] = []

        for source_id, lane_ids, reference, collector in ordered_probes:
            ttl_seconds = SOURCE_REFRESH_TTL_SECONDS[source_id]
            if self._source_is_fresh(source_id, lane_ids, ttl_seconds):
                cached_sources.append(source_id)
                priority[source_id] = {
                    "healthy": True,
                    "refresh_state": "fresh_cached",
                    "refresh_ttl_seconds": ttl_seconds,
                }
                continue

            if memory_budget_exceeded(self.memory_soft_limit_mb):
                deferred_sources.append(source_id)
                priority[source_id] = {
                    "healthy": None,
                    "item_count": 0,
                    "refresh_state": "memory_deferred",
                    "error_type": "MemoryBudgetDeferred",
                    "memory_deferred": True,
                    "preserved_previous_source_observation": True,
                    "refresh_ttl_seconds": ttl_seconds,
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
                refreshed_sources.append(source_id)
                priority[source_id] = {
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "authoritative": probe.authoritative,
                    "refresh_state": "refreshed",
                    "refresh_ttl_seconds": ttl_seconds,
                }
                del probe
            except Exception as exc:
                self._record_failure(source_id, lane_ids, reference, exc)
                failed_sources.append(source_id)
                priority[source_id] = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                    "refresh_state": "provider_failed",
                    "refresh_ttl_seconds": ttl_seconds,
                }
            finally:
                gc.collect()
                memory_by_source[source_id] = self._record_memory(
                    "priority_source_complete",
                    source_id=source_id,
                )

        coverage = self.source_coverage.snapshot()
        stagnation = stagnation_diagnostics(
            self.store,
            lane_ids=[row.lane_id for row in coverage.lanes],
        )
        final_memory = self._record_memory(
            "source_coverage_snapshot_complete",
            sufficient_lane_count=coverage.sufficient_lane_count,
            forward_test_eligible_lane_count=coverage.forward_test_eligible_lane_count,
        )
        refresh_state = "degraded" if (deferred_sources or failed_sources) else "success"
        self._record_refresh_heartbeat(
            state=refresh_state,
            stage="complete",
            sufficient_lane_count=coverage.sufficient_lane_count,
            forward_test_eligible_lane_count=coverage.forward_test_eligible_lane_count,
            dynamic_lane_priority_order=coverage.priority_order,
            refreshed_sources=refreshed_sources,
            fresh_cached_sources=cached_sources,
            memory_deferred_sources=deferred_sources,
            failed_sources=failed_sources,
            stagnant_lane_count=sum(item.stagnant for item in stagnation.values()),
        )
        return {
            **base,
            "priority_sources": priority,
            "source_coverage": {
                "lane_count": coverage.lane_count,
                "sufficient_lane_count": coverage.sufficient_lane_count,
                "insufficient_lane_count": coverage.insufficient_lane_count,
                "research_eligible_lane_count": coverage.research_eligible_lane_count,
                "forward_test_eligible_lane_count": coverage.forward_test_eligible_lane_count,
                "allocation_source_qualified_lane_count": (
                    coverage.allocation_source_qualified_lane_count
                ),
                "priority_order": coverage.priority_order,
            },
            "source_refresh": {
                "state": refresh_state,
                "refreshed_sources": refreshed_sources,
                "fresh_cached_sources": cached_sources,
                "memory_deferred_sources": deferred_sources,
                "failed_sources": failed_sources,
                "source_specific_ttls": dict(SOURCE_REFRESH_TTL_SECONDS),
                "dynamic_distance_to_gate_scheduler": True,
                "dynamic_lane_priority_order": coverage.priority_order,
            },
            "stagnation_control": {
                "window_snapshots": 50,
                "automatic_priority_only": True,
                "qualification_thresholds_unchanged": True,
                "lanes": {
                    lane_id: diagnostic.as_dict()
                    for lane_id, diagnostic in stagnation.items()
                },
            },
            "memory_budget": {
                "soft_limit_mb": self.memory_soft_limit_mb,
                "by_source": memory_by_source,
                "final": final_memory,
            },
            "paper_only": True,
            "live_execution_authority": False,
        }


class SourceCoverageAwareOperatingCertificationService(
    ResilientProviderGapAwareOperatingCertificationService
):
    """Existing operating certification plus the source coverage plane."""

    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(
            core,
            store,
            alpha_factory,
            allocation_certification,
            version=version,
        )
        self.source_coverage = SourceCoveragePlane(store)
        self.provider_gap_collection = PrioritySourceCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
            yield_service=self.yield_service,
            source_coverage=self.source_coverage,
        )
