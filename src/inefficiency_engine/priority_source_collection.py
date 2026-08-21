from __future__ import annotations

import os

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
    """Collect the five priority evidence families after the established resilient probes.

    Every new source is isolated. A failed optional source can reduce source coverage,
    but it cannot suppress the canonical market scan, another provider, or the
    portfolio worker. Source observations remain diagnostic/research evidence only.
    """

    def __init__(self, *, source_coverage: SourceCoveragePlane, yield_service, **kwargs):
        super().__init__(**kwargs)
        self.source_coverage = source_coverage
        self.yield_service = yield_service

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
                source_id=source_id, lane_id=lane_id, healthy=False, item_count=0,
                source_reference=source_reference, error_type=type(exc).__name__,
                detail={"message":str(exc)[:300]},
            ))

    async def run_cycle(self) -> dict[str, object]:
        base = await super().run_cycle()
        eth_source = _safe_reference(os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL))
        # Ordered deliberately by marginal expected value: liquidation -> event -> yield -> options -> fundamentals.
        probes = (
            ("bybit-liquidations", ["liquidation_distress","microstructure"], BYBIT_LINEAR_WS, lambda: collect_bybit_liquidations(self.source_coverage)),
            ("aave-liquidations", ["liquidation_distress"], eth_source, lambda: collect_aave_liquidations(self.source_coverage)),
            ("snapshot-governance", ["event_driven"], SNAPSHOT_GRAPHQL_URL, lambda: collect_snapshot_governance(self.source_coverage,self.alpha_factory)),
            ("morpho-markets", ["yield","fundamental_onchain"], MORPHO_GRAPHQL_URL, lambda: collect_morpho_markets(self.source_coverage,self.yield_service)),
            ("bybit-options", ["volatility"], f"{BYBIT_BASE_URLS[0]}/v5/market/tickers", lambda: collect_bybit_options(self.volatility_service)),
            ("okx-options", ["volatility"], f"{OKX_BASE_URL}/api/v5/public/opt-summary", lambda: collect_okx_options(self.volatility_service)),
            ("defillama-protocols", ["fundamental_onchain"], DEFILLAMA_PROTOCOLS_URL, collect_defillama_protocols),
        )
        priority: dict[str, object] = {}
        for source_id, lane_ids, reference, collector in probes:
            try:
                probe = await collector()
                self._record_probe(probe)
                priority[source_id] = {
                    "healthy":True,
                    "item_count":probe.item_count,
                    "source_reference":probe.source_reference,
                    "authoritative":probe.authoritative,
                }
            except Exception as exc:
                self._record_failure(source_id,lane_ids,reference,exc)
                priority[source_id] = {"healthy":False,"item_count":0,"error_type":type(exc).__name__}
        coverage = self.source_coverage.snapshot()
        return {
            **base,
            "priority_sources": priority,
            "source_coverage": {
                "lane_count":coverage.lane_count,
                "sufficient_lane_count":coverage.sufficient_lane_count,
                "insufficient_lane_count":coverage.insufficient_lane_count,
                "priority_order":coverage.priority_order,
            },
            "paper_only":True,
            "live_execution_authority":False,
        }


class SourceCoverageAwareOperatingCertificationService(ResilientProviderGapAwareOperatingCertificationService):
    """Existing operating certification plus the non-authoritative source plane."""
    def __init__(self, core, store, alpha_factory, allocation_certification, *, version: str):
        super().__init__(core,store,alpha_factory,allocation_certification,version=version)
        self.source_coverage = SourceCoveragePlane(store)
        self.provider_gap_collection = PrioritySourceCollectionService(
            store=store,
            alpha_factory=alpha_factory,
            admissions=self.provider_admissions,
            volatility_service=self.volatility_service,
            yield_service=self.yield_service,
            source_coverage=self.source_coverage,
        )
