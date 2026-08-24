from __future__ import annotations

import asyncio
import gc
import os
from datetime import datetime, timezone

from inefficiency_engine.coinbase_trade_flow import (
    COINBASE_EXCHANGE_URL,
    collect_coinbase_trade_flow,
)
from inefficiency_engine.evidence_velocity import prioritize_source_probes, stagnation_diagnostics
from inefficiency_engine.memory_budget import (
    DEFAULT_RESEARCH_MEMORY_SOFT_LIMIT_MB,
    memory_budget_exceeded,
    memory_snapshot,
)
from inefficiency_engine.option_capacity import (
    DERIBIT_BASE_URL as DERIBIT_CAPACITY_BASE_URL,
    collect_deribit_option_capacity,
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
from inefficiency_engine.runtime_provider_policy import bybit_public_enabled
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane


SOURCE_REFRESH_WORKER_ID = "priority-source-refresh-plane"
ALPHA_L2_WORKER_ID = "alpha-l2-research-sampling"
L2_SOURCE_RECOVERY_STALE_SECONDS = 180.0
BASE_PROVIDER_REFRESH_TTL_SECONDS = 120.0
# These are already-supported authoritative partner surfaces for the five
# provider-gap families. They are collection priorities only; they do not alter
# source sufficiency, qualification, economic, risk, settlement, or allocation gates.
CRITICAL_REDUNDANCY_SOURCES_BY_LANE: dict[str, tuple[str, ...]] = {
    "liquidation_distress": ("aave-liquidations",),
    "event_driven": ("snapshot-governance",),
    "yield": ("morpho-markets",),
    "volatility": ("okx-options", "deribit-option-capacity"),
    "fundamental_onchain": ("morpho-markets",),
}
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
    "deribit-option-capacity": 300.0,
    "public-trade-flow": 60.0,
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
        *,
        latest: dict[tuple[str, str], object] | None = None,
    ) -> bool:
        # The caller may provide one point-in-time latest-state map for the entire
        # source cycle. This avoids one remote PostgreSQL query per source while
        # preserving the same freshness semantics.
        rows = latest if latest is not None else self.source_coverage.ledger.latest()
        now = _now()
        for lane_id in lane_ids:
            row = rows.get((source_id, lane_id))
            if row is None or not bool(getattr(row, "healthy", False)):
                return False
            observed_at = getattr(row, "observed_at", None)
            if not isinstance(observed_at, datetime):
                return False
            age = max(0.0, (now - observed_at).total_seconds())
            if age > max(1.0, ttl_seconds):
                return False
        return True

    def _base_provider_candidates(self) -> dict[str, tuple[str, ...]]:
        """Return only policy-permitted authoritative base-provider identities."""

        if bybit_public_enabled():
            event = (self.BYBIT_CATALOG_PROVIDER, self.COINBASE_CATALOG_PROVIDER)
            distress = (
                self.BYBIT_ADL_PROVIDER,
                self.BYBIT_DISTRESS_PROVIDER,
                self.HYPERLIQUID_DISTRESS_PROVIDER,
            )
        else:
            event = (self.COINBASE_CATALOG_PROVIDER,)
            distress = (self.HYPERLIQUID_DISTRESS_PROVIDER,)
        return {
            "fundamental_onchain": (self.ETHEREUM_PROVIDER,),
            "event_driven": event,
            "yield": (self.LIDO_PROVIDER,),
            "volatility": (self.DERIBIT_PROVIDER,),
            "liquidation_distress": distress,
        }

    def _fresh_base_provider_summary(self) -> dict[str, dict[str, object]] | None:
        """Reuse a very recent admitted base provider instead of refetching all five.

        This TTL is deliberately much shorter than every affected source-coverage
        validity window. It reduces repeated network work in the source-refresh stage
        but never extends evidence freshness or changes qualification authority.
        """

        latest_by_provider = getattr(self.admissions, "latest_by_provider", None)
        if not callable(latest_by_provider):
            return None
        now = _now()
        result: dict[str, dict[str, object]] = {}
        for mechanism_id, allowed_providers in self._base_provider_candidates().items():
            try:
                rows = latest_by_provider(mechanism_id)
            except Exception:
                return None
            candidates: list[tuple[datetime, str, object]] = []
            for provider in allowed_providers:
                row = rows.get(provider) if isinstance(rows, dict) else None
                if row is None or not bool(getattr(row, "admitted", False)):
                    continue
                observed_at = getattr(row, "observed_at", None)
                if not isinstance(observed_at, datetime):
                    continue
                if observed_at.tzinfo is None:
                    observed_at = observed_at.replace(tzinfo=timezone.utc)
                observed_at = observed_at.astimezone(timezone.utc)
                age = max(0.0, (now - observed_at).total_seconds())
                if age > BASE_PROVIDER_REFRESH_TTL_SECONDS:
                    continue
                candidates.append((observed_at, provider, row))
            if not candidates:
                return None
            _, provider, row = max(candidates, key=lambda item: item[0])
            result[mechanism_id] = {
                "provider": provider,
                "healthy": True,
                "item_count": int(getattr(row, "item_count", 0) or 0),
                "source_reference": getattr(row, "source_reference", None),
                "fallback_used": provider != {
                    "fundamental_onchain": self.ETHEREUM_PROVIDER,
                    "event_driven": self.BYBIT_CATALOG_PROVIDER,
                    "yield": self.LIDO_PROVIDER,
                    "volatility": self.DERIBIT_PROVIDER,
                    "liquidation_distress": self.BYBIT_DISTRESS_PROVIDER,
                }[mechanism_id],
                "refresh_state": "fresh_cached",
                "refresh_ttl_seconds": BASE_PROVIDER_REFRESH_TTL_SECONDS,
            }
        return result

    @staticmethod
    def _critical_redundancy_source_ids(coverage: object) -> set[str]:
        """Identify existing partner sources for lanes still short of allocation redundancy."""

        result: set[str] = set()
        for lane in list(getattr(coverage, "lanes", ()) or ()):
            lane_id = str(getattr(lane, "lane_id", "") or "")
            if not lane_id or bool(getattr(lane, "allocation_source_qualified", False)):
                continue
            result.update(CRITICAL_REDUNDANCY_SOURCES_BY_LANE.get(lane_id, ()))
        return result

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
                    "policy_aware_authoritative_redundancy_repair": True,
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

    def _l2_source_refresh_due(self) -> bool:
        """Return true only when the independent bounded L2 sampler needs recovery."""

        try:
            heartbeat = self.store.latest_worker_heartbeat(ALPHA_L2_WORKER_ID)
        except Exception:
            # A failed heartbeat read cannot safely prove another sampler is absent.
            return False
        if heartbeat is None:
            return True
        observed_at = heartbeat.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        age = max(0.0, (_now() - observed_at.astimezone(timezone.utc)).total_seconds())
        return age > L2_SOURCE_RECOVERY_STALE_SECONDS

    async def _refresh_l2_source_if_due(self) -> dict[str, object]:
        """Persist bounded visible L2 as critical source work before heavy research."""

        refresher = getattr(self.alpha_factory, "refresh_l2_source_snapshot", None)
        if not callable(refresher):
            return {"state": "unsupported", "attempted": False}
        l2_due = await asyncio.to_thread(self._l2_source_refresh_due)
        if not l2_due:
            return {"state": "fresh_cached", "attempted": False}
        if memory_budget_exceeded(self.memory_soft_limit_mb):
            return {
                "state": "memory_deferred",
                "attempted": False,
                "preserved_previous_source_observation": True,
            }
        try:
            snapshot = await refresher()
            books = list(getattr(snapshot, "order_books", ()) or ())
            return {
                "state": "refreshed" if books else "degraded",
                "attempted": True,
                "retained_book_count": len(books),
            }
        except Exception as exc:
            return {
                "state": "failed",
                "attempted": True,
                "error_type": type(exc).__name__,
            }
        finally:
            gc.collect()

    async def run_cycle(self) -> dict[str, object]:
        # Durable PostgreSQL reconciliation/writes are synchronous SQLAlchemy calls.
        # Never run them on the source asyncio thread: a slow remote query must not
        # suppress the source worker's independent 30-second progress heartbeat.
        await asyncio.to_thread(
            self._record_refresh_heartbeat,
            state="running",
            stage="provider_admission",
        )
        cached_base = await asyncio.to_thread(self._fresh_base_provider_summary)
        if cached_base is None:
            base = await super().run_cycle()
            base_provider_refresh_state = "refreshed"
        else:
            base = {
                "mechanisms": cached_base,
                "paper_only": True,
                "live_execution_authority": False,
            }
            base_provider_refresh_state = "fresh_cached"
        await asyncio.to_thread(
            self._record_memory,
            "base_provider_gap_complete",
            base_provider_refresh_state=base_provider_refresh_state,
        )

        # Visible L2 is source evidence, not an optional consequence of an already
        # discovered arbitrage/carry candidate. Recover the existing bounded sampler
        # here when its durable heartbeat is absent/stale so L2 persistence can
        # advance before the full alpha/mechanism research tail.
        l2_source_refresh = await self._refresh_l2_source_if_due()
        await asyncio.to_thread(
            self._record_memory,
            "critical_l2_source_refresh_complete",
            l2_source_refresh_state=l2_source_refresh.get("state"),
        )

        eth_source = _safe_reference(os.getenv("CIE_ETHEREUM_RPC_URL", DEFAULT_ETHEREUM_RPC_URL))
        probes: list[tuple[str, list[str], str, object]] = [
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
                "deribit-option-capacity",
                ["volatility"],
                f"{DERIBIT_CAPACITY_BASE_URL}/public/get_order_book",
                lambda: collect_deribit_option_capacity(self.store),
            ),
            (
                "public-trade-flow",
                ["liquidity_provision", "microstructure"],
                f"{COINBASE_EXCHANGE_URL}/products/{{product_id}}/trades",
                lambda: collect_coinbase_trade_flow(self.source_coverage),
            ),
            (
                "defillama-protocols",
                ["fundamental_onchain"],
                DEFILLAMA_PROTOCOLS_URL,
                collect_defillama_protocols,
            ),
        ]

        policy_disabled_sources: list[str] = []
        if not bybit_public_enabled():
            disabled = {"bybit-liquidations", "bybit-options"}
            policy_disabled_sources = sorted(disabled)
            probes = [probe for probe in probes if probe[0] not in disabled]

        coverage_before = await asyncio.to_thread(self.source_coverage.snapshot)
        ordered_probes = await asyncio.to_thread(
            prioritize_source_probes,
            self.store,
            coverage_before.lanes,
            tuple(probes),
        )
        critical_redundancy_sources = self._critical_redundancy_source_ids(coverage_before)
        original_order = {
            str(probe[0]): index for index, probe in enumerate(ordered_probes)
        }
        ordered_probes = sorted(
            ordered_probes,
            key=lambda probe: (
                0 if str(probe[0]) in critical_redundancy_sources else 1,
                original_order.get(str(probe[0]), len(original_order)),
            ),
        )
        # One point-in-time source-state read serves every TTL check in this cycle.
        latest_source_rows = await asyncio.to_thread(self.source_coverage.ledger.latest)
        priority: dict[str, object] = {}
        memory_by_source: dict[str, dict[str, float | None]] = {}
        deferred_sources: list[str] = []
        cached_sources: list[str] = []
        failed_sources: list[str] = []
        refreshed_sources: list[str] = []

        for source_id, lane_ids, reference, collector in ordered_probes:
            ttl_seconds = SOURCE_REFRESH_TTL_SECONDS[source_id]
            if self._source_is_fresh(
                source_id,
                lane_ids,
                ttl_seconds,
                latest=latest_source_rows,
            ):
                cached_sources.append(source_id)
                priority[source_id] = {
                    "healthy": True,
                    "refresh_state": "fresh_cached",
                    "refresh_ttl_seconds": ttl_seconds,
                    "critical_redundancy_source": source_id in critical_redundancy_sources,
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
                    "critical_redundancy_source": source_id in critical_redundancy_sources,
                }
                gc.collect()
                memory_by_source[source_id] = await asyncio.to_thread(
                    self._record_memory,
                    "priority_source_deferred",
                    source_id=source_id,
                )
                continue

            try:
                probe = await collector()
                await asyncio.to_thread(self._record_probe, probe)
                refreshed_sources.append(source_id)
                priority[source_id] = {
                    "healthy": True,
                    "item_count": probe.item_count,
                    "source_reference": probe.source_reference,
                    "authoritative": probe.authoritative,
                    "refresh_state": "refreshed",
                    "refresh_ttl_seconds": ttl_seconds,
                    "critical_redundancy_source": source_id in critical_redundancy_sources,
                }
                del probe
            except Exception as exc:
                await asyncio.to_thread(
                    self._record_failure,
                    source_id,
                    lane_ids,
                    reference,
                    exc,
                )
                failed_sources.append(source_id)
                priority[source_id] = {
                    "healthy": False,
                    "item_count": 0,
                    "error_type": type(exc).__name__,
                    "refresh_state": "provider_failed",
                    "refresh_ttl_seconds": ttl_seconds,
                    "critical_redundancy_source": source_id in critical_redundancy_sources,
                }
            finally:
                gc.collect()
                memory_by_source[source_id] = await asyncio.to_thread(
                    self._record_memory,
                    "priority_source_complete",
                    source_id=source_id,
                )

        coverage = await asyncio.to_thread(self.source_coverage.snapshot)
        stagnation = await asyncio.to_thread(
            stagnation_diagnostics,
            self.store,
            lane_ids=[row.lane_id for row in coverage.lanes],
        )
        final_memory = await asyncio.to_thread(
            self._record_memory,
            "source_coverage_snapshot_complete",
            sufficient_lane_count=coverage.sufficient_lane_count,
            forward_test_eligible_lane_count=coverage.forward_test_eligible_lane_count,
        )
        base_provider_failures = sorted(
            str(mechanism_id)
            for mechanism_id, row in dict(base.get("mechanisms") or {}).items()
            if isinstance(row, dict) and row.get("healthy") is False
        )
        refresh_state = (
            "degraded"
            if (deferred_sources or failed_sources or base_provider_failures)
            else "success"
        )
        await asyncio.to_thread(
            self._record_refresh_heartbeat,
            state=refresh_state,
            stage="complete",
            sufficient_lane_count=coverage.sufficient_lane_count,
            forward_test_eligible_lane_count=coverage.forward_test_eligible_lane_count,
            dynamic_lane_priority_order=coverage.priority_order,
            refreshed_sources=refreshed_sources,
            fresh_cached_sources=cached_sources,
            memory_deferred_sources=deferred_sources,
            failed_sources=failed_sources,
            base_provider_failures=base_provider_failures,
            base_provider_refresh_state=base_provider_refresh_state,
            critical_redundancy_sources=sorted(critical_redundancy_sources),
            policy_disabled_sources=policy_disabled_sources,
            bybit_public_enabled=bybit_public_enabled(),
            l2_source_refresh=l2_source_refresh,
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
                "lanes": {
                    row.lane_id: {
                        "source_state": row.source_state,
                        "missing_evidence_classes": row.missing_evidence_classes,
                        "admitted_authoritative_source_groups": (
                            row.admitted_authoritative_source_groups
                        ),
                        "missing_authoritative_source_count": (
                            row.missing_authoritative_source_count
                        ),
                        "policy_disabled_source_ids": row.policy_disabled_source_ids,
                    }
                    for row in coverage.lanes
                },
            },
            "source_refresh": {
                "state": refresh_state,
                "refreshed_sources": refreshed_sources,
                "fresh_cached_sources": cached_sources,
                "memory_deferred_sources": deferred_sources,
                "failed_sources": failed_sources,
                "base_provider_failures": base_provider_failures,
                "base_provider_refresh_state": base_provider_refresh_state,
                "base_provider_refresh_ttl_seconds": BASE_PROVIDER_REFRESH_TTL_SECONDS,
                "critical_redundancy_sources": sorted(critical_redundancy_sources),
                "policy_disabled_sources": policy_disabled_sources,
                "bybit_public_enabled": bybit_public_enabled(),
                "l2_source_refresh": l2_source_refresh,
                "source_specific_ttls": dict(SOURCE_REFRESH_TTL_SECONDS),
                "dynamic_distance_to_gate_scheduler": True,
                "policy_aware_authoritative_redundancy_repair": True,
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
