from __future__ import annotations

from datetime import datetime, timezone

from inefficiency_engine.executable_lane_runtime import (
    AllLaneAllocationForwardCertificationService,
    AllLaneOperationallyResilientPaperPortfolioService,
    AllLaneQualifiedOpportunityAllocatorService,
    ExecutableMechanismExecutionService,
)
from inefficiency_engine.lane_success import LaneSuccessController
from inefficiency_engine.mechanism_execution import MECHANISM_IDS
from inefficiency_engine.qualified_opportunity import allocate_prequalified_candidates
from inefficiency_engine.unified_allocation import UnifiedPaperAllocationPlan


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LaneSuccessMechanismExecutionService(ExecutableMechanismExecutionService):
    """Mechanism forward evidence with regime tags and universal outcome feedback."""

    def __init__(self, core, store):
        super().__init__(core, store)
        self.lane_success = LaneSuccessController(store)

    def discover_specs(self, snapshot, *, total_capital_usd: float):
        regime = self.lane_success.market_regime(now=snapshot.completed_at)
        rows = super().discover_specs(snapshot, total_capital_usd=total_capital_usd)
        result = []
        for spec in rows:
            payload = dict(spec.settlement_payload)
            payload["lane_success_regime"] = regime
            payload["lane_success_regime_conditioning"] = True
            result.append(spec.model_copy(update={"settlement_payload": payload}))
        return result

    def _outcome(self, trial, settlement):
        outcome = super()._outcome(trial, settlement)
        self.lane_success.record_mechanism_outcome(
            trial,
            outcome,
            settlement_detail=dict(settlement.detail or {}),
        )
        return outcome


class LaneSuccessQualifiedOpportunityAllocatorService(AllLaneQualifiedOpportunityAllocatorService):
    """Apply subtractive lane calibration before the existing portfolio allocator."""

    def __init__(self, core, cex_dex, alpha_factory):
        super().__init__(core, cex_dex, alpha_factory)
        store = getattr(alpha_factory, "store", None)
        if store is None:
            raise RuntimeError("lane-success allocator requires durable evidence")
        self.lane_success = LaneSuccessController(store)
        self.mechanisms = LaneSuccessMechanismExecutionService(core, store)

    def allocate_sync(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        """Run the lane-success allocator as explicit synchronous durable-state work.

        The allocation logic has always been database/CPU bound even though its
        public API is async for compatibility with the broader allocator interface.
        Exposing the synchronous core lets the liveness-critical canonical portfolio
        isolate this work from its asyncio event loop and apply a real wall-clock
        deadline without changing any qualification, ranking, or risk rule.
        """

        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        bridge_candidates, failures, bridge_stale = self._active_candidates_with_diagnostics()
        mechanism_candidates, mechanism_stale = self._mechanism_proxy_candidates(
            total_capital_usd=total_capital_usd
        )
        current_regime = self.lane_success.market_regime()
        adjusted, lane_skipped, diagnostics = self.lane_success.adjust_and_diversify(
            [*bridge_candidates, *mechanism_candidates],
            total_capital_usd=total_capital_usd,
            regime=current_regime,
        )

        # The mature allocator's risk/venue/asset/conflict gates remain unchanged.
        # Only ranking uses capital velocity. Economic values are restored immediately
        # after selection so no profitability threshold or accounting value is changed.
        economics = {str(item.candidate_id): item for item in adjusted}
        decision_by_id = {
            str(row.get("candidate_id")): row
            for row in diagnostics
            if isinstance(row, dict) and row.get("candidate_id")
        }
        rank_proxies = []
        for item in adjusted:
            decision = decision_by_id.get(str(item.candidate_id), {})
            velocity = max(0.0, float(decision.get("capital_velocity_score") or 0.0))
            rank_proxies.append(
                item.model_copy(
                    update={
                        "expected_return_on_reserved_capital": velocity,
                        "expected_profit_usd_per_deployment": (
                            float(item.capital_required_usd) * velocity
                        ),
                    }
                )
            )

        plan = allocate_prequalified_candidates(
            self.settings,
            candidates=rank_proxies,
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )
        converted = []
        for allocation in plan.allocations:
            source = economics.get(str(allocation.candidate_id))
            restored = allocation
            if source is not None:
                restored = allocation.model_copy(
                    update={
                        "expected_profit_usd_per_deployment": source.expected_profit_usd_per_deployment,
                        "expected_return_on_reserved_capital": source.expected_return_on_reserved_capital,
                        "source_return_metric": source.source_return_metric,
                        "source_return_value": source.source_return_value,
                    }
                )
            if restored.opportunity_id in MECHANISM_IDS:
                restored = restored.model_copy(update={"family": "mechanism"})
            converted.append(restored)

        expected_profit = sum(
            float(item.expected_profit_usd_per_deployment) for item in converted
        )
        weighted_return = (
            sum(
                float(item.capital_required_usd)
                * float(item.expected_return_on_reserved_capital)
                for item in converted
            )
            / total_capital_usd
        )
        return plan.model_copy(
            update={
                "rank_basis": (
                    "lane_success_calibrated_expected_profit_per_reserved_capital_per_hour"
                ),
                "expected_profit_usd_current_deployments": expected_profit,
                "weighted_expected_return_on_reserved_capital": weighted_return,
                "allocations": converted,
                "skipped": [
                    *bridge_stale,
                    *mechanism_stale,
                    *lane_skipped,
                    *plan.skipped,
                ],
            }
        )

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        # Compatibility surface for research/certification callers. The canonical
        # account detects allocate_sync and moves it off its event loop.
        return self.allocate_sync(
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )


class LaneSuccessAllocationForwardCertificationService(
    AllLaneAllocationForwardCertificationService
):
    """Feed every settled allocator trial back into universal calibration/decay."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.lane_success = LaneSuccessController(store)
        self.mechanisms = LaneSuccessMechanismExecutionService(core, store)

    def trial_from_allocation(self, allocation, *, plan_observed_at: datetime):
        trial = super().trial_from_allocation(
            allocation,
            plan_observed_at=plan_observed_at,
        )
        self.lane_success.record_allocation_forecast(trial)
        return trial

    def _settle_trial(self, trial, snapshot):
        outcome = super()._settle_trial(trial, snapshot)
        if outcome is not None:
            self.lane_success.record_allocation_outcome(trial, outcome)
        return outcome


class LaneSuccessOperationallyResilientPaperPortfolioService(
    AllLaneOperationallyResilientPaperPortfolioService
):
    """Canonical portfolio with the lane-success feedback plane installed."""

    def __init__(self, core, allocator, store):
        super().__init__(core, allocator, store)
        self.settlement = LaneSuccessAllocationForwardCertificationService(
            core,
            allocator,
            store,
        )
