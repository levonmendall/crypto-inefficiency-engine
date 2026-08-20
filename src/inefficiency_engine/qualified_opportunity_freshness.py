from __future__ import annotations

from datetime import datetime, timedelta, timezone

from inefficiency_engine.memory_bounded_qualified_opportunity import (
    MemoryBoundedQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunityAllocatorService,
    QualifiedOpportunitySnapshot,
    _candidate_has_canonical_settlement,
    allocate_prequalified_candidates,
)
from inefficiency_engine.unified_allocation import UnifiedPaperAllocationPlan, UnifiedPaperCandidate, _core_candidates


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _candidate_freshness_seconds(settings) -> float:
    return max(30.0, float(getattr(settings, "max_quote_age_seconds", 120.0)))


def _bridge_control_freshness_seconds(settings) -> float:
    """Operational TTL for the bridge itself, never for candidate authority.

    The research worker publishes near the beginning of a sequential cycle and can
    then spend a full 60-second DEX shadow horizon plus additional bounded research
    stages before publishing again. Treating the market quote TTL as the control-plane
    TTL therefore creates false degraded cycles. Candidate evidence is filtered
    independently, so a longer bridge-control TTL cannot authorize stale positions.
    """

    horizons = tuple(getattr(settings, "shadow_horizons_seconds", (60.0,)) or (60.0,))
    max_horizon = max((float(value) for value in horizons), default=60.0)
    cycle_interval = max(1.0, float(getattr(settings, "shadow_cycle_interval_seconds", 30.0)))
    heartbeat_stale = max(1.0, float(getattr(settings, "worker_heartbeat_stale_seconds", 180.0)))
    return max(
        600.0,
        heartbeat_stale * 2.0,
        (max_horizon + cycle_interval) * 6.0,
        _candidate_freshness_seconds(settings),
    )


class FreshnessSeparatedQualifiedOpportunityBridgePublisher(
    MemoryBoundedQualifiedOpportunityBridgePublisher
):
    """Publish a long-lived control envelope containing short-lived candidates.

    The envelope says only that the bridge completed successfully. Every candidate
    still has to pass an independent point-in-time freshness check in the canonical
    allocator before it can reserve paper capital.
    """

    async def publish_latest(
        self,
        *,
        total_capital_usd: float,
    ) -> QualifiedOpportunitySnapshot | None:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        snapshot = self._latest_scan()
        if snapshot is None:
            return None

        now = _now()
        candidate_freshness = _candidate_freshness_seconds(self.core.settings)
        source_age = max(0.0, (now - snapshot.completed_at).total_seconds())
        if source_age > candidate_freshness:
            return None

        rows: list[UnifiedPaperCandidate] = []
        failures: list[dict[str, object]] = []

        try:
            rows.extend(_core_candidates(snapshot.opportunities, snapshot.executability))
        except Exception as exc:
            failures.append(
                {
                    "family": "core_cex",
                    "error_type": type(exc).__name__,
                    "reason": "core CEX bridge projection failed closed",
                }
            )

        if self.allocator.alpha_factory is not None:
            try:
                rows.extend(
                    await self.allocator._alpha_family_candidates(
                        snapshot=snapshot,
                        total_capital_usd=total_capital_usd,
                    )
                )
            except Exception as exc:
                failures.append(
                    {
                        "family": "alpha",
                        "error_type": type(exc).__name__,
                        "reason": "alpha bridge projection failed closed",
                    }
                )

        deployable = [
            item
            for item in rows
            if item.allocation_eligible and _candidate_has_canonical_settlement(item)
        ]
        deployable.sort(
            key=lambda item: (
                item.expected_return_on_reserved_capital,
                item.expected_profit_usd_per_deployment,
                -item.capital_required_usd,
            ),
            reverse=True,
        )
        result = QualifiedOpportunitySnapshot(
            observed_at=snapshot.completed_at,
            expires_at=now
            + timedelta(seconds=_bridge_control_freshness_seconds(self.core.settings)),
            source_scan_id=snapshot.scan_id,
            total_capital_usd=total_capital_usd,
            candidates=deployable,
            family_failures=failures,
        )
        self.ledger.record(result)
        return result


class FreshnessSeparatedQualifiedOpportunityAllocatorService(
    QualifiedOpportunityAllocatorService
):
    """Consume an operational bridge without ever consuming stale candidate evidence."""

    def _active_candidates_with_diagnostics(
        self,
    ) -> tuple[
        list[UnifiedPaperCandidate],
        list[dict[str, object]],
        list[dict[str, object]],
    ]:
        snapshot = self.qualified_ledger.latest_active()
        if snapshot is None:
            return [], [
                {
                    "family": "qualified_opportunity_bridge",
                    "error_type": "QualifiedOpportunitySnapshotUnavailableOrStale",
                    "reason": (
                        "canonical portfolio requires a recent successful bridge control envelope; "
                        "research is never rerun inside the accounting hot path"
                    ),
                }
            ], []

        current = _now()
        max_age = _candidate_freshness_seconds(self.settings)
        deployable: list[UnifiedPaperCandidate] = []
        stale_skips: list[dict[str, object]] = []
        for item in snapshot.candidates:
            if not _candidate_has_canonical_settlement(item):
                continue
            source_at = item.source_observed_at
            age = (
                (current - source_at).total_seconds()
                if source_at is not None
                else None
            )
            if age is None or age < 0.0 or age > max_age:
                stale_skips.append(
                    {
                        "candidate_id": item.candidate_id,
                        "family": item.family,
                        "reason": "candidate evidence stale; awaiting fresh research qualification",
                        "source_observed_at": source_at.isoformat() if source_at is not None else None,
                        "max_age_seconds": max_age,
                    }
                )
                continue
            deployable.append(item)

        return deployable, list(snapshot.family_failures), stale_skips

    async def _candidates_with_failures(
        self,
        *,
        total_capital_usd: float,
    ) -> tuple[list[UnifiedPaperCandidate], list[dict[str, object]]]:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        candidates, failures, _ = self._active_candidates_with_diagnostics()
        return candidates, failures

    async def allocate(
        self,
        *,
        total_capital_usd: float,
        max_venue_fraction: float | None = None,
        max_asset_fraction: float | None = None,
        max_allocations: int | None = None,
    ) -> UnifiedPaperAllocationPlan:
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        candidates, failures, stale_skips = self._active_candidates_with_diagnostics()
        plan = allocate_prequalified_candidates(
            self.settings,
            candidates=candidates,
            family_failures=failures,
            total_capital_usd=total_capital_usd,
            max_venue_fraction=max_venue_fraction,
            max_asset_fraction=max_asset_fraction,
            max_allocations=max_allocations,
        )
        plan.skipped = [*stale_skips, *plan.skipped]
        return plan
