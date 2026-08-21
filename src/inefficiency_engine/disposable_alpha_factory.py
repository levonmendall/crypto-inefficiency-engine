from __future__ import annotations

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.batched_cycle_history import BatchedCycleHistoricalResearch
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessMechanismExecutionService,
)


class DisposableExpandedAlphaFactoryService(AllLaneEvidenceFactoryService):
    """Production disposable all-lane research factory.

    Research consumes persisted history but never performs network backfill in the
    disposable research process. It does, however, run the executable alpha
    refinements and all five native mechanism forward loops. Mechanism outcomes are
    also fed into the Release D subtractive lane-success calibration plane.

    The production evidence cycle uses the existing bounded executability snapshot
    rather than the quote-only snapshot. This activates strategies and mechanism
    lanes that require current order books without creating a second L2 fanout path;
    the same adapter registry, batching, timeouts, memory limits and fail-closed
    executability collection remain authoritative.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._historical_research = BatchedCycleHistoricalResearch(self.store)
        self.mechanism_execution = EvidenceVelocityLaneSuccessMechanismExecutionService(
            self.core,
            self.store,
        )

    async def _ensure_historical_research(self) -> None:
        # A separate history subprocess owns all network backfill. Research may read
        # whatever durable history already exists, but it never expands the archive.
        self._historical_backfill_attempted = True
        self._historical_backfill_report = None

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        """Run alpha + mechanism evidence with bounded live L2 attached.

        Base alpha/mechanism code asks the core for ``collect_live_evidence``. In the
        disposable production process only, route that call through the already
        bounded ``collect_live_executability`` path for the duration of this cycle.
        This makes ``snapshot.order_books`` real for microstructure, liquidity-
        conditioned reversal and maker research while preserving the established
        collection limits and restoring the original core method immediately after.
        """

        original = self.core.collect_live_evidence
        self.core.collect_live_evidence = self.core.collect_live_executability
        try:
            return await super().run_evidence_cycle(
                total_capital_usd=total_capital_usd
            )
        finally:
            self.core.collect_live_evidence = original
