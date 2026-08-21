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
