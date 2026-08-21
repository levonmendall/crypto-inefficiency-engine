from __future__ import annotations

import os

from inefficiency_engine.research_reset_runtime import (
    ResearchResetAllLaneEvidenceFactoryService,
)


DEFAULT_MAX_DIAGNOSTIC_SHADOWS_PER_CYCLE = 250


class BoundedResearchResetAllLaneEvidenceFactoryService(ResearchResetAllLaneEvidenceFactoryService):
    """Bound reset shadow scheduling without narrowing candidate observability."""

    @staticmethod
    def _shadow_schedule_limit() -> int:
        raw = os.getenv("CIE_RESEARCH_RESET_MAX_DIAGNOSTIC_SHADOWS_PER_CYCLE")
        if raw in (None, ""):
            return DEFAULT_MAX_DIAGNOSTIC_SHADOWS_PER_CYCLE
        return max(100, int(raw))

    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):
        ledger = self.candidate_observatory
        original = ledger.record_shadow_signal
        limit = self._shadow_schedule_limit()
        recorded = 0

        def bounded_record(observation):
            nonlocal recorded
            if recorded >= limit:
                return False
            result = original(observation)
            if result:
                recorded += 1
            return result

        # Both the base observatory and reset use the same ledger method. Applying the
        # guard at the ledger boundary caps *new* shadow signals across the whole cycle
        # while preserving all candidate observations and rejection telemetry.
        ledger.record_shadow_signal = bounded_record
        try:
            return await super().run_evidence_cycle(total_capital_usd=total_capital_usd)
        finally:
            ledger.record_shadow_signal = original
