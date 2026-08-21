from types import SimpleNamespace

import pytest

from inefficiency_engine.research_reset_guardrails import (
    BoundedResearchResetAllLaneEvidenceFactoryService,
)
from inefficiency_engine.research_reset_runtime import ResearchResetAllLaneEvidenceFactoryService


@pytest.mark.asyncio
async def test_shadow_schedule_is_bounded_across_parent_and_reset(monkeypatch):
    class Ledger:
        def __init__(self):
            self.persisted = 0

        def record_shadow_signal(self, observation):
            self.persisted += 1
            return True

    async def parent_cycle(self, *, total_capital_usd=None):
        for index in range(175):
            self.candidate_observatory.record_shadow_signal(SimpleNamespace(index=index))
        return "cycle"

    monkeypatch.setenv("CIE_RESEARCH_RESET_MAX_DIAGNOSTIC_SHADOWS_PER_CYCLE", "120")
    monkeypatch.setattr(
        ResearchResetAllLaneEvidenceFactoryService,
        "run_evidence_cycle",
        parent_cycle,
    )
    service = object.__new__(BoundedResearchResetAllLaneEvidenceFactoryService)
    service.candidate_observatory = Ledger()

    result = await service.run_evidence_cycle(total_capital_usd=100_000.0)

    assert result == "cycle"
    assert service.candidate_observatory.persisted == 120
