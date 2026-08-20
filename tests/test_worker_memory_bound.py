from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import pytest

import inefficiency_engine.worker_children as worker_children


@pytest.mark.asyncio
async def test_auxiliary_memory_gate_prevents_concurrent_heavy_surfaces():
    active = 0
    peak = 0

    async def heavy(name: str):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return name

    class FakeService:
        settings = SimpleNamespace()

        async def run_shadow_cycle(self):
            return await heavy("core")

    gate = asyncio.Lock()
    serialized_service = worker_children._MemorySerializedService(FakeService(), gate)  # type: ignore[arg-type]
    route = worker_children._serialized_runner(gate, lambda: heavy("route"))
    alpha = worker_children._serialized_runner(gate, lambda: heavy("alpha"))

    results = await asyncio.gather(
        serialized_service.run_shadow_cycle(),
        route(),
        alpha(),
    )

    assert sorted(results) == ["alpha", "core", "route"]
    assert peak == 1


def test_canonical_child_does_not_construct_unused_broad_research_graphs():
    source = inspect.getsource(worker_children.run_portfolio_child)

    assert "UniversalOpportunityService(" not in source
    assert "CexDexCompositeEvidenceService(" not in source
    assert "CexDexPaperPromotionService(" not in source
    assert "CanonicalPortfolioAllocatorService" in source
    assert "alpha_factory" in source
