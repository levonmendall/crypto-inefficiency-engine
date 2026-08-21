from __future__ import annotations

import asyncio
from types import SimpleNamespace

from inefficiency_engine.cex_dex_canonical_runtime import (
    CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService


def test_lightweight_allocator_reads_durable_bridge_without_research_factory(tmp_path):
    store = EvidenceStore(tmp_path / "portfolio.sqlite3")
    core = OpportunityService(settings=Settings(), evidence_store=store)
    durable_handle = SimpleNamespace(store=store)
    allocator = CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService(
        core,
        None,
        durable_handle,
    )

    plan = asyncio.run(allocator.allocate(total_capital_usd=250_000.0))

    assert plan.allocations == []
    assert any(
        item.get("family") == "qualified_opportunity_bridge"
        for item in plan.family_failures
    )
    assert plan.paper_only is True
    assert plan.authorizes_execution is False
