from __future__ import annotations

import asyncio
from types import SimpleNamespace

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.asset_universe import MAX_LIQUID_RESEARCH_ASSETS
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.disposable_research_worker import _error_keys
from inefficiency_engine.volume_universe import TOP_VOLUME_ASSET_COUNT


def test_compatibility_universe_matches_authoritative_top_volume_count():
    assert MAX_LIQUID_RESEARCH_ASSETS == TOP_VOLUME_ASSET_COUNT == 25


def test_disposable_alpha_cycle_routes_live_evidence_through_bounded_executability(monkeypatch):
    calls: list[str] = []

    async def quote_only():
        calls.append("quote_only")
        return "quote-only"

    async def executable():
        calls.append("executable")
        return "with-l2"

    core = SimpleNamespace(
        collect_live_evidence=quote_only,
        collect_live_executability=executable,
    )
    service = object.__new__(DisposableExpandedAlphaFactoryService)
    service.core = core

    async def fake_parent_run(self, *, total_capital_usd=None):
        return await self.core.collect_live_evidence()

    monkeypatch.setattr(
        AllLaneEvidenceFactoryService,
        "run_evidence_cycle",
        fake_parent_run,
    )

    result = asyncio.run(service.run_evidence_cycle())
    assert result == "with-l2"
    assert calls == ["executable"]
    assert core.collect_live_evidence is quote_only


def test_research_heartbeat_error_inventory_is_explicit():
    detail = {
        "sequence": 7,
        "alpha_forward_evidence_error_type": "RuntimeError",
        "qualified_bridge_error_type": "TimeoutError",
        "alpha_candidate_count": 0,
    }
    assert _error_keys(detail) == [
        "alpha_forward_evidence_error_type",
        "qualified_bridge_error_type",
    ]
