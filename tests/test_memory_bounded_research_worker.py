from __future__ import annotations

import asyncio
import weakref
from types import SimpleNamespace

import pytest

from inefficiency_engine.memory_bounded_research_worker import run_memory_bounded_research_worker


class HeavyResult:
    def __init__(self, *, cycle_id: str = "route"):
        self.cycle_id = cycle_id
        self.observations = []
        self.payload = bytearray(2_000_000)


class FakeStore:
    backend = "test"

    def __init__(self):
        self.heartbeats: list[tuple[str, str]] = []

    def record_worker_heartbeat(self, *, worker_id, state, **kwargs):
        self.heartbeats.append((worker_id, state))


@pytest.mark.asyncio
async def test_memory_bounded_worker_releases_previous_surface_before_next_runs():
    order: list[str] = []
    route_ref: weakref.ReferenceType[HeavyResult] | None = None

    class FakeService:
        settings = SimpleNamespace(
            worker_error_backoff_seconds=0.0,
            shadow_cycle_interval_seconds=0.0,
        )

        async def run_shadow_cycle(self):
            order.append("core")
            return SimpleNamespace(
                cycle_id="core-1",
                verification_scan_id="scan-1",
                observations=[],
            )

    async def route_runner():
        nonlocal route_ref
        order.append("route")
        result = HeavyResult()
        route_ref = weakref.ref(result)
        return result

    async def tier_runner():
        order.append("tier")
        assert route_ref is not None
        assert route_ref() is None
        return SimpleNamespace(cycle_id="tier-1", initial_quote_count=0, observations=[])

    store = FakeStore()
    stats = await run_memory_bounded_research_worker(
        FakeService(),  # type: ignore[arg-type]
        store,  # type: ignore[arg-type]
        worker_id="research-test",
        stop_event=asyncio.Event(),
        max_cycles=1,
        route_shadow_runner=route_runner,
        tier_shadow_runner=tier_runner,
        tier_shadow_every_cycles=1,
    )

    assert order == ["core", "route", "tier"]
    assert stats.cycles_attempted == 1
    assert stats.cycles_succeeded == 1
    assert ("research-test", "success") in store.heartbeats
