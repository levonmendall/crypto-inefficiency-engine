from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService,
)
from inefficiency_engine.resilient_paper_portfolio import (
    OperationallyResilientPaperPortfolioService,
)


class _FastSyncAllocator:
    def allocate_sync(self, *, total_capital_usd: float):
        return {"capital": total_capital_usd}


class _SlowSyncAllocator:
    def allocate_sync(self, *, total_capital_usd: float):
        time.sleep(0.15)
        return {"capital": total_capital_usd}


def _portfolio_with_allocator(allocator):
    portfolio = object.__new__(OperationallyResilientPaperPortfolioService)
    portfolio.allocator = allocator
    portfolio._allocation_executor = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="test-canonical-allocation",
    )
    portfolio._allocation_future = None
    return portfolio


@pytest.mark.asyncio
async def test_fast_sync_allocator_runs_off_event_loop_and_returns_plan():
    portfolio = _portfolio_with_allocator(_FastSyncAllocator())
    try:
        plan, error = await portfolio._bounded_allocation_plan(total_capital_usd=250_000.0)
        assert error is None
        assert plan == {"capital": 250_000.0}
        assert portfolio._allocation_future is None
    finally:
        portfolio._allocation_executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
async def test_timed_out_allocator_does_not_spawn_replacement_until_prior_finishes(monkeypatch):
    import inefficiency_engine.resilient_paper_portfolio as module

    monkeypatch.setattr(module, "CANONICAL_ALLOCATION_TIMEOUT_SECONDS", 0.02)
    portfolio = _portfolio_with_allocator(_SlowSyncAllocator())
    try:
        started = time.monotonic()
        plan, error = await portfolio._bounded_allocation_plan(total_capital_usd=250_000.0)
        elapsed = time.monotonic() - started
        assert plan is None
        assert error == "AllocationStageTimeout"
        assert elapsed < 0.10

        first_future = portfolio._allocation_future
        assert first_future is not None
        plan, error = await portfolio._bounded_allocation_plan(total_capital_usd=250_000.0)
        assert plan is None
        assert error == "AllocationStageStillRunning"
        assert portfolio._allocation_future is first_future

        await asyncio.sleep(0.18)
        assert first_future.done()
    finally:
        portfolio._allocation_executor.shutdown(wait=True, cancel_futures=True)


def test_release_d_allocator_exposes_sync_core_for_canonical_isolation():
    assert callable(
        getattr(EvidenceVelocityLaneSuccessQualifiedOpportunityAllocatorService, "allocate_sync", None)
    )
