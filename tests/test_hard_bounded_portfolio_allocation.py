from __future__ import annotations

import asyncio
import inspect
import threading
import time

import inefficiency_engine.lightweight_portfolio_worker as runtime


class _RetryAllocator:
    def __init__(self) -> None:
        self.calls = 0
        self.sentinel = object()

    def allocate_sync(self, *, total_capital_usd: float):
        assert total_capital_usd > 0
        self.calls += 1
        if self.calls == 1:
            time.sleep(1.0)
        return self.sentinel


def _portfolio_with_allocator(allocator):
    portfolio = object.__new__(runtime.PersistedSourceCanonicalPaperPortfolioService)
    portfolio.allocator = allocator
    portfolio._allocation_hard_deadline_enforced = False
    return portfolio


def test_allocation_deadline_interrupts_without_poisoning_next_cycle(monkeypatch):
    monkeypatch.setattr(runtime, "CANONICAL_ALLOCATION_TIMEOUT_SECONDS", 0.05)
    allocator = _RetryAllocator()
    portfolio = _portfolio_with_allocator(allocator)

    before = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("canonical-allocation")
    }
    started = time.monotonic()
    plan, error = asyncio.run(
        portfolio._bounded_allocation_plan(total_capital_usd=100_000.0)
    )
    elapsed = time.monotonic() - started

    assert plan is None
    assert error == "AllocationStageDeadlineExceeded"
    assert elapsed < 0.5
    assert portfolio._allocation_hard_deadline_enforced is True

    # A timed-out call must not survive in a worker and poison the next canonical
    # cycle with AllocationStageStillRunning. The retry runs immediately and returns.
    plan, error = asyncio.run(
        portfolio._bounded_allocation_plan(total_capital_usd=100_000.0)
    )
    assert plan is allocator.sentinel
    assert error is None
    assert allocator.calls == 2

    after = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("canonical-allocation")
    }
    assert after == before


def test_persisted_source_read_does_not_create_executor_thread():
    portfolio = object.__new__(runtime.PersistedSourceCanonicalPaperPortfolioService)
    sentinel = object()
    portfolio._latest_permanent_source_snapshot = lambda: sentinel

    result = asyncio.run(portfolio._collect_canonical_market_snapshot())

    assert result is sentinel
    assert portfolio._current_persisted_source_snapshot is sentinel
    source = inspect.getsource(
        runtime.PersistedSourceCanonicalPaperPortfolioService._collect_canonical_market_snapshot
    )
    assert "to_thread" not in source
    assert "run_in_executor" not in source


def test_portfolio_worker_installs_exact_bounded_database_reads():
    source = inspect.getsource(runtime.run_lightweight_portfolio_worker)

    assert "install_bounded_control_outcome_ledgers()" in source
    assert "install_control_database_timeouts(" in source
    assert "install_control_pool_checkout_timeout(" in source
    assert "allocation_deadline" in source
