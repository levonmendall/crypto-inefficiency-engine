from __future__ import annotations

from inefficiency_engine import threaded_worker as _base
from inefficiency_engine.worker_children_all_lanes import (
    run_portfolio_child,
    run_research_child,
)


async def run_threaded_worker(store, *, settings=None):
    # The consolidated Render process uses threaded_worker directly. Swap only the
    # child entrypoints so all existing watchdog/restart/memory semantics remain.
    _base.run_research_child = run_research_child
    _base.run_portfolio_child = run_portfolio_child
    return await _base.run_threaded_worker(store, settings=settings)
