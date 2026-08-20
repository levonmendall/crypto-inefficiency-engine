from __future__ import annotations

import inspect

from inefficiency_engine.canonical_allocator import CanonicalPortfolioAllocatorService
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService


def test_canonical_accounting_stops_before_global_opportunity_analysis_and_l2():
    run_source = inspect.getsource(OperationallyResilientPaperPortfolioService.run_cycle)
    collect_source = inspect.getsource(
        OperationallyResilientPaperPortfolioService._collect_canonical_market_snapshot
    )

    assert "_collect_canonical_market_snapshot()" in run_source
    assert "collect_live_evidence()" not in run_source
    assert "collect_live_executability()" not in run_source
    assert "adapter_registry.collect_inputs()" in collect_source
    assert "opportunities=[]" in collect_source
    assert ".analyze(" not in collect_source


def test_market_provenance_is_checkpointed_before_allocator_can_timeout():
    source = inspect.getsource(OperationallyResilientPaperPortfolioService.run_cycle)

    checkpoint_index = source.index("self.integrity.record(checkpoint)")
    allocator_index = source.index("await self.allocator.allocate")
    assert checkpoint_index < allocator_index
    assert 'cycle_status="accounting_only"' in source
    assert "market_snapshot_id=snapshot.scan_id" in source


def test_canonical_allocator_accepts_fresh_market_snapshot_without_global_l2_requirement():
    source = inspect.getsource(CanonicalPortfolioAllocatorService._latest_market_snapshot)

    assert "store.scans.c.scan_id" in source
    assert "store.order_books" not in source
    assert "snapshot.market_quotes" in source
