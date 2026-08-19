from __future__ import annotations

import inspect

from inefficiency_engine.canonical_allocator import CanonicalPortfolioAllocatorService
from inefficiency_engine.resilient_paper_portfolio import OperationallyResilientPaperPortfolioService


def test_canonical_accounting_does_not_fan_out_full_executability_scan():
    source = inspect.getsource(OperationallyResilientPaperPortfolioService.run_cycle)

    assert "collect_live_evidence()" in source
    assert "collect_live_executability()" not in source


def test_canonical_allocator_accepts_fresh_market_snapshot_without_global_l2_requirement():
    source = inspect.getsource(CanonicalPortfolioAllocatorService._latest_market_snapshot)

    assert "store.scans.c.scan_id" in source
    assert "store.order_books" not in source
    assert "snapshot.market_quotes" in source
