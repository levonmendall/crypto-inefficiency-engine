from __future__ import annotations

import inspect

from inefficiency_engine import lightweight_portfolio_worker


def test_permanent_portfolio_process_uses_durable_bridge_without_alpha_factory():
    source = inspect.getsource(lightweight_portfolio_worker)

    assert "ExpandedAlphaFactoryService" not in source
    assert "DisposableExpandedAlphaFactoryService" not in source
    assert "_DurableQualifiedStateHandle" in source
    assert "CexDexFreshnessSeparatedQualifiedOpportunityAllocatorService" in source
    assert "CexDexUniversalOperationallyResilientPaperPortfolioService" in source
