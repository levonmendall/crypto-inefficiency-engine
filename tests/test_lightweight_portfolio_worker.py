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


def test_permanent_portfolio_process_contains_no_external_source_acquisition():
    source = inspect.getsource(lightweight_portfolio_worker)

    assert "PermanentSourcePlane" not in source
    assert "resolve_top_volume_assets" not in source
    assert "DynamicVolumePublicAdapterRegistry" not in source
    assert "_permanent_source_refresh_loop" not in source
    assert "_volume_universe_refresh_loop" not in source
    assert "provider_calls\": False" in source
