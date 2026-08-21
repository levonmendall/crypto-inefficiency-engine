from __future__ import annotations

import pytest

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.registry import PublicAdapterRegistry
from inefficiency_engine.asset_universe import (
    DEFAULT_LIQUID_RESEARCH_ASSETS,
    MAX_LIQUID_RESEARCH_ASSETS,
    configured_liquid_research_assets,
)


def test_default_liquid_research_universe_is_only_a_constructor_seed():
    assert DEFAULT_LIQUID_RESEARCH_ASSETS == ("BTC",)
    assert len(DEFAULT_LIQUID_RESEARCH_ASSETS) != MAX_LIQUID_RESEARCH_ASSETS
    assert "ALGO" not in DEFAULT_LIQUID_RESEARCH_ASSETS


def test_production_ignores_environment_asset_override(monkeypatch):
    monkeypatch.setenv("CIE_LIQUID_RESEARCH_ASSETS", "btc,eth,xrp,sui")
    # Production top-40 selection is volume-derived; environment lists cannot
    # redefine it. With no durable snapshot, callers only receive the tiny seed.
    assert configured_liquid_research_assets() == ("BTC",)

    # Explicit call-site input remains bounded for unit/dev construction only.
    assert configured_liquid_research_assets("btc,eth,xrp,btc,sui") == (
        "BTC",
        "ETH",
        "XRP",
        "SUI",
    )

    too_many = ",".join(f"A{index}" for index in range(MAX_LIQUID_RESEARCH_ASSETS + 1))
    with pytest.raises(ValueError):
        configured_liquid_research_assets(too_many)


def test_coinbase_and_plain_registry_use_only_constructor_seed_before_dynamic_refresh():
    coinbase = CoinbaseSpotAdapter()
    assert coinbase.assets == ("BTC",)

    registry = PublicAdapterRegistry()
    assert registry.coinbase.assets == ("BTC",)
    assert registry.bybit.assets == ("BTC",)
    assert registry.kraken.assets == ("BTC",)
    assert registry.okx.assets == ("BTC",)
