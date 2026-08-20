from __future__ import annotations

import pytest

from inefficiency_engine.adapters.coinbase import CoinbaseSpotAdapter
from inefficiency_engine.adapters.registry import PublicAdapterRegistry
from inefficiency_engine.asset_universe import (
    DEFAULT_LIQUID_RESEARCH_ASSETS,
    MAX_LIQUID_RESEARCH_ASSETS,
    configured_liquid_research_assets,
)


def test_default_liquid_research_universe_is_broad_and_stablecoin_free():
    assert len(DEFAULT_LIQUID_RESEARCH_ASSETS) == 16
    assert DEFAULT_LIQUID_RESEARCH_ASSETS[:3] == ("BTC", "ETH", "SOL")
    assert {"XRP", "DOGE", "ADA", "AVAX", "LINK", "SUI"}.issubset(
        DEFAULT_LIQUID_RESEARCH_ASSETS
    )
    assert not {"USDT", "USDC", "DAI"}.intersection(DEFAULT_LIQUID_RESEARCH_ASSETS)


def test_liquid_research_universe_supports_bounded_environment_override(monkeypatch):
    monkeypatch.setenv("CIE_LIQUID_RESEARCH_ASSETS", "btc,eth,xrp,btc,sui")
    assert configured_liquid_research_assets() == ("BTC", "ETH", "XRP", "SUI")

    too_many = ",".join(f"A{index}" for index in range(MAX_LIQUID_RESEARCH_ASSETS + 1))
    with pytest.raises(ValueError):
        configured_liquid_research_assets(too_many)


def test_coinbase_and_default_registry_use_expanded_research_universe(monkeypatch):
    monkeypatch.delenv("CIE_LIQUID_RESEARCH_ASSETS", raising=False)
    coinbase = CoinbaseSpotAdapter()
    assert coinbase.assets == DEFAULT_LIQUID_RESEARCH_ASSETS

    registry = PublicAdapterRegistry()
    assert registry.coinbase.assets == DEFAULT_LIQUID_RESEARCH_ASSETS
    assert registry.bybit.assets == DEFAULT_LIQUID_RESEARCH_ASSETS
    assert registry.kraken.assets == DEFAULT_LIQUID_RESEARCH_ASSETS
    assert registry.okx.assets == DEFAULT_LIQUID_RESEARCH_ASSETS
