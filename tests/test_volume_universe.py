from __future__ import annotations

from datetime import datetime, timezone

import pytest

from inefficiency_engine.adapters.dynamic_registry import DynamicVolumePublicAdapterRegistry
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.volume_universe import (
    BOOTSTRAP_LIQUID_ASSETS,
    TOP_VOLUME_ASSET_COUNT,
    VolumeUniverseLedger,
    parse_bybit_turnover,
    parse_hyperliquid_turnover,
    rank_volume_observations,
    resolve_top_volume_assets,
)


def _snapshot(now: datetime) -> dict[str, object]:
    return {
        "observed_at": now.isoformat(),
        "method": "aggregate_24h_traded_notional",
        "asset_count": 40,
        "stable_value_assets_excluded": True,
        "source_health": {},
        "assets": [
            {
                "rank": index,
                "asset": asset,
                "aggregate_24h_notional_usd": float(41 - index) * 1_000_000.0,
                "sources": ["test"],
            }
            for index, asset in enumerate(BOOTSTRAP_LIQUID_ASSETS, start=1)
        ],
        "paper_only": True,
        "allocation_authority": False,
    }


def test_volume_ranking_aggregates_sources_and_excludes_stables():
    observations = [
        {"asset": "BTC", "notional_usd": 10.0, "source": "a"},
        {"asset": "BTC", "notional_usd": 15.0, "source": "b"},
        {"asset": "ETH", "notional_usd": 20.0, "source": "a"},
        {"asset": "USDT", "notional_usd": 1_000_000.0, "source": "a"},
    ]
    ranked = rank_volume_observations(observations, limit=2)
    assert [row["asset"] for row in ranked] == ["BTC", "ETH"]
    assert ranked[0]["aggregate_24h_notional_usd"] == 25.0
    assert ranked[0]["sources"] == ["a", "b"]


def test_bybit_and_hyperliquid_volume_parsers_normalize_multiplier_contracts():
    bybit = parse_bybit_turnover(
        {
            "retCode": 0,
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "turnover24h": "1000"},
                    {"symbol": "1000PEPEUSDT", "turnover24h": "500"},
                    {"symbol": "USDCUSDT", "turnover24h": "999999"},
                ]
            },
        },
        source="bybit:test",
    )
    assert {row["asset"] for row in bybit} == {"BTC", "PEPE"}

    hyper = parse_hyperliquid_turnover(
        [
            {"universe": [{"name": "BTC"}, {"name": "kPEPE"}]},
            [{"dayNtlVlm": "2000"}, {"dayNtlVlm": "700"}],
        ]
    )
    assert {row["asset"] for row in hyper} == {"BTC", "PEPE"}


@pytest.mark.asyncio
async def test_resolver_uses_recent_durable_snapshot_and_last_known_good_on_failure(tmp_path):
    store = EvidenceStore(tmp_path / "volume.db")
    now = datetime.now(timezone.utc)
    snapshot = _snapshot(now)
    VolumeUniverseLedger(store).record(snapshot)

    async def must_not_run(**_kwargs):
        raise AssertionError("fresh durable snapshot should avoid a refresh")

    assets = await resolve_top_volume_assets(store, now=now, collector=must_not_run)
    assert assets == BOOTSTRAP_LIQUID_ASSETS
    assert len(assets) == TOP_VOLUME_ASSET_COUNT

    async def failed_refresh(**_kwargs):
        raise TimeoutError("provider unavailable")

    assets = await resolve_top_volume_assets(
        store,
        now=now,
        force_refresh=True,
        collector=failed_refresh,
    )
    assert assets == BOOTSTRAP_LIQUID_ASSETS


@pytest.mark.asyncio
async def test_dynamic_registry_updates_only_managed_cex_assets(monkeypatch, tmp_path):
    selected = tuple(reversed(BOOTSTRAP_LIQUID_ASSETS))

    async def fake_resolve(_store):
        return selected

    monkeypatch.setattr(
        "inefficiency_engine.adapters.dynamic_registry.resolve_top_volume_assets",
        fake_resolve,
    )
    store = EvidenceStore(tmp_path / "registry.db")
    registry = DynamicVolumePublicAdapterRegistry(evidence_store=store)
    await registry._refresh_managed_assets()

    assert registry.coinbase.assets == selected
    assert registry.bybit.assets == selected
    assert registry.kraken.assets == selected
    assert registry.okx.assets == selected


def test_opportunity_service_defaults_to_dynamic_volume_registry(tmp_path):
    store = EvidenceStore(tmp_path / "service.db")
    service = OpportunityService(evidence_store=store)
    assert isinstance(service.adapter_registry, DynamicVolumePublicAdapterRegistry)
