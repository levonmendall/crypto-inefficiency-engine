from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from inefficiency_engine.adapters.dynamic_registry import DynamicVolumePublicAdapterRegistry
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.service import OpportunityService
from inefficiency_engine.volume_universe import (
    STRICT_VOLUME_METHOD,
    STRICT_VOLUME_SOURCE,
    TOP_VOLUME_ASSET_COUNT,
    VOLUME_UNIVERSE_REFRESH_SECONDS,
    VolumeUniverseLedger,
    VolumeUniverseUnavailableError,
    parse_bybit_turnover,
    parse_coingecko_markets,
    parse_hyperliquid_turnover,
    rank_marketwide_volume,
    read_latest_volume_universe,
    resolve_top_volume_assets,
    validated_volume_assets,
)


TEST_ASSETS = tuple(f"TKN{index}" for index in range(TOP_VOLUME_ASSET_COUNT))


def _snapshot(now: datetime, assets: tuple[str, ...] = TEST_ASSETS) -> dict[str, object]:
    return {
        "observed_at": now.isoformat(),
        "method": STRICT_VOLUME_METHOD,
        "ranking_metric": "reported_24h_trading_volume_usd",
        "ranking_source": STRICT_VOLUME_SOURCE,
        "ranking_scope": "marketwide",
        "volume_is_defining_metric": True,
        "asset_count": TOP_VOLUME_ASSET_COUNT,
        "stable_value_assets_excluded": True,
        "eligibility_note": "test",
        "source_health": {},
        "assets": [
            {
                "rank": index,
                "asset": asset,
                "reported_24h_volume_usd": float(1_000_000_000 - index * 1_000_000),
                "aggregate_24h_notional_usd": float(1_000_000_000 - index * 1_000_000),
                "sources": [STRICT_VOLUME_SOURCE],
                "source_asset_id": asset.lower(),
            }
            for index, asset in enumerate(assets, start=1)
        ],
        "paper_only": True,
        "allocation_authority": False,
    }


def test_marketwide_volume_is_the_defining_rank_and_low_volume_algo_is_excluded():
    observations = [
        {
            "asset": asset,
            "notional_usd": float(1_000_000_000 - index * 1_000_000),
            "source_asset_id": asset.lower(),
        }
        for index, asset in enumerate(TEST_ASSETS)
    ]
    observations.extend(
        [
            {"asset": "ALGO", "notional_usd": 19_000_000.0, "source_asset_id": "algorand"},
            # Stable-value assets are intentionally ineligible for this directional/CEX lane.
            {"asset": "USDT", "notional_usd": 90_000_000_000.0, "source_asset_id": "tether"},
        ]
    )

    ranked = rank_marketwide_volume(observations)

    assert len(ranked) == TOP_VOLUME_ASSET_COUNT
    assert "ALGO" not in {row["asset"] for row in ranked}
    assert "USDT" not in {row["asset"] for row in ranked}
    volumes = [row["reported_24h_volume_usd"] for row in ranked]
    assert volumes == sorted(volumes, reverse=True)


def test_coingecko_parser_uses_total_volume_and_deduplicates_symbol_by_highest_volume():
    parsed = parse_coingecko_markets(
        [
            {"id": "bitcoin", "symbol": "btc", "total_volume": 1000},
            {"id": "bitcoin-clone", "symbol": "btc", "total_volume": 100},
            {"id": "ethereum", "symbol": "eth", "total_volume": 900},
            {"id": "tether", "symbol": "usdt", "total_volume": 999999},
        ]
    )
    ranked = rank_marketwide_volume(parsed, limit=2)

    assert [row["asset"] for row in ranked] == ["BTC", "ETH"]
    assert ranked[0]["reported_24h_volume_usd"] == 1000.0
    assert ranked[0]["source_asset_id"] == "bitcoin"


def test_bybit_and_hyperliquid_volume_parsers_remain_available_for_diagnostics():
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


def test_legacy_or_static_snapshot_cannot_be_validated_as_current_top40():
    legacy = {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "method": "aggregate_24h_traded_notional",
        "asset_count": 40,
        "assets": [{"rank": i, "asset": asset} for i, asset in enumerate(TEST_ASSETS, start=1)],
    }
    assert validated_volume_assets(legacy) == ()


@pytest.mark.asyncio
async def test_resolver_uses_recent_valid_snapshot_and_last_known_good_on_failure(tmp_path):
    store = EvidenceStore(tmp_path / "volume.db")
    now = datetime.now(timezone.utc)
    snapshot = _snapshot(now)
    VolumeUniverseLedger(store).record(snapshot)

    async def must_not_run(**_kwargs):
        raise AssertionError("fresh validated snapshot should avoid a refresh")

    assets = await resolve_top_volume_assets(store, now=now, collector=must_not_run)
    assert assets == TEST_ASSETS

    async def failed_refresh(**_kwargs):
        raise TimeoutError("provider unavailable")

    assets = await resolve_top_volume_assets(
        store,
        now=now,
        force_refresh=True,
        collector=failed_refresh,
    )
    assert assets == TEST_ASSETS


@pytest.mark.asyncio
async def test_resolver_refreshes_membership_after_bounded_cache_interval(tmp_path):
    store = EvidenceStore(tmp_path / "membership-refresh.db")
    now = datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc)
    VolumeUniverseLedger(store).record(_snapshot(now))
    changed = (*TEST_ASSETS[:-1], "NEWCOIN")
    later = now + timedelta(seconds=VOLUME_UNIVERSE_REFRESH_SECONDS + 1)
    calls = 0

    async def changed_snapshot(**_kwargs):
        nonlocal calls
        calls += 1
        return _snapshot(later, changed)

    assets = await resolve_top_volume_assets(store, now=later, collector=changed_snapshot)

    assert calls == 1
    assert assets == changed
    assert validated_volume_assets(read_latest_volume_universe(store)) == changed


@pytest.mark.asyncio
async def test_resolver_fails_closed_instead_of_returning_static_40(tmp_path):
    store = EvidenceStore(tmp_path / "empty-volume.db")

    async def failed_refresh(**_kwargs):
        raise TimeoutError("market-wide volume source unavailable")

    with pytest.raises(VolumeUniverseUnavailableError):
        await resolve_top_volume_assets(store, force_refresh=True, collector=failed_refresh)


@pytest.mark.asyncio
async def test_dynamic_registry_updates_only_managed_cex_assets(monkeypatch, tmp_path):
    selected = tuple(reversed(TEST_ASSETS))

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
