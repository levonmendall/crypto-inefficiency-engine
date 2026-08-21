from __future__ import annotations

from datetime import datetime, timezone

import pytest

from inefficiency_engine.volume_universe import (
    STRICT_VOLUME_METHOD,
    TOP_VOLUME_ASSET_COUNT,
    collect_top_volume_snapshot,
    parse_coingecko_stablecoin_ids,
    validated_volume_assets,
)


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Client:
    def __init__(self, market_payload, stable_payload):
        self.market_payload = market_payload
        self.stable_payload = stable_payload
        self.calls = []

    async def get(self, _url, *, params):
        self.calls.append(dict(params))
        if params.get("category") == "stablecoins":
            return _Response(self.stable_payload)
        return _Response(self.market_payload)


def _market_row(index: int):
    return {
        "id": f"risk-{index}",
        "symbol": f"r{index}",
        "total_volume": float(2_000_000_000 - index * 10_000_000),
    }


def test_stablecoin_category_parser_is_id_based_not_static_ticker_based():
    ids = parse_coingecko_stablecoin_ids(
        [
            {"id": "usdt0", "symbol": "usdt0"},
            {"id": "fidelity-digital-dollar", "symbol": "fidd"},
        ]
    )
    assert ids == frozenset({"usdt0", "fidelity-digital-dollar"})


@pytest.mark.asyncio
async def test_top40_excludes_new_dynamic_stablecoins_even_when_they_have_higher_volume():
    risk = [_market_row(index) for index in range(45)]
    market_payload = [
        {"id": "usdt0", "symbol": "usdt0", "total_volume": 20_000_000_000.0},
        {"id": "fidelity-digital-dollar", "symbol": "fidd", "total_volume": 15_000_000_000.0},
        *risk,
    ]
    stable_payload = [
        {"id": "usdt0", "symbol": "usdt0", "total_volume": 20_000_000_000.0},
        {"id": "fidelity-digital-dollar", "symbol": "fidd", "total_volume": 15_000_000_000.0},
    ]
    client = _Client(market_payload, stable_payload)

    snapshot = await collect_top_volume_snapshot(
        now=datetime(2026, 8, 21, tzinfo=timezone.utc),
        client=client,
    )

    assets = validated_volume_assets(snapshot)
    assert len(assets) == TOP_VOLUME_ASSET_COUNT
    assert "USDT0" not in assets
    assert "FIDD" not in assets
    assert assets == tuple(f"R{index}" for index in range(TOP_VOLUME_ASSET_COUNT))
    assert snapshot["dynamic_stable_value_classification"] is True
    assert snapshot["stable_value_observations_excluded"] == 2
    assert any(call.get("category") == "stablecoins" for call in client.calls)
    assert any("category" not in call for call in client.calls)


def test_pre_dynamic_snapshot_method_is_rejected():
    payload = {
        "observed_at": datetime(2026, 8, 21, tzinfo=timezone.utc).isoformat(),
        "method": "marketwide_24h_trading_volume_usd",
        "ranking_metric": "reported_24h_trading_volume_usd",
        "ranking_source": "coingecko:coins_markets:total_volume",
        "assets": [
            {
                "rank": index + 1,
                "asset": f"R{index}",
                "reported_24h_volume_usd": float(1_000_000 - index),
            }
            for index in range(TOP_VOLUME_ASSET_COUNT)
        ],
    }
    assert STRICT_VOLUME_METHOD != payload["method"]
    assert validated_volume_assets(payload) == ()
