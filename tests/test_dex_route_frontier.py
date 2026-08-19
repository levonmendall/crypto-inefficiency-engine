from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.dex_frontier import build_size_frontier
from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote, ShadowCycle
from inefficiency_engine.universal_service import UniversalOpportunityService
from inefficiency_engine.worker import run_shadow_worker


NOW = datetime(2026, 8, 19, 2, 20, tzinfo=timezone.utc)


def route(*, direction: str, target: float, price: float) -> DexRouteQuote:
    if direction == "buy_asset":
        source_amount = target
        destination_amount = target / price
        source_decimals, destination_decimals = 6, 18
    else:
        source_amount = target / 4000.0
        destination_amount = source_amount * price
        source_decimals, destination_decimals = 18, 6
    return DexRouteQuote(
        provider="Velora", network_id=1, chain_id="ethereum", asset="ETH", quote_currency="USDC",
        direction=direction, source_token="src", destination_token="dst",
        source_decimals=source_decimals, destination_decimals=destination_decimals,
        source_amount_raw=str(max(1, int(source_amount * (10 ** source_decimals)))),
        destination_amount_raw=str(max(1, int(destination_amount * (10 ** destination_decimals)))),
        source_amount=source_amount, destination_amount=destination_amount,
        effective_asset_price=price, block_number=24000000,
        route_exchanges=["UniswapV3"], gas_cost_usd=5.0, request_latency_ms=10.0,
        observed_at=NOW, source="test", amount_specific=True,
        transaction_built=False, executable_eligible=False,
    )


def test_size_frontier_requires_contiguous_success_and_deterioration_limit():
    frontier = build_size_frontier(
        asset="ETH", direction="sell_asset", reference_price=4000.0,
        deterioration_limit_bps=25.0,
        quote_results=[
            (1000.0, route(direction="sell_asset", target=1000.0, price=4000.0), None),
            (5000.0, route(direction="sell_asset", target=5000.0, price=3996.0), None),
            (10000.0, route(direction="sell_asset", target=10000.0, price=3980.0), None),
            (25000.0, None, "TimeoutError"),
        ],
    )
    assert frontier.largest_successful_tier_usd == 10000.0
    assert frontier.largest_contiguous_acceptable_tier_usd == 5000.0
    assert frontier.capacity_claimed is False
    assert frontier.executable_eligible is False
    assert frontier.points[1].price_deterioration_bps == pytest.approx(10.0)
    assert frontier.points[2].price_deterioration_bps == pytest.approx(50.0)
    assert frontier.points[2].contiguous_acceptable is False
    assert frontier.points[3].quoted is False


def test_size_frontier_gap_cannot_be_rescued_by_larger_quote():
    frontier = build_size_frontier(
        asset="ETH", direction="sell_asset", reference_price=4000.0,
        deterioration_limit_bps=25.0,
        quote_results=[
            (1000.0, route(direction="sell_asset", target=1000.0, price=4000.0), None),
            (5000.0, None, "HTTPStatusError"),
            (10000.0, route(direction="sell_asset", target=10000.0, price=3998.0), None),
        ],
    )
    assert frontier.largest_successful_tier_usd == 10000.0
    assert frontier.largest_contiguous_acceptable_tier_usd == 1000.0
    assert frontier.points[2].within_deterioration_limit is True
    assert frontier.points[2].contiguous_acceptable is False


def test_buy_direction_marks_higher_price_as_deterioration():
    frontier = build_size_frontier(
        asset="ETH", direction="buy_asset", reference_price=4000.0,
        deterioration_limit_bps=30.0,
        quote_results=[
            (1000.0, route(direction="buy_asset", target=1000.0, price=4000.0), None),
            (5000.0, route(direction="buy_asset", target=5000.0, price=4008.0), None),
        ],
    )
    assert frontier.points[1].price_deterioration_bps == pytest.approx(20.0)
    assert frontier.largest_contiguous_acceptable_tier_usd == 5000.0


class FrontierCore:
    def __init__(self, store):
        self.settings = Settings(
            dex_route_frontier_notionals_usd=(1000.0, 5000.0, 10000.0),
            dex_route_frontier_max_deterioration_bps=25.0,
        )
        self.evidence_store = store

    async def collect_live_evidence(self):
        return SimpleNamespace(market_quotes=[
            MarketQuote(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTC-USD", quote_currency="USD", mid=100000, observed_at=NOW, source="test"),
            MarketQuote(venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD", quote_currency="USD", mid=4000, observed_at=NOW, source="test"),
        ])


class FrontierVelora:
    def __init__(self):
        self.calls = []

    async def quote(self, asset, direction, *, notional_usd, reference_price):
        self.calls.append((asset, direction, notional_usd, reference_price))
        price = reference_price
        if asset == "ETH" and direction == "sell_asset":
            price = {1000.0: 4000.0, 5000.0: 3996.0, 10000.0: 3980.0}[notional_usd]
        target_asset = "ETH" if asset == "ETH" else "BTC"
        if direction == "buy_asset":
            source_amount = notional_usd
            destination_amount = notional_usd / price
            src_decimals, dst_decimals = 6, 18
        else:
            source_amount = notional_usd / reference_price
            destination_amount = source_amount * price
            src_decimals, dst_decimals = 18, 6
        return DexRouteQuote(
            provider="Velora", network_id=1, chain_id="ethereum", asset=target_asset,
            quote_currency="USDC", direction=direction, source_token="src", destination_token="dst",
            source_decimals=src_decimals, destination_decimals=dst_decimals,
            source_amount_raw=str(max(1, int(source_amount * (10 ** src_decimals)))),
            destination_amount_raw=str(max(1, int(destination_amount * (10 ** dst_decimals)))),
            source_amount=source_amount, destination_amount=destination_amount,
            effective_asset_price=price, block_number=24000000, route_exchanges=["UniswapV3"],
            gas_cost_usd=5.0, request_latency_ms=10.0, observed_at=NOW, source="test",
            transaction_built=False, executable_eligible=False,
        )


@pytest.mark.asyncio
async def test_service_probes_and_persists_four_frontiers(tmp_path):
    store = EvidenceStore(tmp_path / "frontiers.sqlite3")
    velora = FrontierVelora()
    service = UniversalOpportunityService(FrontierCore(store), velora_adapter=velora)  # type: ignore[arg-type]
    frontiers = await service.probe_dex_route_size_frontiers()
    assert len(frontiers) == 4
    assert len(velora.calls) == 12
    eth_sell = next(item for item in frontiers if item.asset == "ETH" and item.direction == "sell_asset")
    assert eth_sell.largest_contiguous_acceptable_tier_usd == 5000.0
    assert eth_sell.capacity_claimed is False
    assert store.counts().dex_route_size_frontiers == 4
    loaded = store.load_dex_route_size_frontier(eth_sell.frontier_id)
    assert loaded.frontier_id == eth_sell.frontier_id
    summary = store.dex_route_size_frontier_summary()
    assert summary["frontier_count"] == 4
    assert summary["capacity_claimed"] is False


class WorkerCore:
    def __init__(self):
        self.settings = Settings(worker_error_backoff_seconds=0.0, shadow_cycle_interval_seconds=0.0)

    async def run_shadow_cycle(self):
        now = datetime.now(timezone.utc)
        return ShadowCycle(cycle_id="ok", started_at=now, completed_at=now, delay_seconds=0,
                           initial_scan_id="i", verification_scan_id="v", observations=[])


async def no_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_worker_runs_frontier_at_configured_period_without_core_dependency(tmp_path):
    calls = 0

    async def frontier_runner():
        nonlocal calls
        calls += 1
        return []

    store = EvidenceStore(tmp_path / "frontier-worker.sqlite3")
    stats = await run_shadow_worker(
        WorkerCore(),  # type: ignore[arg-type]
        store,
        worker_id="frontier-worker",
        sleep=no_sleep,
        max_cycles=3,
        frontier_runner=frontier_runner,
        frontier_every_cycles=2,
    )
    assert stats.cycles_succeeded == 3
    assert stats.cycles_failed == 0
    assert calls == 1
