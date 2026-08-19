from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.cex_dex_evidence_service import CexDexCompositeEvidenceService
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, DexRouteSizePoint
from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot
from inefficiency_engine.universal_models import StablecoinConversionObservation


NOW = datetime.now(timezone.utc)


def route_frontier() -> DexRouteSizeFrontier:
    route = DexRouteQuote(
        provider="Velora", network_id=1, chain_id="ethereum", asset="ETH", quote_currency="USDC",
        direction="buy_asset", source_token="usdc", destination_token="eth",
        source_decimals=6, destination_decimals=18,
        source_amount_raw="1000000000", destination_amount_raw="250000000000000000",
        source_amount=1000.0, destination_amount=0.25, effective_asset_price=4000.0,
        block_number=24000000, route_exchanges=["UniswapV3"], gas_cost_usd=4.0,
        request_latency_ms=20.0, observed_at=NOW, source="test",
    )
    point = DexRouteSizePoint(
        target_notional_usd=1000.0,
        quoted=True,
        quote_notional_usd_proxy=1000.0,
        effective_asset_price=4000.0,
        price_deterioration_bps=0.0,
        gas_cost_usd=4.0,
        gas_cost_bps=40.0,
        request_latency_ms=20.0,
        block_number=24000000,
        route_exchanges=["UniswapV3"],
        route_changed_from_baseline=False,
        within_deterioration_limit=True,
        contiguous_acceptable=True,
        quote=route,
    )
    return DexRouteSizeFrontier(
        asset="ETH",
        direction="buy_asset",
        reference_price=4000.0,
        requested_notionals_usd=[1000.0],
        deterioration_limit_bps=25.0,
        points=[point],
        largest_successful_tier_usd=1000.0,
        largest_contiguous_acceptable_tier_usd=1000.0,
        observed_at=NOW,
    )


def conversion_books() -> list[OrderBookSnapshot]:
    def make(asset: str, bids, asks) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT,
            symbol=f"{asset}-USD", quote_currency="USD", contract_key="spot",
            bids=[OrderBookLevel(price=p, size=s) for p, s in bids],
            asks=[OrderBookLevel(price=p, size=s) for p, s in asks],
            observed_at=NOW, source="test", request_latency_ms=10.0,
        )

    return [
        make("USDC", [(1.0, 5000)], [(1.001, 5000)]),
        # Deliberately shallow so the OKX USDT -> USD -> USDC comparison fails,
        # while USD -> USDC comparisons remain fully reconstructable.
        make("USDT", [(0.999, 10)], [(1.0, 10)]),
    ]


class FakeUniversal:
    async def probe_dex_route_size_frontiers(self):
        return [route_frontier()]


class FakeConversionDepth:
    async def collect_books(self):
        return conversion_books()


class FakePartialConversionDepth:
    async def collect_books(self):
        return conversion_books()[:1]


class FakeStablecoinAdapter:
    async def observations(self):
        return [
            StablecoinConversionObservation(
                venue="Coinbase", base_currency="USDC", quote_currency="USD", symbol="USDC-USD",
                bid=0.999, ask=1.001, mid=1.0, observed_at=NOW, source="test",
            ),
            StablecoinConversionObservation(
                venue="Coinbase", base_currency="USDT", quote_currency="USD", symbol="USDT-USD",
                bid=0.998, ask=1.0, mid=0.999, observed_at=NOW, source="test",
            ),
        ]


class FakeCore:
    def __init__(self):
        self.settings = Settings(
            coinbase_spot_taker_fee_bps=60.0,
            kraken_spot_taker_fee_bps=80.0,
            okx_spot_taker_fee_bps=10.0,
            max_quote_age_seconds=120.0,
            max_order_book_age_seconds=15.0,
            max_order_book_skew_seconds=5.0,
        )

    async def collect_live_evidence(self):
        quotes = [
            MarketQuote(
                venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
                quote_currency="USD", bid=4050, ask=4052, mid=4051, observed_at=NOW, source="test",
            ),
            MarketQuote(
                venue="Kraken", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
                quote_currency="USD", bid=4060, ask=4062, mid=4061, observed_at=NOW, source="test",
            ),
            MarketQuote(
                venue="OKX", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USDT",
                quote_currency="USDT", bid=4055, ask=4057, mid=4056, observed_at=NOW, source="test",
            ),
            # Unrelated asset must never be attempted against the ETH frontier.
            MarketQuote(
                venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, symbol="BTC-USD",
                quote_currency="USD", bid=100000, ask=100010, mid=100005, observed_at=NOW, source="test",
            ),
        ]
        return SimpleNamespace(market_quotes=quotes)


@pytest.mark.asyncio
async def test_composite_probe_isolates_depth_failure_and_sorts_complete_rows():
    service = CexDexCompositeEvidenceService(
        FakeCore(),  # type: ignore[arg-type]
        universal=FakeUniversal(),  # type: ignore[arg-type]
        conversion_depth=FakeConversionDepth(),  # type: ignore[arg-type]
        stablecoin_adapter=FakeStablecoinAdapter(),
    )

    probe = await service.probe()

    assert probe.frontier_count == 1
    assert probe.quoted_route_point_count == 1
    assert probe.comparison_attempt_count == 3
    assert probe.evidence_count == 2
    assert probe.rejection_reasons == {"InsufficientDepthError": 1}
    assert [item.cex_venue for item in probe.evidence] == ["Kraken", "Coinbase"]
    assert probe.evidence[0].net_research_edge_bps > probe.evidence[1].net_research_edge_bps
    assert all(item.evidence_complete for item in probe.evidence)
    assert all(item.capacity_claimed is False for item in probe.evidence)
    assert all(item.executable_eligible is False for item in probe.evidence)
    assert probe.capacity_claimed is False
    assert probe.executable_eligible is False


@pytest.mark.asyncio
async def test_composite_probe_keeps_usdc_routes_when_usdt_depth_is_unavailable():
    service = CexDexCompositeEvidenceService(
        FakeCore(),  # type: ignore[arg-type]
        universal=FakeUniversal(),  # type: ignore[arg-type]
        conversion_depth=FakePartialConversionDepth(),  # type: ignore[arg-type]
        stablecoin_adapter=FakeStablecoinAdapter(),
    )

    probe = await service.probe()

    assert probe.comparison_attempt_count == 3
    assert probe.evidence_count == 2
    assert probe.rejection_reasons == {"ValueError": 1}
    assert [item.cex_venue for item in probe.evidence] == ["Kraken", "Coinbase"]
    assert all(item.cex_quote_currency == "USD" for item in probe.evidence)
    assert all(item.evidence_complete for item in probe.evidence)
    assert probe.capacity_claimed is False
    assert probe.executable_eligible is False
