from datetime import datetime, timezone

import pytest

from inefficiency_engine.cex_dex_evidence import build_cex_dex_composite_evidence
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, DexRouteSizePoint
from inefficiency_engine.dex_routes import DexRouteQuote
from inefficiency_engine.models import MarketKind, MarketQuote, OrderBookLevel, OrderBookSnapshot
from inefficiency_engine.universal import StablecoinConversionModel, build_conversion_edges
from inefficiency_engine.universal_models import StablecoinConversionObservation


NOW = datetime(2026, 8, 19, 3, 30, tzinfo=timezone.utc)
SETTINGS = Settings(
    max_quote_age_seconds=120,
    max_order_book_age_seconds=15,
    max_order_book_skew_seconds=5,
    coinbase_spot_taker_fee_bps=60,
    okx_spot_taker_fee_bps=10,
)


def conversion_books():
    def book(asset, bids, asks):
        return OrderBookSnapshot(
            venue="Coinbase", asset=asset, market_kind=MarketKind.SPOT,
            symbol=f"{asset}-USD", quote_currency="USD", contract_key="spot",
            bids=[OrderBookLevel(price=p, size=s) for p, s in bids],
            asks=[OrderBookLevel(price=p, size=s) for p, s in asks],
            observed_at=NOW, source="test", request_latency_ms=10,
        )
    return [
        book("USDC", [(1.0, 2000), (0.999, 5000)], [(1.001, 2000), (1.002, 5000)]),
        book("USDT", [(0.999, 2000), (0.998, 5000)], [(1.0, 2000), (1.001, 5000)]),
    ]


def conversion_model():
    rows = [
        StablecoinConversionObservation(
            venue="Coinbase", base_currency="USDC", quote_currency="USD", symbol="USDC-USD",
            bid=0.999, ask=1.001, mid=1.0, observed_at=NOW, source="test",
        ),
        StablecoinConversionObservation(
            venue="Coinbase", base_currency="USDT", quote_currency="USD", symbol="USDT-USD",
            bid=0.998, ask=1.0, mid=0.999, observed_at=NOW, source="test",
        ),
    ]
    return StablecoinConversionModel(build_conversion_edges(rows, depeg_multiplier=1.5, risk_floor_bps=2.0))


def route_quote(direction: str) -> DexRouteQuote:
    if direction == "sell_asset":
        return DexRouteQuote(
            provider="Velora", network_id=1, chain_id="ethereum", asset="ETH", quote_currency="USDC",
            direction="sell_asset", source_token="eth", destination_token="usdc",
            source_decimals=18, destination_decimals=6,
            source_amount_raw="250000000000000000", destination_amount_raw="1010000000",
            source_amount=0.25, destination_amount=1010.0, effective_asset_price=4040.0,
            block_number=24000000, route_exchanges=["UniswapV3"], gas_cost_usd=5.0,
            request_latency_ms=20.0, observed_at=NOW, source="test",
        )
    return DexRouteQuote(
        provider="Velora", network_id=1, chain_id="ethereum", asset="ETH", quote_currency="USDC",
        direction="buy_asset", source_token="usdc", destination_token="eth",
        source_decimals=6, destination_decimals=18,
        source_amount_raw="1000000000", destination_amount_raw="250000000000000000",
        source_amount=1000.0, destination_amount=0.25, effective_asset_price=4000.0,
        block_number=24000000, route_exchanges=["UniswapV3"], gas_cost_usd=4.0,
        request_latency_ms=20.0, observed_at=NOW, source="test",
    )


def frontier(direction: str) -> tuple[DexRouteSizeFrontier, DexRouteSizePoint]:
    quote = route_quote(direction)
    point = DexRouteSizePoint(
        target_notional_usd=1000.0, quoted=True, quote_notional_usd_proxy=1000.0,
        effective_asset_price=quote.effective_asset_price, price_deterioration_bps=0.0,
        gas_cost_usd=quote.gas_cost_usd, gas_cost_bps=40.0,
        request_latency_ms=20.0, block_number=24000000, route_exchanges=["UniswapV3"],
        route_changed_from_baseline=False, within_deterioration_limit=True,
        contiguous_acceptable=True, quote=quote,
    )
    surface = DexRouteSizeFrontier(
        asset="ETH", direction=direction, reference_price=4000.0,
        requested_notionals_usd=[1000.0], deterioration_limit_bps=25.0,
        points=[point], largest_successful_tier_usd=1000.0,
        largest_contiguous_acceptable_tier_usd=1000.0, observed_at=NOW,
    )
    return surface, point


def test_sell_route_uses_exact_usdc_proceeds_for_depth_conversion_before_cex_comparison():
    surface, point = frontier("sell_asset")
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=3998, ask=4000, mid=3999, observed_at=NOW, source="test",
    )
    evidence = build_cex_dex_composite_evidence(
        surface, point, cex, conversion_books(), conversion_model(), SETTINGS, now=NOW,
    )
    assert evidence is not None
    assert evidence.conversion_depth_quote is not None
    assert evidence.conversion_depth_quote.source_currency == "USDC"
    assert evidence.conversion_depth_quote.target_currency == "USD"
    assert evidence.conversion_depth_quote.input_amount == 1010.0
    # All 1010 USDC fills at the first 1.0 bid in this fixture.
    expected_gross = (1010.0 / (0.25 * 4000.0) - 1.0) * 10_000.0
    assert evidence.gross_edge_after_conversion_depth_bps == pytest.approx(expected_gross)
    assert evidence.cex_taker_fee_bps == 60.0
    assert evidence.conversion_risk_haircut_bps == 2.0
    assert evidence.capacity_claimed is False
    assert evidence.executable_eligible is False
    assert "statistical confidence" in evidence.blocked_reason


def test_buy_route_converts_actual_cex_sale_proceeds_back_to_usdc():
    surface, point = frontier("buy_asset")
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=4050, ask=4052, mid=4051, observed_at=NOW, source="test",
    )
    evidence = build_cex_dex_composite_evidence(
        surface, point, cex, conversion_books(), conversion_model(), SETTINGS, now=NOW,
    )
    assert evidence is not None
    conversion = evidence.conversion_depth_quote
    assert conversion is not None
    assert conversion.source_currency == "USD"
    assert conversion.target_currency == "USDC"
    assert conversion.input_amount == pytest.approx(0.25 * 4050.0)
    expected_output = (0.25 * 4050.0) / 1.001
    expected_gross = (expected_output / 1000.0 - 1.0) * 10_000.0
    assert evidence.gross_edge_after_conversion_depth_bps == pytest.approx(expected_gross)
    assert evidence.gas_cost_bps == pytest.approx(40.0)


def test_usdt_cex_route_uses_two_hop_depth_with_real_intermediate_amount():
    surface, point = frontier("buy_asset")
    cex = MarketQuote(
        venue="OKX", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USDT",
        quote_currency="USDT", bid=4050, ask=4052, mid=4051, observed_at=NOW, source="test",
    )
    evidence = build_cex_dex_composite_evidence(
        surface, point, cex, conversion_books(), conversion_model(), SETTINGS, now=NOW,
    )
    assert evidence is not None
    conversion = evidence.conversion_depth_quote
    assert conversion is not None
    assert len(conversion.legs) == 2
    assert conversion.legs[0].source_currency == "USDT"
    assert conversion.legs[0].target_currency == "USD"
    assert conversion.legs[1].source_currency == "USD"
    assert conversion.legs[1].target_currency == "USDC"
    assert conversion.legs[1].input_amount == pytest.approx(conversion.legs[0].output_amount)
    assert evidence.conversion_risk_haircut_bps == pytest.approx(17.0)
    assert evidence.cex_taker_fee_bps == 10.0


def test_unquoted_or_stale_route_does_not_create_composite_evidence():
    surface, point = frontier("sell_asset")
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=3998, ask=4000, mid=3999, observed_at=NOW, source="test",
    )
    missing = point.model_copy(update={"quoted": False, "quote": None})
    assert build_cex_dex_composite_evidence(
        surface, missing, cex, conversion_books(), conversion_model(), SETTINGS, now=NOW,
    ) is None

    stale_cex = cex.model_copy(update={"observed_at": datetime(2026, 8, 19, 3, 0, tzinfo=timezone.utc)})
    assert build_cex_dex_composite_evidence(
        surface, point, stale_cex, conversion_books(), conversion_model(), SETTINGS,
        now=datetime(2026, 8, 19, 3, 30, tzinfo=timezone.utc),
    ) is None
