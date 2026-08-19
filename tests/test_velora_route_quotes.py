from datetime import datetime, timedelta, timezone

import httpx
import pytest

from inefficiency_engine.adapters.velora import VeloraPriceRouteAdapter, parse_velora_price_route
from inefficiency_engine.dex_routes import detect_route_quoted_cex_dex
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.universal import StablecoinConversionModel, build_conversion_edges
from inefficiency_engine.universal_models import StablecoinConversionObservation


NOW = datetime(2026, 8, 19, 1, 50, tzinfo=timezone.utc)


def price_payload(*, src_token: str, dest_token: str, src_amount: str, dest_amount: str,
                  src_decimals: int, dest_decimals: int, gas_cost_usd: str = "5"):
    return {
        "priceRoute": {
            "blockNumber": 24000000,
            "network": 1,
            "srcToken": src_token,
            "destToken": dest_token,
            "srcAmount": src_amount,
            "destAmount": dest_amount,
            "srcDecimals": src_decimals,
            "destDecimals": dest_decimals,
            "gasCostUSD": gas_cost_usd,
            "bestRoute": [
                {
                    "percent": 100,
                    "swaps": [
                        {
                            "swapExchanges": [
                                {"exchange": "UniswapV3", "percent": 70},
                                {"exchange": "CurveV1", "percent": 30},
                            ]
                        }
                    ],
                }
            ],
        }
    }


def stablecoin_model(*observations: StablecoinConversionObservation) -> StablecoinConversionModel:
    return StablecoinConversionModel(build_conversion_edges(
        list(observations), depeg_multiplier=1.5, risk_floor_bps=2.0,
    ))


def stablecoin_observation(
    base: str,
    quote: str,
    *,
    bid: float,
    ask: float,
    mid: float,
    observed_at: datetime = NOW,
) -> StablecoinConversionObservation:
    return StablecoinConversionObservation(
        venue="Coinbase",
        base_currency=base,
        quote_currency=quote,
        symbol=f"{base}-{quote}",
        bid=bid,
        ask=ask,
        mid=mid,
        observed_at=observed_at,
        source="test",
    )


def sell_route() -> object:
    return parse_velora_price_route(
        price_payload(
            src_token="eth", dest_token="usdc",
            src_amount="250000000000000000", dest_amount="1010000000",
            src_decimals=18, dest_decimals=6,
        ),
        asset="ETH",
        direction="sell_asset",
        observed_at=NOW,
    )


def test_parse_velora_sell_route_preserves_amount_specific_evidence():
    quote = parse_velora_price_route(
        price_payload(
            src_token="eth", dest_token="usdc",
            src_amount="250000000000000000", dest_amount="1010000000",
            src_decimals=18, dest_decimals=6,
        ),
        asset="ETH",
        direction="sell_asset",
        request_latency_ms=25.0,
        observed_at=NOW,
    )
    assert quote.source_amount == 0.25
    assert quote.destination_amount == 1010.0
    assert quote.effective_asset_price == 4040.0
    assert quote.route_exchanges == ["CurveV1", "UniswapV3"]
    assert quote.gas_cost_usd == 5.0
    assert quote.block_number == 24000000
    assert quote.amount_specific is True
    assert quote.transaction_built is False
    assert quote.executable_eligible is False


def test_cross_currency_route_candidate_requires_fresh_observed_conversion():
    route = sell_route()
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=3998, ask=4000, mid=3999,
        observed_at=NOW, source="test",
    )

    assert detect_route_quoted_cex_dex([cex], [route], minimum_edge_bps=10.0) == []

    model = stablecoin_model(stablecoin_observation(
        "USDC", "USD", bid=0.999, ask=1.001, mid=1.0,
    ))
    candidates = detect_route_quoted_cex_dex(
        [cex], [route], conversion_model=model, minimum_edge_bps=10.0,
        conversion_max_age_seconds=120.0,
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    expected_gross = ((4040.0 * 0.999) / 4000.0 - 1.0) * 10_000.0
    assert candidate.gross_edge_bps == pytest.approx(expected_gross)
    assert candidate.evidence["price_evidence"] == "amount_specific_route_quote_with_observed_conversion"
    assert candidate.evidence["conversion_source_currency"] == "USDC"
    assert candidate.evidence["conversion_target_currency"] == "USD"
    assert candidate.evidence["conversion_rate"] == pytest.approx(0.999)
    assert candidate.evidence["conversion_market_spread_embedded_in_rate"] is True
    assert candidate.evidence["conversion_spread_bps_reference"] == pytest.approx(10.0)
    assert candidate.risk_haircut_bps == pytest.approx(2.0)
    assert candidate.capacity_usd is None
    assert candidate.evidence["capacity_claimed"] is False
    assert candidate.executable_eligible is False
    assert "settlement" in (candidate.blocked_reason or "")


def test_stale_conversion_path_fails_closed():
    route = sell_route()
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=3998, ask=4000, mid=3999,
        observed_at=NOW, source="test",
    )
    stale_model = stablecoin_model(stablecoin_observation(
        "USDC", "USD", bid=0.999, ask=1.001, mid=1.0,
        observed_at=NOW - timedelta(seconds=300),
    ))
    assert detect_route_quoted_cex_dex(
        [cex], [route], conversion_model=stale_model, minimum_edge_bps=10.0,
        conversion_max_age_seconds=120.0,
    ) == []


def test_usdt_cex_buy_dex_path_uses_two_hop_conversion_back_to_usdc():
    route = parse_velora_price_route(
        price_payload(
            src_token="usdc", dest_token="eth",
            src_amount="1000000000", dest_amount="250000000000000000",
            src_decimals=6, dest_decimals=18,
            gas_cost_usd="4",
        ),
        asset="ETH",
        direction="buy_asset",
        observed_at=NOW,
    )
    cex = MarketQuote(
        venue="OKX", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USDT",
        quote_currency="USDT", bid=4050, ask=4052, mid=4051,
        observed_at=NOW, source="test",
    )
    model = stablecoin_model(
        stablecoin_observation("USDC", "USD", bid=0.999, ask=1.001, mid=1.0),
        stablecoin_observation("USDT", "USD", bid=0.998, ask=1.000, mid=0.999),
    )
    candidates = detect_route_quoted_cex_dex(
        [cex], [route], conversion_model=model, minimum_edge_bps=10.0,
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    conversion_rate = 0.998 * (1.0 / 1.001)
    expected_gross = ((4050.0 * conversion_rate) / 4000.0 - 1.0) * 10_000.0
    assert candidate.gross_edge_bps == pytest.approx(expected_gross)
    assert candidate.evidence["conversion_source_currency"] == "USDT"
    assert candidate.evidence["conversion_target_currency"] == "USDC"
    assert candidate.evidence["conversion_rate"] == pytest.approx(conversion_rate)
    assert len(candidate.evidence["conversion_path"]) == 2
    assert candidate.risk_haircut_bps == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_velora_adapter_calls_prices_only_and_excludes_rfq():
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.url.path == "/prices"
        params = dict(request.url.params)
        assert params["version"] == "6.2"
        assert params["excludeRFQ"] == "true"
        assert params["side"] == "SELL"
        return httpx.Response(
            200,
            json=price_payload(
                src_token=params["srcToken"], dest_token=params["destToken"],
                src_amount=params["amount"], dest_amount="250000000000000000",
                src_decimals=int(params["srcDecimals"]), dest_decimals=int(params["destDecimals"]),
            ),
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VeloraPriceRouteAdapter(client=client)
        quote = await adapter.quote("ETH", "buy_asset", notional_usd=1000.0, reference_price=4000.0)

    assert len(seen) == 1
    assert quote.direction == "buy_asset"
    assert quote.source_amount == 1000.0
    assert quote.transaction_built is False
