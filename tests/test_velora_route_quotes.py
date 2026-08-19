from datetime import datetime, timezone

import httpx
import pytest

from inefficiency_engine.adapters.velora import VeloraPriceRouteAdapter, parse_velora_price_route
from inefficiency_engine.dex_routes import detect_route_quoted_cex_dex
from inefficiency_engine.models import MarketKind, MarketQuote


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


def test_route_quote_candidate_improves_price_evidence_but_claims_no_capacity():
    route = parse_velora_price_route(
        price_payload(
            src_token="eth", dest_token="usdc",
            src_amount="250000000000000000", dest_amount="1010000000",
            src_decimals=18, dest_decimals=6,
        ),
        asset="ETH",
        direction="sell_asset",
        observed_at=NOW,
    )
    cex = MarketQuote(
        venue="Coinbase", asset="ETH", market_kind=MarketKind.SPOT, symbol="ETH-USD",
        quote_currency="USD", bid=3998, ask=4000, mid=3999,
        observed_at=NOW, source="test",
    )
    candidates = detect_route_quoted_cex_dex(
        [cex], [route], minimum_edge_bps=10.0, conversion_risk_floor_bps=2.0,
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.evidence["price_evidence"] == "amount_specific_route_quote"
    assert candidate.evidence["economic_direction"] == "buy_cex_sell_dex"
    assert candidate.capacity_usd is None
    assert candidate.evidence["quote_notional_usd_proxy"] == 1010.0
    assert candidate.evidence["capacity_claimed"] is False
    assert candidate.executable_eligible is False
    assert "settlement" in (candidate.blocked_reason or "")
    assert candidate.evidence["transaction_built"] is False


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
