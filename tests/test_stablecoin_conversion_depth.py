from datetime import datetime, timedelta, timezone

import httpx
import pytest

from inefficiency_engine.adapters.stablecoin_depth import CoinbaseStablecoinDepthAdapter
from inefficiency_engine.conversion_depth import (
    InsufficientDepthError,
    quote_stablecoin_conversion_depth,
)
from inefficiency_engine.models import MarketKind, OrderBookLevel, OrderBookSnapshot


NOW = datetime(2026, 8, 19, 3, 0, tzinfo=timezone.utc)


def book(
    asset: str,
    *,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    observed_at: datetime = NOW,
    latency_ms: float = 12.0,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue="Coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot",
        bids=[OrderBookLevel(price=price, size=size) for price, size in bids],
        asks=[OrderBookLevel(price=price, size=size) for price, size in asks],
        observed_at=observed_at,
        source="coinbase-exchange:book-level2",
        request_latency_ms=latency_ms,
    )


def books() -> list[OrderBookSnapshot]:
    return [
        book("USDC", bids=[(1.0, 600), (0.999, 600)], asks=[(1.001, 500), (1.002, 700)]),
        book("USDT", bids=[(0.999, 600), (0.998, 600)], asks=[(1.0, 500), (1.001, 700)]),
    ]


def test_usdc_to_usd_walks_visible_bids_by_exact_base_amount():
    quote = quote_stablecoin_conversion_depth(
        "USDC", "USD", 1000.0, books(), now=NOW,
    )
    assert quote.output_amount == pytest.approx(999.6)
    assert quote.effective_rate == pytest.approx(0.9996)
    assert quote.total_slippage_bps == pytest.approx(4.0)
    assert len(quote.legs) == 1
    assert quote.legs[0].levels_consumed == 2
    assert quote.legs[0].request_latency_ms == 12.0
    assert quote.visible_depth_only is True
    assert quote.capacity_claimed is False
    assert quote.executable_eligible is False


def test_usd_to_usdc_walks_asks_by_exact_usd_input():
    quote = quote_stablecoin_conversion_depth(
        "USD", "USDC", 1000.0, books(), now=NOW,
    )
    expected_base = 500.0 + (499.5 / 1.002)
    assert quote.output_amount == pytest.approx(expected_base)
    assert quote.legs[0].input_amount == 1000.0
    assert quote.legs[0].levels_consumed == 2
    assert quote.effective_rate == pytest.approx(expected_base / 1000.0)
    assert quote.total_slippage_bps > 0


def test_usdt_to_usdc_two_hop_uses_actual_intermediate_usd_amount():
    quote = quote_stablecoin_conversion_depth(
        "USDT", "USDC", 1000.0, books(), now=NOW,
    )
    assert len(quote.legs) == 2
    first, second = quote.legs
    expected_usd = 600 * 0.999 + 400 * 0.998
    assert first.output_amount == pytest.approx(expected_usd)
    assert second.input_amount == pytest.approx(expected_usd)
    assert quote.output_amount == pytest.approx(second.output_amount)
    assert first.source_currency == "USDT"
    assert first.target_currency == "USD"
    assert second.source_currency == "USD"
    assert second.target_currency == "USDC"
    assert quote.book_skew_seconds == 0.0
    assert quote.capacity_claimed is False


def test_conversion_fails_closed_when_visible_depth_cannot_fill_full_input():
    shallow = [
        book("USDC", bids=[(1.0, 10)], asks=[(1.001, 10)]),
        book("USDT", bids=[(0.999, 10)], asks=[(1.0, 10)]),
    ]
    with pytest.raises(InsufficientDepthError):
        quote_stablecoin_conversion_depth("USDC", "USD", 1000.0, shallow, now=NOW)
    with pytest.raises(InsufficientDepthError):
        quote_stablecoin_conversion_depth("USD", "USDT", 1000.0, shallow, now=NOW)


def test_conversion_fails_closed_on_stale_or_skewed_books():
    stale = books()
    stale[0] = stale[0].model_copy(update={"observed_at": NOW - timedelta(seconds=30)})
    with pytest.raises(ValueError, match="stale"):
        quote_stablecoin_conversion_depth(
            "USDC", "USD", 100.0, stale, now=NOW, max_book_age_seconds=15.0,
        )

    skewed = books()
    skewed[1] = skewed[1].model_copy(update={"observed_at": NOW - timedelta(seconds=10)})
    with pytest.raises(ValueError, match="skew"):
        quote_stablecoin_conversion_depth(
            "USDT", "USDC", 100.0, skewed, now=NOW,
            max_book_age_seconds=15.0, max_book_skew_seconds=5.0,
        )


@pytest.mark.asyncio
async def test_coinbase_stablecoin_depth_adapter_requests_public_level2_books_only():
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.method == "GET"
        assert request.url.path in {
            "/products/USDC-USD/book",
            "/products/USDT-USD/book",
        }
        assert request.url.params["level"] == "2"
        payload = {
            "bids": [["0.999", "1000", "1"]],
            "asks": [["1.001", "1000", "1"]],
        }
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = CoinbaseStablecoinDepthAdapter(client=client)
        result = await adapter.books()

    assert len(seen) == 2
    assert {item.symbol for item in result} == {"USDC-USD", "USDT-USD"}
    assert all(item.request_latency_ms is not None for item in result)


@pytest.mark.asyncio
async def test_coinbase_stablecoin_depth_adapter_preserves_available_book_when_peer_fails():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/USDT-USD/book":
            return httpx.Response(503, json={"message": "temporarily unavailable"}, request=request)
        return httpx.Response(
            200,
            json={
                "bids": [["0.999", "1000", "1"]],
                "asks": [["1.001", "1000", "1"]],
            },
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = CoinbaseStablecoinDepthAdapter(client=client)
        result = await adapter.books()

    assert [item.symbol for item in result] == ["USDC-USD"]
    observed_at = result[0].observed_at
    usdc_quote = quote_stablecoin_conversion_depth(
        "USDC", "USD", 100.0, result, now=observed_at,
    )
    assert usdc_quote.source_currency == "USDC"
    assert usdc_quote.target_currency == "USD"
    with pytest.raises(ValueError, match="USDT-USD"):
        quote_stablecoin_conversion_depth("USDT", "USD", 100.0, result, now=observed_at)


@pytest.mark.asyncio
async def test_coinbase_stablecoin_depth_adapter_does_not_hide_unexpected_runtime_bug():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/USDT-USD/book":
            raise RuntimeError("unexpected adapter bug")
        return httpx.Response(
            200,
            json={
                "bids": [["0.999", "1000", "1"]],
                "asks": [["1.001", "1000", "1"]],
            },
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = CoinbaseStablecoinDepthAdapter(client=client)
        with pytest.raises(RuntimeError, match="unexpected adapter bug"):
            await adapter.books()
