from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from inefficiency_engine.cycle_probation import (
    CycleHistoricalResearch,
    CycleReplaySummary,
    PROBATIONARY_FORWARD_MIN_SAMPLES,
    probationary_policy,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote


def _quote(
    asset: str,
    day: int,
    price: float,
    *,
    venue: str = "Coinbase",
    observed_at: datetime | None = None,
) -> MarketQuote:
    observed = observed_at or datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=day)
    quote_currency = "USD" if venue in {"Coinbase", "CoinGecko"} else "USDT"
    symbol = {
        "Coinbase": f"{asset}-USD",
        "OKX": f"{asset}-USDT",
        "Bybit": f"{asset}USDT",
        "CoinGecko": asset.lower(),
    }[venue]
    return MarketQuote(
        venue=venue,
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=symbol,
        quote_currency=quote_currency,
        contract_key="spot" if venue != "CoinGecko" else "spot-reference",
        mid=price,
        observed_at=observed,
        source=f"test-history:{venue.lower()}",
    )


def _six_hour_series(
    asset: str,
    *,
    start: datetime,
    count: int,
    venue: str,
) -> list[MarketQuote]:
    return [
        _quote(
            asset,
            0,
            100.0 + index * 0.01,
            venue=venue,
            observed_at=start + timedelta(hours=6 * index),
        )
        for index in range(count)
    ]


def test_historical_quotes_are_separate_from_live_market_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "evidence.db")
    research = CycleHistoricalResearch(store)

    inserted = research.record_quotes([_quote("BTC", 0, 100.0), _quote("BTC", 1, 101.0)])

    assert inserted == 2
    history = research.history(
        start=datetime(2024, 12, 31, tzinfo=timezone.utc),
        end=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    assert len(history[("Coinbase", "BTC", MarketKind.SPOT)]) == 2
    with store.engine.connect() as db:
        assert list(db.execute(store.market_quotes.select()).mappings()) == []


def test_parse_coinbase_candles_uses_close_price():
    payload = [[1735689600, "90", "110", "95", "105", "1234"]]
    rows = CycleHistoricalResearch._parse_candles(payload, asset="BTC")

    assert len(rows) == 1
    assert rows[0].mid == 105.0
    assert rows[0].asset == "BTC"
    assert rows[0].market_kind == MarketKind.SPOT


def test_parse_okx_candles_uses_confirmed_close_and_utc_spot_identity():
    payload = {
        "code": "0",
        "data": [
            ["1735689600000", "90", "110", "95", "105", "1234", "9999", "9999", "1"],
            ["1735711200000", "105", "111", "104", "110", "10", "100", "100", "0"],
        ],
    }
    rows = CycleHistoricalResearch._parse_okx_candles(payload, asset="TRX")

    assert len(rows) == 1
    assert rows[0].mid == 105.0
    assert rows[0].venue == "OKX"
    assert rows[0].symbol == "TRX-USDT"
    assert rows[0].quote_currency == "USDT"


def test_parse_bybit_candles_uses_close_price_and_usdt_spot_identity():
    payload = {
        "retCode": 0,
        "result": {
            "list": [["1735689600000", "90", "110", "95", "105", "1234", "9999"]]
        },
    }
    rows = CycleHistoricalResearch._parse_bybit_candles(payload, asset="PEPE")

    assert len(rows) == 1
    assert rows[0].mid == 105.0
    assert rows[0].venue == "Bybit"
    assert rows[0].symbol == "PEPEUSDT"
    assert rows[0].quote_currency == "USDT"


def test_parse_coingecko_prices_downsamples_hourly_reference_to_six_hours():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = {
        "prices": [
            [int((base + timedelta(hours=hour)).timestamp() * 1000), 100.0 + hour]
            for hour in range(12)
        ]
    }
    rows = CycleHistoricalResearch._parse_coingecko_prices(
        payload,
        asset="WBT",
        coin_id="whitebit",
    )

    assert len(rows) == 2
    assert rows[0].venue == "CoinGecko"
    assert rows[0].observed_at == base
    assert rows[0].mid == 105.0
    assert rows[1].observed_at == base + timedelta(hours=6)
    assert rows[1].mid == 111.0


@pytest.mark.asyncio
async def test_coinbase_not_listed_falls_back_to_okx_before_bybit(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "fallback.db")
    research = CycleHistoricalResearch(store, backfill_days=260)
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    start = now - timedelta(days=260)

    async def coinbase_missing(**_kwargs):
        return [], "NotListed"

    async def okx_history(**_kwargs):
        return _six_hour_series("TRX", start=start, count=1040, venue="OKX")

    async def bybit_must_not_run(**_kwargs):
        raise AssertionError("OKX complete history should stop the fallback cascade")

    monkeypatch.setattr(research, "_fetch_coinbase_history", coinbase_missing)
    monkeypatch.setattr(research, "_fetch_okx_history", okx_history)
    monkeypatch.setattr(research, "_fetch_bybit_history", bybit_must_not_run)

    report = await research.ensure_backfilled(["TRX"], now=now)

    assert report.fetched_assets == ("TRX",)
    assert not report.errors
    assert research.preferred_venue("TRX", start=start, end=now) == "OKX"
    assert "Coinbase:NotListed" in report.provider_diagnostics["TRX"]
    assert "OKX:rows=1040" in report.provider_diagnostics["TRX"]
    history = research.history(start=start, end=now)
    assert ("OKX", "TRX", MarketKind.SPOT) in history
    with store.engine.connect() as db:
        assert list(db.execute(store.market_quotes.select()).mappings()) == []


@pytest.mark.asyncio
async def test_bybit_403_falls_through_to_coingecko_reference(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "bybit-403.db")
    research = CycleHistoricalResearch(store, backfill_days=260)
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    start = now - timedelta(days=260)

    async def coinbase_missing(**_kwargs):
        return [], "NotListed"

    async def okx_empty(**_kwargs):
        return []

    async def bybit_blocked(**_kwargs):
        request = httpx.Request("GET", "https://api.bybit.com/v5/market/kline")
        response = httpx.Response(403, request=request)
        raise httpx.HTTPStatusError("blocked", request=request, response=response)

    async def coingecko_history(**_kwargs):
        return _six_hour_series("WBT", start=start, count=1040, venue="CoinGecko")

    monkeypatch.setattr(research, "_fetch_coinbase_history", coinbase_missing)
    monkeypatch.setattr(research, "_fetch_okx_history", okx_empty)
    monkeypatch.setattr(research, "_fetch_bybit_history", bybit_blocked)
    monkeypatch.setattr(research, "_fetch_coingecko_history", coingecko_history)

    report = await research.ensure_backfilled(["WBT"], now=now)

    assert report.fetched_assets == ("WBT",)
    assert report.errors == ()
    assert research.preferred_venue("WBT", start=start, end=now) == "CoinGecko"
    diagnostics = report.provider_diagnostics["WBT"]
    assert "Bybit:HTTP403" in diagnostics
    assert "CoinGecko:rows=1040" in diagnostics


@pytest.mark.asyncio
async def test_partial_coinbase_history_no_longer_suppresses_fallback(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "partial.db")
    research = CycleHistoricalResearch(store, backfill_days=260)
    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    start = now - timedelta(days=260)
    research.record_quotes(
        _six_hour_series("BOME", start=now - timedelta(days=25), count=100, venue="Coinbase")
    )

    async def coinbase_partial_failure(**_kwargs):
        return [], "HTTP500"

    async def okx_history(**_kwargs):
        return _six_hour_series("BOME", start=start, count=1040, venue="OKX")

    monkeypatch.setattr(research, "_fetch_coinbase_history", coinbase_partial_failure)
    monkeypatch.setattr(research, "_fetch_okx_history", okx_history)

    report = await research.ensure_backfilled(["BOME"], now=now)

    assert report.errors == ()
    count, earliest, latest = research._coverage("BOME", start=start, end=now)
    assert count == 1040
    assert earliest == start
    assert latest == start + timedelta(hours=6 * 1039)
    assert research.preferred_venue("BOME", start=start, end=now) == "OKX"


def test_provider_coverage_is_not_summed_across_venues(tmp_path):
    store = EvidenceStore(tmp_path / "coverage.db")
    research = CycleHistoricalResearch(store, backfill_days=260)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    research.record_quotes(_six_hour_series("ACE", start=start, count=600, venue="Coinbase"))
    research.record_quotes(_six_hour_series("ACE", start=start, count=600, venue="Bybit"))

    count, earliest, latest = research._coverage("ACE")

    assert count == 600
    assert earliest == start
    assert latest == start + timedelta(hours=6 * 599)
    assert research.preferred_venue("ACE") == "Coinbase"


def test_probationary_policy_requires_history_and_real_forward_learning():
    replay = CycleReplaySummary(
        strategy_id="cycle_aware_multi_horizon_trend_v1",
        asset="BTC",
        direction="long",
        sample_count=40,
        positive_count=25,
        hit_rate=0.625,
        mean_realized_net_return=0.01,
        regime_count=2,
        regime_means={"normal": 0.01, "high_vol": 0.008},
        qualified_for_probationary_support=True,
    )
    qualification = SimpleNamespace(
        statistically_qualified=False,
        sample_count=PROBATIONARY_FORWARD_MIN_SAMPLES,
        mean_realized_net_return=0.01,
        required_mean_lower_bound=0.001,
        hit_rate=0.625,
        regime_count=1,
    )
    health = SimpleNamespace(healthy_for_paper_allocation=True, capital_multiplier=0.5)
    settings = SimpleNamespace()

    decision = probationary_policy(qualification, health, replay, settings)
    assert decision.eligible is True

    qualification.sample_count = PROBATIONARY_FORWARD_MIN_SAMPLES - 1
    blocked = probationary_policy(qualification, health, replay, settings)
    assert blocked.eligible is False
    assert "insufficient genuine forward outcomes for probationary paper" in blocked.blockers