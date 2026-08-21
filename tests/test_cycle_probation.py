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


def _quote(asset: str, day: int, price: float) -> MarketQuote:
    observed = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=day)
    return MarketQuote(
        venue="Coinbase",
        asset=asset,
        market_kind=MarketKind.SPOT,
        symbol=f"{asset}-USD",
        quote_currency="USD",
        contract_key="spot",
        mid=price,
        observed_at=observed,
        source="test-history",
    )


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


@pytest.mark.asyncio
async def test_coinbase_not_listed_falls_back_to_bybit_without_touching_live_evidence(
    tmp_path, monkeypatch
):
    store = EvidenceStore(tmp_path / "fallback.db")
    research = CycleHistoricalResearch(store, backfill_days=260)

    async def coinbase_missing(*_args, **_kwargs):
        request = httpx.Request("GET", "https://api.exchange.coinbase.com/products/PEPE-USD/candles")
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("not listed", request=request, response=response)

    async def bybit_history(*, params):
        observed = int(int(params["start"]) + 6 * 60 * 60 * 1000)
        return {
            "retCode": 0,
            "result": {
                "list": [[str(observed), "1", "1.1", "0.9", "1.05", "100", "105"]]
            },
        }

    monkeypatch.setattr(research, "_get", coinbase_missing)
    monkeypatch.setattr(research, "_get_bybit", bybit_history)

    now = datetime(2026, 8, 20, tzinfo=timezone.utc)
    report = await research.ensure_backfilled(["PEPE"], now=now)

    assert report.fetched_assets == ("PEPE",)
    assert not report.errors
    history = research.history(start=now - timedelta(days=261), end=now)
    assert ("Bybit", "PEPE", MarketKind.SPOT) in history
    with store.engine.connect() as db:
        assert list(db.execute(store.market_quotes.select()).mappings()) == []


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
