from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.cycle_trend_strategy import CycleAwareMultiHorizonTrendStrategy
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.memory_bounded_alpha_factory import (
    MemoryBoundedExpandedAlphaFactoryService,
)
from inefficiency_engine.models import FundingQuote, MarketKind, MarketQuote


NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)


def quote(at: datetime, price: float, *, venue: str, kind: MarketKind) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=kind,
        symbol="BTC-USD" if kind == MarketKind.SPOT else "BTC",
        mid=price,
        bid=price * 0.9999,
        ask=price * 1.0001,
        observed_at=at,
        source="test",
    )


def trend_history(*, venue: str, kind: MarketKind, daily_multiplier: float, now: datetime = NOW):
    start = now - timedelta(days=185)
    rows = []
    price = 40000.0
    for day in range(186):
        rows.append(quote(start + timedelta(days=day), price, venue=venue, kind=kind))
        price *= daily_multiplier
    return rows


def snapshot(current: MarketQuote, *, funding=None) -> ScanSnapshot:
    return ScanSnapshot(
        scan_id="cycle-trend-test",
        started_at=current.observed_at,
        completed_at=current.observed_at,
        providers=[],
        funding_quotes=funding or [],
        market_quotes=[current],
        opportunities=[],
    )


def test_multi_horizon_confirmation_creates_long_candidate():
    settings = Settings(alpha_research_cost_floor_bps=5.0)
    history = trend_history(venue="OKX", kind=MarketKind.SPOT, daily_multiplier=1.004)
    current = history[-1]
    rows = CycleAwareMultiHorizonTrendStrategy().discover(
        snapshot(current),
        {("OKX", "BTC", MarketKind.SPOT): history},
        settings,
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    candidate = rows[0]
    assert candidate.direction == "long"
    assert candidate.features["trend_horizons_available"] >= 3
    assert candidate.features["halving_cycle_is_prior_not_trigger"] is True
    assert candidate.features["cycle_prior_weight"] <= 0.10
    assert candidate.paper_allocation_eligible is False
    assert candidate.live_execution_eligible is False


def test_halving_prior_cannot_reverse_confirmed_downtrend():
    observed_at = datetime(2024, 8, 1, tzinfo=timezone.utc)
    history = trend_history(
        venue="HlPerp",
        kind=MarketKind.PERPETUAL,
        daily_multiplier=0.996,
        now=observed_at,
    )
    current = history[-1]
    funding = [
        FundingQuote(
            venue="HlPerp",
            asset="BTC",
            rate=0.0001,
            interval_hours=8.0,
            symbol="BTC",
            observed_at=observed_at,
            source="test",
        )
    ]
    rows = CycleAwareMultiHorizonTrendStrategy().discover(
        snapshot(current, funding=funding),
        {("HlPerp", "BTC", MarketKind.PERPETUAL): history},
        Settings(alpha_research_cost_floor_bps=5.0),
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    assert rows[0].direction == "short"
    assert rows[0].features["cycle_prior_score"] > 0


def test_adverse_short_funding_is_charged_over_holding_period():
    history = trend_history(
        venue="HlPerp", kind=MarketKind.PERPETUAL, daily_multiplier=0.995
    )
    current = history[-1]
    funding = [
        FundingQuote(
            venue="HlPerp",
            asset="BTC",
            rate=-0.0002,
            interval_hours=8.0,
            symbol="BTC",
            observed_at=NOW,
            source="test",
        )
    ]
    rows = CycleAwareMultiHorizonTrendStrategy().discover(
        snapshot(current, funding=funding),
        {("HlPerp", "BTC", MarketKind.PERPETUAL): history},
        Settings(alpha_research_cost_floor_bps=5.0),
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    carry = rows[0].features["holding_carry_cost_return"]
    assert carry == pytest.approx(0.0018)
    assert rows[0].estimated_cost_return >= carry


def test_production_memory_bounded_factory_registers_cycle_strategy(tmp_path):
    store = EvidenceStore(tmp_path / "cycle-trend.sqlite3")
    core = SimpleNamespace(settings=Settings())
    factory = MemoryBoundedExpandedAlphaFactoryService(core, store)  # type: ignore[arg-type]
    assert "cycle_aware_multi_horizon_trend_v1" in {
        item.strategy_id for item in factory.manifests()
    }
