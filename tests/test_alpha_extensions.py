from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_extensions import (
    FundamentalFactorLedger,
    FundamentalFactorObservation,
    MeanReversionStrategy,
    OnChainFundamentalStrategy,
)
from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaForwardOutcome
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.expanded_alpha_factory import ExpandedAlphaFactoryService, _ExpandedSettingsView
from inefficiency_engine.models import MarketKind, MarketQuote


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def quote(at: datetime, price: float, *, kind: MarketKind = MarketKind.SPOT) -> MarketQuote:
    return MarketQuote(
        venue="Coinbase" if kind == MarketKind.SPOT else "HlPerp",
        asset="BTC",
        market_kind=kind,
        symbol="BTC-USD" if kind == MarketKind.SPOT else "BTC",
        mid=price,
        bid=price * 0.9999,
        ask=price * 1.0001,
        observed_at=at,
        source="test",
    )


def snapshot(current: list[MarketQuote]) -> ScanSnapshot:
    return ScanSnapshot(
        scan_id="test",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=current,
        opportunities=[],
    )


def alpha_candidate() -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id="reversal-btc",
        strategy_id="mean_reversion_v1",
        family="directional_reversal",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=60000.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.002,
        expected_net_return=0.008,
        expected_profit_usd=80.0,
        notional_usd=10000.0,
        capital_required_usd=10000.0,
        confidence_score=0.8,
        regime="normal",
    )


def test_mean_reversion_discovers_large_robust_displacement_without_allocation_authority():
    base = Settings(alpha_min_history_points=8, alpha_research_cost_floor_bps=5.0)
    settings = _ExpandedSettingsView(base)
    history = [
        quote(NOW - timedelta(hours=24 - index * 3), 60000.0 + ((index % 3) - 1) * 120.0)
        for index in range(8)
    ]
    current = quote(NOW, 54000.0)
    history.append(current)
    rows = MeanReversionStrategy().discover(
        snapshot([current]),
        {(current.venue, "BTC", current.market_kind): history},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    assert rows[0].strategy_id == "mean_reversion_v1"
    assert rows[0].direction == "long"
    assert rows[0].features["robust_z"] < 0
    assert rows[0].expected_net_return > 0
    assert rows[0].paper_allocation_eligible is False
    assert rows[0].live_execution_eligible is False


def test_fundamental_factor_family_fails_closed_until_source_is_authoritative_and_commercial(tmp_path):
    store = EvidenceStore(tmp_path / "factor.sqlite3")
    ledger = FundamentalFactorLedger(store)
    current = quote(NOW, 60000.0)
    settings = _ExpandedSettingsView(Settings(alpha_research_cost_floor_bps=5.0))
    strategy = OnChainFundamentalStrategy(ledger)

    ledger.record(FundamentalFactorObservation(
        provider="research-feed",
        asset="BTC",
        observed_at=NOW,
        as_of_at=NOW - timedelta(hours=1),
        factor_scores={"network_growth": 0.8, "economic_activity": 0.7},
        authoritative=False,
        commercial_use_permitted=True,
    ))
    assert strategy.discover(
        snapshot([current]),
        {(current.venue, "BTC", current.market_kind): [current]},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    ) == []

    ledger.record(FundamentalFactorObservation(
        provider="licensed-authoritative-feed",
        asset="BTC",
        observed_at=NOW,
        as_of_at=NOW - timedelta(minutes=30),
        factor_scores={"network_growth": 0.9, "economic_activity": 0.8},
        authoritative=True,
        commercial_use_permitted=True,
    ))
    rows = strategy.discover(
        snapshot([current]),
        {(current.venue, "BTC", current.market_kind): [current]},
        settings,  # type: ignore[arg-type]
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    assert rows[0].family == "onchain_fundamental"
    assert rows[0].direction == "long"
    assert rows[0].paper_allocation_eligible is False


def test_expanded_factory_counts_only_non_overlapping_forward_outcomes(tmp_path):
    settings = Settings(
        alpha_min_forward_samples=3,
        alpha_min_forward_mean_return=0.0001,
        alpha_multiple_testing_penalty_return=0.0,
        alpha_min_hit_rate_lower_bound=0.0,
        alpha_min_regimes=1,
        alpha_min_regime_mean_return=-1.0,
    )
    store = EvidenceStore(tmp_path / "independent.sqlite3")
    core = SimpleNamespace(settings=settings)
    factory = ExpandedAlphaFactoryService(core, store)  # type: ignore[arg-type]
    item = alpha_candidate()

    observations = [
        (NOW, NOW + timedelta(hours=6)),
        (NOW + timedelta(hours=1), NOW + timedelta(hours=7)),
        (NOW + timedelta(hours=6), NOW + timedelta(hours=12)),
        (NOW + timedelta(hours=7), NOW + timedelta(hours=13)),
    ]
    for index, (observed_at, due_at) in enumerate(observations):
        factory.ledger.record_outcome(AlphaForwardOutcome(
            signal_id=f"signal-{index}",
            strategy_id=item.strategy_id,
            family=item.family,
            asset=item.asset,
            direction=item.direction,
            venue=item.venue,
            market_kind=item.market_kind,
            symbol=item.symbol,
            observed_at=observed_at,
            due_at=due_at,
            matured_at=due_at,
            horizon_hours=6.0,
            regime="normal",
            predicted_net_return=0.005,
            entry_price=60000.0,
            exit_price=60600.0,
            realized_gross_return=0.01,
            realized_net_return=0.008,
            correct_direction=True,
        ))

    qualification = factory.qualification(item)
    assert qualification.sample_count == 2
    assert qualification.statistically_qualified is False
    assert "insufficient independent forward samples" in qualification.blockers


def test_expanded_factory_registers_six_alpha_families(tmp_path):
    store = EvidenceStore(tmp_path / "registry.sqlite3")
    core = SimpleNamespace(settings=Settings())
    factory = ExpandedAlphaFactoryService(core, store)  # type: ignore[arg-type]
    ids = {item.strategy_id for item in factory.manifests()}
    assert ids == {
        "time_series_momentum_v1",
        "mean_reversion_v1",
        "onchain_fundamental_composite_v1",
        "cross_sectional_relative_value_v1",
        "microstructure_imbalance_v1",
        "event_driven_surprise_v1",
    }
