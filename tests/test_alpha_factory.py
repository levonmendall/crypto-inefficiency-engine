from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_factory import (
    AlphaCandidate,
    AlphaEvidenceLedger,
    AlphaFactoryService,
    AlphaForwardOutcome,
    AlphaForwardSignal,
    TimeSeriesMomentumStrategy,
)
from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
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


def candidate(direction: str = "long", regime: str = "normal") -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id=f"alpha-test-{direction}-{regime}",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction=direction,
        venue="Coinbase" if direction == "long" else "HlPerp",
        market_kind=MarketKind.SPOT if direction == "long" else MarketKind.PERPETUAL,
        symbol="BTC-USD" if direction == "long" else "BTC",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=60000.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.0025,
        expected_net_return=0.0075,
        expected_profit_usd=75.0,
        notional_usd=10000.0,
        capital_required_usd=10000.0,
        confidence_score=0.8,
        regime=regime,
    )


def test_time_series_strategy_creates_research_candidate_from_point_in_time_history():
    settings = Settings(
        alpha_min_history_points=6,
        alpha_momentum_lookback_hours=24.0,
        alpha_momentum_horizon_hours=6.0,
        alpha_momentum_min_abs_return=0.01,
        alpha_forecast_shrinkage=0.25,
        alpha_research_cost_floor_bps=5.0,
    )
    history_quotes = [quote(NOW - timedelta(hours=24 - 4 * index), 50000.0 + index * 1800.0) for index in range(7)]
    current = history_quotes[-1]
    snapshot = ScanSnapshot(
        scan_id="test",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[current],
        opportunities=[],
    )
    strategy = TimeSeriesMomentumStrategy()
    rows = strategy.discover(
        snapshot,
        {(current.venue, "BTC", current.market_kind): history_quotes},
        settings,
        total_capital_usd=100000.0,
    )
    assert len(rows) == 1
    assert rows[0].direction == "long"
    assert rows[0].expected_net_return > 0
    assert rows[0].paper_allocation_eligible is False
    assert rows[0].live_execution_eligible is False


def test_alpha_evidence_ledger_is_append_only_and_tracks_signal_outcome(tmp_path):
    store = EvidenceStore(tmp_path / "alpha.sqlite3")
    ledger = AlphaEvidenceLedger(store)
    item = candidate()
    signal = AlphaForwardSignal(
        signal_id=item.candidate_id,
        candidate=item,
        due_at=NOW + timedelta(hours=6),
    )
    ledger.record_signal(signal)
    outcome = AlphaForwardOutcome(
        signal_id=signal.signal_id,
        strategy_id=item.strategy_id,
        family=item.family,
        asset=item.asset,
        direction=item.direction,
        venue=item.venue,
        market_kind=item.market_kind,
        symbol=item.symbol,
        observed_at=item.observed_at,
        due_at=signal.due_at,
        matured_at=signal.due_at,
        horizon_hours=item.horizon_hours,
        regime=item.regime,
        predicted_net_return=item.expected_net_return,
        entry_price=60000.0,
        exit_price=60600.0,
        realized_gross_return=0.01,
        realized_net_return=0.0075,
        correct_direction=True,
    )
    ledger.record_outcome(outcome)
    assert ledger.summary()["signal_count"] == 1
    assert ledger.summary()["outcome_count"] == 1
    assert ledger.outcomes(strategy_id=item.strategy_id, asset="BTC")[0].realized_net_return == pytest.approx(0.0075)


def test_forward_statistical_gate_requires_sample_confidence_and_regime_robustness(tmp_path):
    settings = Settings(
        alpha_min_forward_samples=20,
        alpha_min_forward_mean_return=0.0005,
        alpha_multiple_testing_penalty_return=0.0001,
        alpha_min_hit_rate_lower_bound=0.50,
        alpha_min_regimes=2,
        alpha_min_regime_mean_return=0.0,
    )
    store = EvidenceStore(tmp_path / "qualification.sqlite3")
    core = SimpleNamespace(settings=settings)
    factory = AlphaFactoryService(core, store)  # type: ignore[arg-type]
    item = candidate()

    for index in range(30):
        regime = "normal" if index % 2 == 0 else "high_vol"
        realized = 0.008 if index % 5 else 0.002
        factory.ledger.record_outcome(AlphaForwardOutcome(
            signal_id=f"signal-{index}",
            strategy_id=item.strategy_id,
            family=item.family,
            asset=item.asset,
            direction=item.direction,
            venue=item.venue,
            market_kind=item.market_kind,
            symbol=item.symbol,
            observed_at=NOW + timedelta(hours=6 * index),
            due_at=NOW + timedelta(hours=6 * (index + 1)),
            matured_at=NOW + timedelta(hours=6 * (index + 1)),
            horizon_hours=6.0,
            regime=regime,
            predicted_net_return=0.006,
            entry_price=60000.0,
            exit_price=60400.0,
            realized_gross_return=realized + 0.002,
            realized_net_return=realized,
            correct_direction=True,
        ))

    qualification = factory.qualification(item)
    assert qualification.sample_count == 30
    assert qualification.statistically_qualified is True
    assert qualification.mean_realized_net_return_ci_lower is not None
    assert qualification.mean_realized_net_return_ci_lower > qualification.required_mean_lower_bound
    assert qualification.regime_count == 2
    assert qualification.live_execution_authority is False
