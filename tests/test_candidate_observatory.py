from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from inefficiency_engine.alpha_factory import AlphaCandidate
from inefficiency_engine.candidate_observatory import (
    CandidateDiagnosticShadowOutcome,
    CandidateObservation,
    CandidateObservatoryLedger,
    build_observatory_snapshot,
    settle_diagnostic_shadows,
)
from inefficiency_engine.candidate_observatory_runtime import CandidateObservedAllLaneEvidenceFactoryService
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.models import MarketKind, MarketQuote


NOW = datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc)


def quote(venue: str, price: float, *, at: datetime = NOW) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        asset="BTC",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD" if venue == "Coinbase" else "BTC-USDT",
        quote_currency="USD" if venue == "Coinbase" else "USDT",
        mid=price,
        bid=price * 0.9999,
        ask=price * 1.0001,
        observed_at=at,
        source="test",
    )


def observation(**updates) -> CandidateObservation:
    payload = {
        "source_scan_id": "scan-1",
        "observed_at": NOW,
        "lane_id": "trend_momentum",
        "candidate_id": "candidate-1",
        "signal_candidate_id": "signal-1",
        "strategy_id": "time_series_momentum_v1",
        "family": "directional_time_series",
        "asset": "BTC",
        "direction": "long",
        "stage": "net_hurdle_rejected",
        "venue": "OKX",
        "signal_reference_venue": "Coinbase",
        "market_kind": "spot",
        "symbol": "BTC-USDT",
        "horizon_hours": 0.25,
        "entry_reference_price": 100.0,
        "expected_gross_return": 0.0014,
        "estimated_cost_return": 0.0010,
        "expected_net_return": 0.0004,
        "required_net_return": 0.0005,
        "gap_to_hurdle": -0.0001,
        "notional_usd": 10_000.0,
        "expected_profit_usd": 4.0,
        "confidence_score": 0.75,
        "source_groups": ["okx", "coinbase"],
        "blockers": ["current net return is below hurdle"],
        "diagnostic_shadow_eligible": True,
    }
    payload.update(updates)
    return CandidateObservation(**payload)


def momentum_candidate() -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id="raw-momentum",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="Coinbase",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USD",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=100.0,
        expected_gross_return=0.01,
        estimated_cost_return=0.001,
        expected_net_return=0.009,
        expected_profit_usd=90.0,
        notional_usd=10_000.0,
        capital_required_usd=10_000.0,
        confidence_score=0.8,
        regime="normal",
        conflict_keys=["alpha-instrument:Coinbase:BTC-USD"],
    )


def configured_service(history):
    service = object.__new__(CandidateObservedAllLaneEvidenceFactoryService)
    service.settings = SimpleNamespace(
        alpha_min_history_points=6,
        alpha_min_current_net_return=0.0005,
    )
    service.source_plane = SimpleNamespace(snapshot=lambda now: object())
    service._history_for_snapshot = lambda snapshot: history
    service._source_gate = lambda candidate, coverage: SimpleNamespace(
        research_eligible=True,
        forward_test_eligible=True,
        allocation_source_qualified=False,
        admitted_source_groups=[candidate.venue.lower()],
        blockers=[],
    )
    service._snapshot_book = lambda candidate, snapshot: None
    service._fallback_research_cost = lambda candidate: (
        0.012 if candidate.venue == "Coinbase" else 0.002
    )
    service._holding_carry_cost = lambda candidate: 0.0
    service._discovery_cost_floor_bps = lambda: 1.0
    service._last_alpha_discovery_diagnostics = {}
    return service


def test_observatory_snapshot_persists_near_miss_and_lane_priority(tmp_path):
    store = EvidenceStore(tmp_path / "candidate-observatory.sqlite3")
    ledger = CandidateObservatoryLedger(store)
    rows = [
        observation(stage="raw_signal", estimated_cost_return=None, expected_net_return=None, gap_to_hurdle=None),
        observation(),
    ]
    rows = ledger.record_observations("cycle-1", rows)
    snapshot = build_observatory_snapshot(
        cycle_id="cycle-1",
        observed_at=NOW,
        source_scan_id="scan-1",
        diagnostics={
            "trend_momentum": {
                "raw_candidate_count": 1,
                "post_gate_candidate_count": 0,
                "emitted_candidate_count": 0,
                "best_net_economics": 0.0004,
                "dominant_rejection_gate": "net_return_hurdle",
            }
        },
        observations=rows,
        qualifications={
            ("time_series_momentum_v1", "BTC", "long"): {
                "sample_count": 12,
                "hit_rate_ci_lower": 0.55,
                "mean_realized_net_return_ci_lower": 0.0003,
                "regime_count": 2,
                "blockers": ["insufficient correlation-adjusted independent forward samples"],
            }
        },
        required_samples=30,
        research_capital_usd=100_000.0,
        shadow_signals_recorded=1,
        shadow_outcomes_matured=0,
    )
    ledger.record_snapshot(snapshot)

    latest = ledger.latest_snapshot()
    assert latest is not None
    assert latest.raw_signal_count == 1
    assert latest.near_misses[0]["forward_sample_deficit"] == 18
    assert latest.near_misses[0]["net_return"] == pytest.approx(0.0004)
    trend = next(row for row in latest.lane_priorities if row["lane_id"] == "trend_momentum")
    assert trend["priority_score"] > 0
    assert trend["allocation_authority"] is False
    assert latest.qualification_thresholds_unchanged is True
    assert latest.allocation_authority is False


def test_diagnostic_shadow_learning_is_separate_from_qualification(tmp_path):
    store = EvidenceStore(tmp_path / "candidate-shadow.sqlite3")
    ledger = CandidateObservatoryLedger(store)
    row = ledger.record_observations("cycle-1", [observation()])[0]
    assert ledger.record_shadow_signal(row) is True

    matured_at = NOW + timedelta(minutes=15)
    market_snapshot = ScanSnapshot(
        scan_id="scan-2",
        started_at=matured_at,
        completed_at=matured_at,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote("OKX", 102.0, at=matured_at)],
        opportunities=[],
    )
    assert settle_diagnostic_shadows(ledger, market_snapshot) == 1
    assert ledger.pending_shadow_signals(now=matured_at) == []

    with store.engine.connect() as db:
        raw = db.execute(
            select(ledger.shadow_events.c.payload_json)
            .where(ledger.shadow_events.c.event_type == "outcome")
            .order_by(ledger.shadow_events.c.id.desc())
            .limit(1)
        ).scalar_one()
    outcome = CandidateDiagnosticShadowOutcome.model_validate_json(raw)
    assert outcome.realized_gross_return == pytest.approx(0.02)
    assert outcome.realized_net_return == pytest.approx(0.019)
    assert outcome.qualification_authority is False
    assert outcome.allocation_authority is False


def test_discovery_observatory_preserves_rejected_and_selected_execution_variants(monkeypatch):
    history_rows = [
        quote("Coinbase", 90.0 + index, at=NOW - timedelta(hours=24 - 4 * index))
        for index in range(7)
    ]
    history = {("Coinbase", "BTC", MarketKind.SPOT): history_rows}
    service = configured_service(history)
    candidate = momentum_candidate()
    monkeypatch.setattr(
        MemoryBoundedExpandedAlphaFactoryService,
        "discover",
        lambda self, snapshot, *, total_capital_usd: [candidate],
    )
    market_snapshot = ScanSnapshot(
        scan_id="candidate-observatory",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote("Coinbase", 100.0), quote("OKX", 100.1)],
        opportunities=[],
    )

    emitted = service.discover(market_snapshot, total_capital_usd=250_000.0)

    assert len(emitted) == 1
    assert emitted[0].venue == "OKX"
    stages = {(row.stage, row.venue): row for row in service._last_candidate_observations}
    assert ("raw_signal", "Coinbase") in stages
    assert ("net_hurdle_rejected", "Coinbase") in stages
    assert ("forward_candidate_selected", "OKX") in stages
    assert stages[("net_hurdle_rejected", "Coinbase")].diagnostic_shadow_eligible is True
    assert stages[("forward_candidate_selected", "OKX")].selected_for_forward_test is True
    assert all(row.allocation_authority is False for row in service._last_candidate_observations)
