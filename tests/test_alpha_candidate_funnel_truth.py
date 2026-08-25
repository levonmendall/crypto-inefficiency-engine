import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaEvidenceCycle
from inefficiency_engine.alpha_funnel_projection import publish_alpha_funnel_projection
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.executable_alpha_factory import ExecutableExpandedAlphaFactoryService
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.research_closure_worker import (
    ResearchClosureCycleSummary,
    ResearchClosureSummaryLedger,
)


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
    service = object.__new__(AllLaneEvidenceFactoryService)
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
        admitted_source_groups=[candidate.venue],
    )
    service._snapshot_book = lambda candidate, snapshot: None
    service._fallback_research_cost = lambda candidate: (
        0.012 if candidate.venue == "Coinbase" else 0.002
    )
    service._holding_carry_cost = lambda candidate: 0.0
    service._discovery_cost_floor_bps = lambda: 1.0
    service._last_alpha_discovery_diagnostics = {}
    return service


def test_current_economics_choose_execution_venue_after_raw_signal(monkeypatch):
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
    snapshot = ScanSnapshot(
        scan_id="candidate-funnel",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote("Coinbase", 100.0), quote("OKX", 100.1)],
        opportunities=[],
    )

    emitted = service.discover(snapshot, total_capital_usd=250_000.0)

    assert len(emitted) == 1
    assert emitted[0].venue == "OKX"
    assert emitted[0].expected_net_return == pytest.approx(0.008)
    assert emitted[0].features["venue_selected_after_current_economics"] is True
    funnel = service.last_discovery_diagnostics()["trend_momentum"]
    assert funnel["raw_candidate_count"] == 1
    assert funnel["execution_variant_count"] == 2
    assert funnel["net_hurdle_rejected_count"] == 1
    assert funnel["post_gate_candidate_count"] == 1
    assert funnel["emitted_candidate_count"] == 1
    assert funnel["dominant_rejection_gate"] == "candidate_emitted"


def test_named_24h_signal_rejects_dense_but_short_history(monkeypatch):
    history_rows = [
        quote("Coinbase", 95.0 + index, at=NOW - timedelta(hours=3.0 - 0.6 * index))
        for index in range(6)
    ]
    history = {("Coinbase", "BTC", MarketKind.SPOT): history_rows}
    service = configured_service(history)
    candidate = momentum_candidate()
    monkeypatch.setattr(
        MemoryBoundedExpandedAlphaFactoryService,
        "discover",
        lambda self, snapshot, *, total_capital_usd: [candidate],
    )
    snapshot = ScanSnapshot(
        scan_id="short-history",
        started_at=NOW,
        completed_at=NOW,
        providers=[],
        funding_quotes=[],
        market_quotes=[quote("Coinbase", 100.0), quote("OKX", 100.1)],
        opportunities=[],
    )

    emitted = service.discover(snapshot, total_capital_usd=250_000.0)

    assert emitted == []
    funnel = service.last_discovery_diagnostics()["trend_momentum"]
    assert funnel["raw_candidate_count"] == 1
    assert funnel["history_coverage_rejected_count"] == 1
    assert funnel["emitted_candidate_count"] == 0
    assert funnel["dominant_rejection_gate"] == "history_coverage"
    assert funnel["minimum_required_history_span_hours"] == pytest.approx(19.2)
    assert funnel["best_observed_history_span_hours"] < 4.0


def test_alpha_cycle_persists_dashboard_success_marker(monkeypatch):
    cycle = AlphaEvidenceCycle(
        cycle_id="alpha-cycle-123",
        observed_at=NOW,
        candidate_count=2,
        signals_recorded=2,
        outcomes_matured=1,
    )

    async def fake_alpha_cycle(self, *, total_capital_usd=None):
        return cycle

    monkeypatch.setattr(
        ExecutableExpandedAlphaFactoryService,
        "run_evidence_cycle",
        fake_alpha_cycle,
    )

    class Store:
        def __init__(self):
            self.rows = []

        def record_worker_heartbeat(self, **kwargs):
            self.rows.append(kwargs)

    class MechanismExecution:
        async def run_evidence_cycle(self, *, total_capital_usd=None):
            return SimpleNamespace(
                trials_recorded=0,
                outcomes_matured=0,
                current_specs=0,
                promoted_candidates=0,
                by_mechanism={},
            )

    service = object.__new__(AllLaneEvidenceFactoryService)
    service.store = Store()
    service.mechanism_execution = MechanismExecution()
    service._last_alpha_discovery_diagnostics = {
        "trend_momentum": {"raw_candidate_count": 3, "post_gate_candidate_count": 2}
    }

    result = asyncio.run(service.run_evidence_cycle())

    assert result.cycle_id == cycle.cycle_id
    alpha_rows = [
        row for row in service.store.rows
        if row["worker_id"] == "shadow-research-auxiliary"
        and row.get("detail", {}).get("alpha_forward_evidence_cycle_id")
    ]
    assert len(alpha_rows) == 1
    detail = alpha_rows[0]["detail"]
    assert detail["alpha_forward_evidence_cycle_id"] == cycle.cycle_id
    assert detail["raw_candidate_count"] == 3
    assert detail["post_gate_candidate_count"] == 2
    assert detail["qualification_thresholds_unchanged"] is True


def test_alpha_funnel_projection_preserves_structural_freshness_and_uses_same_cycle_microstructure(tmp_path):
    store = EvidenceStore(tmp_path / "alpha-projection.sqlite3")
    ledger = ResearchClosureSummaryLedger(store)
    baseline = ResearchClosureCycleSummary(
        observed_at=NOW,
        source_scan_id="source-scan-1",
        source_order_book_count=4,
        usable_order_book_count=4,
        rejection_funnels={
            "price_discrepancy": {
                "raw_candidate_count": 4,
                "dominant_rejection_gate": "net_return_hurdle",
            },
            "microstructure": {
                "raw_candidate_count": 2,
                "emitted_candidate_count": 5,
                "dominant_rejection_gate": "legacy-cross-cycle-value",
            },
        },
        capital_location_forward={},
        maker_shadow={"trial_count": 3},
        canonical_capabilities={"live_execution_authority": False},
        provider_admission={},
    )
    ledger.record(baseline)

    projected_at = NOW + timedelta(minutes=1)
    published = publish_alpha_funnel_projection(
        store,
        {
            "trend_momentum": {
                "raw_candidate_count": 3,
                "emitted_candidate_count": 1,
                "dominant_rejection_gate": "candidate_emitted",
            },
            "microstructure": {
                "raw_candidate_count": 7,
                "emitted_candidate_count": 2,
                "dominant_rejection_gate": "candidate_emitted",
            },
        },
        observed_at=projected_at,
    )

    assert published is True
    with store.engine.connect() as db:
        payload = db.execute(
            select(ledger.table.c.payload_json)
            .order_by(ledger.table.c.id.desc())
            .limit(1)
        ).scalar_one()
    latest = ResearchClosureCycleSummary.model_validate_json(payload)
    assert latest.summary_id != baseline.summary_id
    # Updating alpha observability must not make the structural closure checkpoint
    # look fresher than it is.
    assert latest.observed_at == baseline.observed_at
    assert latest.source_scan_id == baseline.source_scan_id
    assert latest.rejection_funnels["price_discrepancy"]["raw_candidate_count"] == 4
    assert latest.rejection_funnels["microstructure"]["raw_candidate_count"] == 7
    assert latest.rejection_funnels["microstructure"]["emitted_candidate_count"] == 2
    assert latest.rejection_funnels["microstructure"]["same_cycle_candidate_funnel"] is True
    assert (
        latest.rejection_funnels["microstructure"]["alpha_funnel_observed_at"]
        == projected_at.isoformat()
    )
    assert latest.rejection_funnels["trend_momentum"]["raw_candidate_count"] == 3
    assert (
        latest.rejection_funnels["trend_momentum"]["alpha_funnel_observed_at"]
        == projected_at.isoformat()
    )
    assert latest.maker_shadow == baseline.maker_shadow
    assert latest.canonical_capabilities == baseline.canonical_capabilities
