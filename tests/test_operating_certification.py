from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.operating_certification import (
    MechanismOperatingStatus,
    OperatingCertificationLedger,
    OperatingCertificationService,
    OperatingCertificationSnapshot,
)
from inefficiency_engine.profit_coverage import build_profit_coverage_summary


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)
ALPHA_FAMILIES = {
    "directional_time_series",
    "directional_reversal",
    "onchain_fundamental",
    "cross_sectional_relative_value",
    "microstructure_orderflow",
    "event_driven",
}


def mechanism(summary, mechanism_id: str):
    return next(row for row in summary.mechanisms if row.mechanism_id == mechanism_id)


def status(state: str = "collecting") -> MechanismOperatingStatus:
    return MechanismOperatingStatus(
        mechanism_id="trend_momentum",
        name="Directional trend / momentum",
        state=state,
        stage="profitability_certifiable",
        provider_ready=True,
        primary_reason="test",
        next_action="test",
    )


def snapshot(snapshot_id: str, state: str = "collecting") -> OperatingCertificationSnapshot:
    row = status(state)
    return OperatingCertificationSnapshot(
        snapshot_id=snapshot_id,
        observed_at=NOW,
        version="3.1.0",
        public_market_provider_healthy=True,
        public_market_surface_count=5,
        public_market_surface_ok_count=5,
        public_order_book_probe_count=2,
        public_order_book_probe_ok_count=2,
        market_quote_count=20,
        funding_quote_count=8,
        mechanism_count=1,
        provider_gap_count=1 if state == "provider_gap" else 0,
        collecting_count=1 if state == "collecting" else 0,
        poor_economics_count=1 if state == "poor_economics" else 0,
        blocked_count=0,
        certifying_count=1 if state == "certifying" else 0,
        certified_count=1 if state == "certified" else 0,
        mechanisms=[row],
    )


def test_operating_certification_ledger_is_append_only_and_latest_is_deterministic(tmp_path):
    store = EvidenceStore(tmp_path / "operating.sqlite3")
    ledger = OperatingCertificationLedger(store)
    first = snapshot("first", "provider_gap")
    second = snapshot("second", "collecting")

    ledger.record(first)
    ledger.record(first)
    ledger.record(second)

    history = ledger.history(limit=10)
    assert [row.snapshot_id for row in history] == ["second", "first"]
    assert ledger.latest().snapshot_id == "second"
    assert ledger.summary()["snapshot_count"] == 2


def test_research_state_separates_provider_gap_from_bad_economics():
    summary = build_profit_coverage_summary(
        version="3.1.0",
        alpha_families=ALPHA_FAMILIES,
        yield_authoritative_observation_count=1,
    )
    coverage = mechanism(summary, "yield")

    provider_gap = OperatingCertificationService._research_state(
        coverage,
        authoritative_count=0,
        economic_candidate_count=0,
        best_economics=None,
        next_evidence_action="collect",
    )
    poor = OperatingCertificationService._research_state(
        coverage,
        authoritative_count=2,
        economic_candidate_count=2,
        best_economics=-0.01,
        next_evidence_action="collect",
    )
    positive = OperatingCertificationService._research_state(
        coverage,
        authoritative_count=2,
        economic_candidate_count=2,
        best_economics=0.05,
        next_evidence_action="collect",
    )

    assert provider_gap[0] == "provider_gap"
    assert poor[0] == "poor_economics"
    assert positive[0] == "collecting"


def test_alpha_state_requires_forward_and_allocator_evidence_before_certification():
    service = OperatingCertificationService.__new__(OperatingCertificationService)
    service.core = SimpleNamespace(settings=Settings(
        alpha_min_forward_samples=3,
        alpha_min_forward_mean_return=0.0005,
    ))
    summary = build_profit_coverage_summary(
        version="3.1.0",
        alpha_families=ALPHA_FAMILIES,
    )
    coverage = mechanism(summary, "trend_momentum")

    collecting = service._alpha_state(
        coverage,
        "directional_time_series",
        signal_count=2,
        forward={"count": 2, "mean": 0.01, "mean_lower": 0.005},
        current_candidate_count=1,
        qualified_count=1,
        promoted_count=1,
        allocator=None,
        provider_ready=True,
    )
    poor = service._alpha_state(
        coverage,
        "directional_time_series",
        signal_count=3,
        forward={"count": 3, "mean": -0.002, "mean_lower": -0.004},
        current_candidate_count=1,
        qualified_count=1,
        promoted_count=1,
        allocator=None,
        provider_ready=True,
    )
    certifying = service._alpha_state(
        coverage,
        "directional_time_series",
        signal_count=30,
        forward={"count": 30, "mean": 0.01, "mean_lower": 0.006},
        current_candidate_count=1,
        qualified_count=1,
        promoted_count=1,
        allocator={"count": 5, "realized_profit": 50.0, "mean_lower": 0.003, "hit_lower": 0.60},
        provider_ready=True,
    )
    certified = service._alpha_state(
        coverage,
        "directional_time_series",
        signal_count=30,
        forward={"count": 30, "mean": 0.01, "mean_lower": 0.006},
        current_candidate_count=1,
        qualified_count=1,
        promoted_count=1,
        allocator={"count": 20, "realized_profit": 500.0, "mean_lower": 0.003, "hit_lower": 0.60},
        provider_ready=True,
    )

    assert collecting[0] == "collecting"
    assert poor[0] == "poor_economics"
    assert certifying[0] == "certifying"
    assert certifying[3] is False
    assert certified[0] == "certified"
    assert certified[3] is True
