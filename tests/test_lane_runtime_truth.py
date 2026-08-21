from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import inefficiency_engine.evidence_velocity_runtime as runtime_module
from inefficiency_engine.evidence_velocity_runtime import (
    EvidenceVelocityAllLaneOperatingCertificationService,
)
from inefficiency_engine.operating_certification import (
    MechanismOperatingStatus,
    OperatingCertificationSnapshot,
)


def _status(
    lane_id: str = "mean_reversion",
    *,
    state: str = "statistical_failure",
) -> MechanismOperatingStatus:
    return MechanismOperatingStatus(
        mechanism_id=lane_id,
        name=lane_id,
        state=state,
        stage="profitability_certifiable",
        provider_ready=True,
        authoritative_observation_count=1,
        forward_signal_count=40,
        independent_forward_outcome_count=40,
        current_candidate_count=0,
        current_statistically_qualified_count=0,
        current_promoted_count=0,
        settled_allocator_outcome_count=0,
        primary_reason="legacy family-pooled status",
        next_action="legacy",
        blockers=[],
    )


def _lane(
    lane_id: str = "mean_reversion",
    *,
    research: bool = True,
    forward: bool = True,
    allocation: bool = True,
    missing: list[str] | None = None,
    source_count: int = 2,
):
    missing = list(missing or [])
    groups = ["one", "two"] if allocation else ["one"]
    sources = [
        {
            "source_id": f"source-{index}",
            "state": "healthy",
            "healthy": True,
            "fresh": True,
            "admitted": True,
            "authoritative": True,
            "group": group,
            "item_count": 10 + index,
        }
        for index, group in enumerate(groups)
    ]
    return SimpleNamespace(
        lane_id=lane_id,
        source_layer_sufficient=allocation,
        healthy_source_count=source_count,
        source_redundancy_satisfied=allocation,
        missing_evidence_classes=missing,
        downstream_evidence_gaps=[],
        research_eligible=research,
        forward_test_eligible=forward,
        allocation_source_qualified=allocation,
        sources=sources,
    )


def _strategy(
    strategy_id: str,
    state: str,
    *,
    outcomes: int,
    local: int,
    mean: float | None = None,
    mean_lower: float | None = None,
    hit: float | None = None,
    hit_lower: float | None = None,
):
    return {
        "strategy_id": strategy_id,
        "state": state,
        "forward_signal_count": outcomes + 2,
        "independent_forward_outcome_count": outcomes,
        "candidate_local_forward_outcome_count": local,
        "mean_forward_net_return": mean,
        "mean_forward_net_return_ci_lower": mean_lower,
        "forward_hit_rate": hit,
        "forward_hit_rate_ci_lower": hit_lower,
        "failed_gates": ["test gate"] if state == "statistical_failure" else [],
    }


def test_mixed_alpha_lane_does_not_turn_one_failed_strategy_into_lane_failure():
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    rows = [
        _strategy(
            "failed_strategy",
            "statistical_failure",
            outcomes=30,
            local=30,
            mean=0.001,
            mean_lower=-0.001,
            hit=0.55,
            hit_lower=0.40,
        ),
        _strategy(
            "learning_strategy",
            "collecting",
            outcomes=5,
            local=5,
            mean=0.002,
            mean_lower=None,
            hit=0.60,
            hit_lower=None,
        ),
    ]

    reconciled = service._alpha_runtime_status(_status(), _lane(), rows)

    assert reconciled.state == "collecting"
    assert reconciled.independent_forward_outcome_count == 30
    assert "negative conclusions remain attached" in reconciled.primary_reason
    assert "failed_strategy" not in reconciled.primary_reason


def test_positive_alpha_evidence_with_one_source_is_provisional_not_allocatable():
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    rows = [
        _strategy(
            "positive_strategy",
            "certifying",
            outcomes=30,
            local=8,
            mean=0.004,
            mean_lower=0.001,
            hit=0.70,
            hit_lower=0.55,
        )
    ]

    reconciled = service._alpha_runtime_status(
        _status(state="certifying"),
        _lane(allocation=False, source_count=1),
        rows,
    )

    assert reconciled.state == "collecting"
    assert reconciled.stage == "provisional_forward_positive"
    assert "source redundancy" in reconciled.primary_reason
    assert "authoritative source redundancy target is not satisfied" in reconciled.blockers


def test_connected_source_class_gap_is_not_reported_as_provider_gap():
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    existing = _status("price_discrepancy", state="provider_gap")
    lane = _lane(
        "price_discrepancy",
        research=True,
        forward=False,
        allocation=False,
        missing=["executable_depth"],
        source_count=1,
    )

    reconciled = service._source_reconciled_status(existing, lane)

    assert reconciled.state == "collecting"
    assert reconciled.provider_ready is True
    assert reconciled.stage == "research_active_waiting_for_complete_forward_evidence"
    assert "executable_depth" in reconciled.blockers


def test_post_evidence_reconciliation_is_durable_only(monkeypatch):
    service = object.__new__(EvidenceVelocityAllLaneOperatingCertificationService)
    existing = _status("mean_reversion", state="statistical_failure")
    latest = OperatingCertificationSnapshot(
        observed_at=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        version="test",
        public_market_provider_healthy=True,
        public_market_surface_count=1,
        public_market_surface_ok_count=1,
        public_order_book_probe_count=1,
        public_order_book_probe_ok_count=1,
        market_quote_count=10,
        funding_quote_count=2,
        mechanism_count=1,
        provider_gap_count=0,
        collecting_count=0,
        poor_economics_count=0,
        blocked_count=1,
        certifying_count=0,
        certified_count=0,
        mechanisms=[existing],
    )

    class Ledger:
        def __init__(self):
            self.recorded = []

        def latest(self):
            return latest

        def record(self, snapshot):
            self.recorded.append(snapshot)

    class SourcePlane:
        @staticmethod
        def lane(lane_id):
            return _lane(lane_id)

    service.ledger = Ledger()
    service.source_coverage = SourcePlane()
    service.store = object()
    # Deliberately provide no provider/live-scan methods on core. If reconciliation
    # accidentally performs network/live certification work this test will fail.
    service.core = SimpleNamespace(settings=SimpleNamespace())
    monkeypatch.setattr(
        runtime_module,
        "_load_strategy_evidence",
        lambda store, settings: {
            "mean_reversion": [
                _strategy(
                    "still_learning",
                    "collecting",
                    outcomes=7,
                    local=4,
                    mean=0.001,
                    hit=0.57,
                )
            ]
        },
    )

    corrected = service.reconcile_latest_runtime_truth()

    assert corrected is not None
    assert corrected.snapshot_id != latest.snapshot_id
    assert corrected.observed_at > latest.observed_at
    assert corrected.mechanisms[0].state == "collecting"
    assert len(service.ledger.recorded) == 1


def test_disposable_worker_reconciles_before_research_projection():
    from inefficiency_engine import disposable_research_worker

    import inspect

    source = inspect.getsource(disposable_research_worker.run_disposable_research_cycle)
    reconcile_at = source.index("reconcile_latest_runtime_truth")
    publish_at = source.index("research_projection.publish")
    assert reconcile_at < publish_at
