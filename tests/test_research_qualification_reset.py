from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_factory import AlphaQualification
from inefficiency_engine.candidate_observatory_runtime import (
    CandidateObservedAllLaneEvidenceFactoryService,
)
from inefficiency_engine.models import MarketKind
from inefficiency_engine.research_reset_runtime import (
    HIT_RATE_BLOCKER,
    RESEARCH_RESET_POLICY_VERSION,
    ResearchResetAllLaneEvidenceFactoryService,
    ResearchResetPolicy,
    _scientific_checkpoint,
)


def qualification(*, blockers, statistically_qualified=False):
    return AlphaQualification(
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        sample_count=30,
        positive_count=13,
        hit_rate=13 / 30,
        hit_rate_ci_lower=0.27,
        mean_realized_net_return=0.003,
        mean_realized_net_return_ci_lower=0.0012,
        p10_realized_net_return=-0.01,
        worst_realized_net_return=-0.02,
        regime_count=2,
        regime_means={"normal": 0.002, "high_vol": 0.004},
        multiple_testing_penalty_return=0.0004,
        required_mean_lower_bound=0.0009,
        statistically_qualified=statistically_qualified,
        blockers=list(blockers),
        paper_allocation_authority=statistically_qualified,
        live_execution_authority=False,
        paper_only=True,
    )


def service_without_init():
    service = object.__new__(ResearchResetAllLaneEvidenceFactoryService)
    service.research_reset_policy = ResearchResetPolicy()
    return service


def test_expectancy_primary_lane_does_not_require_hit_rate(monkeypatch):
    base = qualification(blockers=[HIT_RATE_BLOCKER])
    monkeypatch.setattr(
        CandidateObservedAllLaneEvidenceFactoryService,
        "qualification",
        lambda self, candidate: base,
    )
    monkeypatch.setattr(
        ResearchResetAllLaneEvidenceFactoryService,
        "_lane_for_candidate",
        lambda self, candidate: "trend_momentum",
    )
    result = service_without_init().qualification(SimpleNamespace())
    assert result.statistically_qualified is True
    assert result.paper_allocation_authority is True
    assert HIT_RATE_BLOCKER not in result.blockers
    assert result.hit_rate == pytest.approx(13 / 30)


def test_hit_rate_primary_lane_keeps_hit_rate_gate(monkeypatch):
    base = qualification(blockers=[HIT_RATE_BLOCKER])
    monkeypatch.setattr(
        CandidateObservedAllLaneEvidenceFactoryService,
        "qualification",
        lambda self, candidate: base,
    )
    monkeypatch.setattr(
        ResearchResetAllLaneEvidenceFactoryService,
        "_lane_for_candidate",
        lambda self, candidate: "mean_reversion",
    )
    result = service_without_init().qualification(SimpleNamespace())
    assert result.statistically_qualified is False
    assert HIT_RATE_BLOCKER in result.blockers


def test_all_costed_net_hurdle_rejections_are_shadow_eligible(monkeypatch):
    observation = SimpleNamespace(
        stage="net_hurdle_rejected",
        direction="long",
        estimated_cost_return=0.004,
        expected_net_return=-0.003,
        entry_reference_price=100.0,
        diagnostic_shadow_eligible=False,
    )

    def parent_discover(self, snapshot, *, total_capital_usd):
        self._last_candidate_observations = [observation]
        return []

    monkeypatch.setattr(
        CandidateObservedAllLaneEvidenceFactoryService,
        "discover",
        parent_discover,
    )
    service = service_without_init()
    assert service.discover(SimpleNamespace(), total_capital_usd=100_000.0) == []
    assert observation.diagnostic_shadow_eligible is True


def test_provisional_gate_uses_expectancy_not_hit_rate():
    service = service_without_init()
    q = qualification(blockers=[HIT_RATE_BLOCKER])
    q.sample_count = 12
    q.regime_count = 1
    q.mean_realized_net_return_ci_lower = 0.0003
    q.multiple_testing_penalty_return = 0.0004
    q.hit_rate = 0.35
    q.hit_rate_ci_lower = 0.15
    eligible, blockers, required = service._provisional_statistical_gate(SimpleNamespace(), q)
    assert required == pytest.approx(0.0002)
    assert eligible is True
    assert blockers == []

    q.sample_count = 11
    eligible, blockers, _ = service._provisional_statistical_gate(SimpleNamespace(), q)
    assert eligible is False
    assert "insufficient forward samples for provisional paper" in blockers


def test_verified_fee_override_blends_maker_and_taker(monkeypatch):
    monkeypatch.setattr(
        CandidateObservedAllLaneEvidenceFactoryService,
        "_one_way_fee_bps",
        lambda self, venue, market_kind: 10.0,
    )
    monkeypatch.setenv("CIE_EXECUTION_TAKER_FEE_BPS_OKX_SPOT", "8")
    monkeypatch.setenv("CIE_EXECUTION_MAKER_FEE_BPS_OKX_SPOT", "2")
    monkeypatch.setenv("CIE_EXECUTION_EXPECTED_MAKER_FRACTION_OKX_SPOT", "0.5")
    service = service_without_init()
    assert service._one_way_fee_bps("OKX", MarketKind.SPOT) == pytest.approx(5.0)
    assert "CIE_EXECUTION_TAKER_FEE_BPS_OKX_SPOT" in service.configured_execution_fee_overrides()


def test_scientific_checkpoint_changes_strategy_only_after_enough_evidence():
    policy = ResearchResetPolicy(
        decision_min_total_outcomes=120,
        decision_min_outcomes_per_strategy=20,
    )
    state, _ = _scientific_checkpoint(
        total_outcomes=119,
        strategy_summaries=[],
        policy=policy,
    )
    assert state == "collect_more_evidence"

    state, _ = _scientific_checkpoint(
        total_outcomes=120,
        strategy_summaries=[
            {"strategy_id": "a", "outcome_count": 60, "mean_realized_net_return": -0.001},
            {"strategy_id": "b", "outcome_count": 60, "mean_realized_net_return": -0.002},
        ],
        policy=policy,
    )
    assert state == "strategy_universe_change_recommended"

    state, _ = _scientific_checkpoint(
        total_outcomes=120,
        strategy_summaries=[
            {"strategy_id": "a", "outcome_count": 60, "mean_realized_net_return": 0.001},
            {"strategy_id": "b", "outcome_count": 60, "mean_realized_net_return": -0.002},
        ],
        policy=policy,
    )
    assert state == "concentrate_on_positive_strategies"
    assert RESEARCH_RESET_POLICY_VERSION == "research-reset-v1"
