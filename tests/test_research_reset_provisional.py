from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_factory import AlphaCandidate, AlphaQualification
from inefficiency_engine.models import MarketKind
from inefficiency_engine.research_reset_runtime import (
    ResearchResetAllLaneEvidenceFactoryService,
    ResearchResetPolicy,
)


NOW = datetime(2026, 8, 21, 22, 0, tzinfo=timezone.utc)


def candidate() -> AlphaCandidate:
    return AlphaCandidate(
        candidate_id="candidate-1",
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        venue="OKX",
        market_kind=MarketKind.SPOT,
        symbol="BTC-USDT",
        observed_at=NOW,
        horizon_hours=6.0,
        lookback_hours=24.0,
        entry_reference_price=100.0,
        expected_gross_return=0.0010,
        estimated_cost_return=0.0005,
        expected_net_return=0.0005,
        expected_profit_usd=5.0,
        notional_usd=10_000.0,
        capital_required_usd=10_000.0,
        confidence_score=0.75,
        regime="normal",
    )


def provisional_qualification() -> AlphaQualification:
    return AlphaQualification(
        strategy_id="time_series_momentum_v1",
        family="directional_time_series",
        asset="BTC",
        direction="long",
        sample_count=12,
        positive_count=5,
        hit_rate=5 / 12,
        hit_rate_ci_lower=0.18,
        mean_realized_net_return=0.0010,
        mean_realized_net_return_ci_lower=0.0004,
        p10_realized_net_return=-0.005,
        worst_realized_net_return=-0.01,
        regime_count=1,
        regime_means={"normal": 0.0010},
        multiple_testing_penalty_return=0.0004,
        required_mean_lower_bound=0.0009,
        statistically_qualified=False,
        blockers=["insufficient correlation-adjusted independent forward samples"],
        paper_allocation_authority=False,
        live_execution_authority=False,
        paper_only=True,
    )


@pytest.mark.asyncio
async def test_provisional_candidate_enters_tiny_paper_tier_without_full_source_redundancy():
    service = object.__new__(ResearchResetAllLaneEvidenceFactoryService)
    service.research_reset_policy = ResearchResetPolicy()
    service.settings = SimpleNamespace(alpha_min_notional_usd=1000.0)
    service.source_plane = SimpleNamespace(snapshot=lambda now: object())
    service._costed_research_candidates = lambda: [candidate()]
    service.qualification = lambda item: provisional_qualification()
    service.strategy_health = lambda item: SimpleNamespace(
        healthy_for_paper_allocation=True,
        health_score=0.80,
    )
    service._source_gate = lambda item, source_snapshot: SimpleNamespace(
        forward_test_eligible=True,
        allocation_source_qualified=False,
    )
    service._snapshot_book = lambda item, snapshot: object()
    service._cost_from_book = lambda item, book: 0.0005
    service._holding_carry_cost = lambda item: 0.0

    rows = await service._generic_provisional_candidates(
        SimpleNamespace(completed_at=NOW),
        total_capital_usd=250_000.0,
        promoted=[],
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.candidate_id.startswith("provisional:")
    assert row.stage == "research"
    assert row.paper_allocation_eligible is True
    assert row.live_execution_eligible is False
    assert row.notional_usd == pytest.approx(1000.0)
    assert row.capital_required_usd == pytest.approx(1000.0)
    assert row.expected_net_return == pytest.approx(0.0004)
    assert row.features["qualification_tier"] == "provisional_paper"
    assert row.features["source_allocation_qualified"] is False
    assert row.features["full_allocation_source_redundancy_required"] is True
    assert row.features["live_execution_authority"] is False
