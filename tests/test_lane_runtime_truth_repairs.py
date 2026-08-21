from __future__ import annotations

from types import SimpleNamespace

import pytest

from inefficiency_engine.all_lane_alpha_factory import AllLaneEvidenceFactoryService
from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.governed_mechanism_execution import (
    GovernedMechanismExecutionService,
    _GovernedMechanismLedgerView,
)


def _outcome(*, method: str, detail: dict[str, object]):
    return SimpleNamespace(
        settlement_evidence_complete=True,
        settlement_method=method,
        detail=detail,
        realized_net_return=0.01,
    )


def test_modeled_liquidation_recovery_is_not_allocation_grade():
    modeled = _outcome(
        method="latency_adjusted_capture_probability_recovery_shadow",
        detail={
            "capture_assumed": False,
            "paper_capture_probability_model": True,
        },
    )
    empirical = _outcome(
        method="empirical_liquidation_fill_and_settlement",
        detail={
            "capture_assumed": True,
            "paper_capture_probability_model": False,
        },
    )

    class Ledger:
        def outcomes(self, *, cohort_key=None, mechanism_id=None):
            del cohort_key, mechanism_id
            return [modeled, empirical]

    view = _GovernedMechanismLedgerView(
        Ledger(),
        GovernedMechanismExecutionService._liquidation_outcome_is_allocation_grade,
    )

    assert view.outcomes(mechanism_id="liquidation_distress") == [empirical]
    # Other mechanism reads are not filtered by liquidation semantics.
    assert view.outcomes(mechanism_id="yield") == [modeled, empirical]


@pytest.mark.asyncio
async def test_disposable_cycle_shares_independent_l2_with_native_mechanisms(monkeypatch):
    original_evidence_calls = 0

    async def original_evidence():
        nonlocal original_evidence_calls
        original_evidence_calls += 1
        return SimpleNamespace(name="quote-only")

    async def original_executability():
        raise AssertionError(
            "native mechanisms must not fall back to structural-opportunity-dependent executability"
        )

    core = SimpleNamespace(
        collect_live_evidence=original_evidence,
        collect_live_executability=original_executability,
    )
    service = DisposableExpandedAlphaFactoryService.__new__(
        DisposableExpandedAlphaFactoryService
    )
    service.core = core
    sampled = SimpleNamespace(name="bounded-independent-l2")
    sampler_calls = 0

    async def fake_sampler(collector):
        nonlocal sampler_calls
        sampler_calls += 1
        assert collector is original_evidence
        await collector()
        return sampled

    service._collect_alpha_l2_snapshot = fake_sampler

    async def fake_all_lane_cycle(self, *, total_capital_usd=None):
        del total_capital_usd
        alpha_snapshot = await self.core.collect_live_evidence()
        mechanism_snapshot = await self.core.collect_live_executability()
        assert alpha_snapshot is sampled
        assert mechanism_snapshot is sampled
        assert alpha_snapshot is mechanism_snapshot
        return "shared"

    monkeypatch.setattr(
        AllLaneEvidenceFactoryService,
        "run_evidence_cycle",
        fake_all_lane_cycle,
    )

    result = await DisposableExpandedAlphaFactoryService.run_evidence_cycle(service)

    assert result == "shared"
    assert sampler_calls == 1
    assert original_evidence_calls == 1
    assert core.collect_live_evidence is original_evidence
    assert core.collect_live_executability is original_executability
