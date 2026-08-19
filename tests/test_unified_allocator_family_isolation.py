from types import SimpleNamespace

import pytest

from inefficiency_engine.unified_allocation import (
    UnifiedPaperAllocatorService,
    UnifiedPaperCandidate,
)


def candidate(candidate_id: str, family: str) -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id=candidate_id,
        family=family,
        strategy="test",
        asset="BTC",
        venues=["test-venue"],
        capital_required_usd=1000.0,
        notional_usd_per_leg=1000.0,
        expected_profit_usd_per_deployment=10.0,
        expected_return_on_reserved_capital=0.01,
        source_return_metric="test",
        source_return_value=0.01,
    )


class IsolatedAllocator(UnifiedPaperAllocatorService):
    def __init__(self, *, core_error=None, cex_dex_error=None, alpha_error=None):
        self.alpha_factory = object()
        self.core_error = core_error
        self.cex_dex_error = cex_dex_error
        self.alpha_error = alpha_error

    async def _core_family_candidates(self, *, total_capital_usd: float):
        if self.core_error is not None:
            raise self.core_error
        return SimpleNamespace(), [candidate("core", "core_cex")]

    async def _cex_dex_family_candidates(self, *, total_capital_usd: float):
        if self.cex_dex_error is not None:
            raise self.cex_dex_error
        return [candidate("cex-dex", "cex_dex")]

    async def _alpha_family_candidates(self, *, snapshot, total_capital_usd: float):
        if self.alpha_error is not None:
            raise self.alpha_error
        return [candidate("alpha", "alpha")]


@pytest.mark.asyncio
async def test_core_failure_does_not_block_cex_dex_family():
    allocator = IsolatedAllocator(core_error=RuntimeError("core provider unavailable"))

    rows, failures = await allocator._candidates_with_failures(total_capital_usd=250000.0)

    assert [row.candidate_id for row in rows] == ["cex-dex"]
    assert {item["family"] for item in failures} == {"core_cex", "alpha"}
    assert next(item for item in failures if item["family"] == "core_cex")["error_type"] == "RuntimeError"
    assert next(item for item in failures if item["family"] == "alpha")["error_type"] == "UpstreamSnapshotUnavailable"


@pytest.mark.asyncio
async def test_cex_dex_failure_does_not_block_core_or_alpha():
    allocator = IsolatedAllocator(cex_dex_error=ConnectionError("dex provider unavailable"))

    rows, failures = await allocator._candidates_with_failures(total_capital_usd=250000.0)

    assert {row.candidate_id for row in rows} == {"core", "alpha"}
    assert failures == [{
        "family": "cex_dex",
        "error_type": "ConnectionError",
        "reason": "CEX↔DEX candidate family failed closed",
    }]


@pytest.mark.asyncio
async def test_alpha_failure_does_not_block_structural_families():
    allocator = IsolatedAllocator(alpha_error=LookupError("alpha evidence unavailable"))

    rows, failures = await allocator._candidates_with_failures(total_capital_usd=250000.0)

    assert {row.candidate_id for row in rows} == {"core", "cex-dex"}
    assert failures == [{
        "family": "alpha",
        "error_type": "LookupError",
        "reason": "alpha candidate family failed closed",
    }]
