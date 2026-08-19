from types import SimpleNamespace

import pytest

from inefficiency_engine.operating_worker import run_operating_bundle_once


class FakeLedger:
    def __init__(self, nav: float = 250000.0):
        self.current = SimpleNamespace(nav_usd=nav)
        self.latest = SimpleNamespace(nav_usd=nav)
        self.recorded_snapshots: list[object] = []

    def current_state(self, *, observed_at=None):
        if observed_at is None:
            return self.current
        return SimpleNamespace(nav_usd=self.current.nav_usd, observed_at=observed_at)

    def latest_snapshot(self):
        return self.latest

    def record_snapshot(self, snapshot):
        self.latest = snapshot
        self.recorded_snapshots.append(snapshot)


class FakePortfolio:
    def __init__(self, *, fail: bool = False):
        self.ledger = FakeLedger()
        self.fail = fail
        self.calls = 0

    async def run_cycle(self):
        self.calls += 1
        if self.fail:
            raise RuntimeError("portfolio failed")
        self.ledger.latest = SimpleNamespace(nav_usd=250123.0)
        return SimpleNamespace(cycle_id="portfolio-cycle", nav_usd=250123.0)


class FakeAllocationCertification:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = 0

    async def run_cycle(self, *, total_capital_usd: float):
        self.calls += 1
        assert total_capital_usd > 0
        if self.fail:
            raise ValueError("allocation certification failed")
        return SimpleNamespace(cycle_id="allocation-cycle")


class FakeOperatingCertification:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = 0
        self.capitals: list[float] = []

    async def run_cycle(self, *, total_capital_usd: float):
        self.calls += 1
        self.capitals.append(total_capital_usd)
        if self.fail:
            raise LookupError("operating certification failed")
        return SimpleNamespace(cycle_id="operating-cycle")


@pytest.mark.asyncio
async def test_allocation_certification_failure_does_not_block_portfolio_or_operating_status():
    portfolio = FakePortfolio()
    allocation = FakeAllocationCertification(fail=True)
    operating = FakeOperatingCertification()

    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
    )

    assert allocation.calls == 1
    assert portfolio.calls == 1
    assert operating.calls == 1
    assert result.nav_usd == 250123.0
    assert result.errors == {"allocation_certification_error_type": "ValueError"}
    assert result.portfolio_cycle is not None
    assert result.operating_cycle is not None
    assert result.fallback_snapshot_recorded is False


@pytest.mark.asyncio
async def test_portfolio_failure_records_fresh_unchanged_snapshot_and_keeps_operating_status_running():
    portfolio = FakePortfolio(fail=True)
    allocation = FakeAllocationCertification()
    operating = FakeOperatingCertification()

    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
    )

    assert portfolio.calls == 1
    assert operating.calls == 1
    assert operating.capitals == [250000.0]
    assert result.nav_usd == 250000.0
    assert result.errors == {"portfolio_error_type": "RuntimeError"}
    assert result.operating_cycle is not None
    assert result.fallback_snapshot_recorded is True
    assert len(portfolio.ledger.recorded_snapshots) == 1
    assert portfolio.ledger.recorded_snapshots[0].nav_usd == 250000.0
