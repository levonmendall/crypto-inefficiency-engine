import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.operating_worker import run_operating_bundle_once
from inefficiency_engine.portfolio_integrity import PortfolioIntegritySnapshot


class FakeAccount:
    def __init__(self, nav: float = 250000.0, open_position_count: int = 0):
        self.nav_usd = nav
        self.open_position_count = open_position_count
        self.observed_at = datetime.now(timezone.utc)


class FakeLedger:
    def __init__(self, nav: float = 250000.0):
        self.current = FakeAccount(nav)
        self.latest = FakeAccount(nav)
        self.recorded_snapshots: list[object] = []

    def current_state(self, *, observed_at=None):
        row = FakeAccount(self.current.nav_usd, self.current.open_position_count)
        if observed_at is not None:
            row.observed_at = observed_at
        return row

    def latest_snapshot(self):
        return self.latest

    def record_snapshot(self, snapshot):
        self.latest = snapshot
        self.recorded_snapshots.append(snapshot)


class FakeIntegrity:
    def __init__(self):
        self.latest_row: PortfolioIntegritySnapshot | None = None
        self.rows: list[PortfolioIntegritySnapshot] = []

    def latest(self):
        return self.latest_row

    def record(self, row: PortfolioIntegritySnapshot):
        self.latest_row = row
        self.rows.append(row)


class FakePortfolio:
    def __init__(self, *, fail: bool = False, degraded: bool = False):
        self.ledger = FakeLedger()
        self.integrity = FakeIntegrity()
        self.fail = fail
        self.degraded = degraded
        self.calls = 0

    async def run_cycle(self):
        self.calls += 1
        if self.fail:
            raise RuntimeError("portfolio failed")
        self.ledger.latest = FakeAccount(250123.0)
        self.integrity.record(PortfolioIntegritySnapshot(
            account_snapshot_at=self.ledger.latest.observed_at,
            market_evidence_at=datetime.now(timezone.utc),
            valuation_status="fresh",
            cycle_status="degraded" if self.degraded else "success",
            allocation_family_failures=(
                [{
                    "family": "cex_dex",
                    "error_type": "ConnectionError",
                    "reason": "CEX↔DEX candidate family failed closed",
                }]
                if self.degraded else []
            ),
        ))
        return SimpleNamespace(cycle_id="portfolio-cycle", nav_usd=250123.0)


class HangingPortfolio(FakePortfolio):
    async def run_cycle(self):
        self.calls += 1
        await asyncio.Event().wait()


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


class HangingAllocationCertification(FakeAllocationCertification):
    async def run_cycle(self, *, total_capital_usd: float):
        self.calls += 1
        assert total_capital_usd > 0
        await asyncio.Event().wait()


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


class HangingOperatingCertification(FakeOperatingCertification):
    async def run_cycle(self, *, total_capital_usd: float):
        self.calls += 1
        self.capitals.append(total_capital_usd)
        await asyncio.Event().wait()


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
async def test_degraded_family_is_visible_without_blocking_portfolio_result():
    portfolio = FakePortfolio(degraded=True)
    allocation = FakeAllocationCertification()
    operating = FakeOperatingCertification()

    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
    )

    assert result.nav_usd == 250123.0
    assert result.errors == {"portfolio_cycle_degraded": "family_failure"}
    assert result.portfolio_cycle is not None
    assert result.fallback_snapshot_recorded is False


@pytest.mark.asyncio
async def test_portfolio_failure_records_explicit_integrity_fallback_and_keeps_operating_status_running():
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
    assert len(portfolio.integrity.rows) == 1
    fallback = portfolio.integrity.rows[0]
    assert fallback.valuation_status == "cash_only"
    assert fallback.cycle_status == "failed"
    assert fallback.fallback_snapshot is True
    assert fallback.cycle_error_type == "RuntimeError"


@pytest.mark.asyncio
async def test_hung_allocation_certification_times_out_after_portfolio_snapshot_advances():
    portfolio = FakePortfolio()
    allocation = HangingAllocationCertification()
    operating = FakeOperatingCertification()

    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
        allocation_certification_timeout_seconds=0.01,
    )

    assert portfolio.calls == 1
    assert portfolio.ledger.latest.nav_usd == 250123.0
    assert result.portfolio_cycle is not None
    assert result.nav_usd == 250123.0
    assert result.errors == {"allocation_certification_error_type": "TimeoutError"}
    assert operating.calls == 1
    assert operating.capitals == [250123.0]


@pytest.mark.asyncio
async def test_hung_portfolio_stage_times_out_and_records_fresh_fallback_snapshot():
    portfolio = HangingPortfolio()
    allocation = FakeAllocationCertification()
    operating = FakeOperatingCertification()

    before = datetime.now(timezone.utc)
    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
        portfolio_timeout_seconds=0.01,
    )

    assert result.errors == {"portfolio_error_type": "TimeoutError"}
    assert result.fallback_snapshot_recorded is True
    assert portfolio.ledger.recorded_snapshots
    fallback_account = portfolio.ledger.recorded_snapshots[-1]
    assert fallback_account.observed_at >= before
    assert portfolio.integrity.rows[-1].cycle_error_type == "TimeoutError"
    assert allocation.calls == 1
    assert operating.calls == 1


@pytest.mark.asyncio
async def test_hung_operating_certification_is_bounded_after_accounting():
    portfolio = FakePortfolio()
    allocation = FakeAllocationCertification()
    operating = HangingOperatingCertification()

    result = await run_operating_bundle_once(
        portfolio=portfolio,  # type: ignore[arg-type]
        allocation_certification=allocation,  # type: ignore[arg-type]
        operating_certification=operating,  # type: ignore[arg-type]
        operating_certification_timeout_seconds=0.01,
    )

    assert result.portfolio_cycle is not None
    assert result.nav_usd == 250123.0
    assert result.errors == {"operating_certification_error_type": "TimeoutError"}
    assert operating.capitals == [250123.0]
