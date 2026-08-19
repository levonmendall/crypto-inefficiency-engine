from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import BaseModel, Field

from inefficiency_engine.operating_worker import run_operating_bundle_once


class FakeSnapshot(BaseModel):
    nav_usd: float = 250000.0
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    open_position_count: int = 0
    market_evidence_observed_at: datetime | None = None
    valuation_status: str = "cash_only"
    cycle_status: str = "accounting_only"
    fallback_snapshot: bool = False
    cycle_error_type: str | None = None
    allocation_family_failures: dict[str, str] = Field(default_factory=dict)
    stale_position_count: int = 0


class FakeLedger:
    def __init__(self, nav: float = 250000.0):
        self.current = FakeSnapshot(nav_usd=nav)
        self.latest = FakeSnapshot(nav_usd=nav)
        self.recorded_snapshots: list[FakeSnapshot] = []

    def current_state(self, *, observed_at=None):
        return self.current.model_copy(update={
            "observed_at": observed_at or self.current.observed_at,
        })

    def latest_snapshot(self):
        return self.latest

    def record_snapshot(self, snapshot):
        self.latest = snapshot
        self.recorded_snapshots.append(snapshot)


class FakePortfolio:
    def __init__(self, *, fail: bool = False, degraded: bool = False):
        self.ledger = FakeLedger()
        self.fail = fail
        self.degraded = degraded
        self.calls = 0

    async def run_cycle(self):
        self.calls += 1
        if self.fail:
            raise RuntimeError("portfolio failed")
        self.ledger.latest = FakeSnapshot(
            nav_usd=250123.0,
            market_evidence_observed_at=datetime.now(timezone.utc),
            valuation_status="fresh",
            cycle_status="degraded" if self.degraded else "success",
            allocation_family_failures=(
                {"cex_dex": "ConnectionError: provider unavailable"}
                if self.degraded else {}
            ),
        )
        return SimpleNamespace(
            cycle_id="portfolio-cycle",
            nav_usd=250123.0,
            degraded=self.degraded,
            allocation_error_type=None,
            allocation_family_failures=(
                {"cex_dex": "ConnectionError: provider unavailable"}
                if self.degraded else {}
            ),
            stale_position_count=0,
        )


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
async def test_degraded_portfolio_family_is_visible_without_becoming_fatal():
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
async def test_portfolio_failure_records_explicit_stale_or_cash_only_fallback_and_keeps_operating_status_running():
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
    fallback = portfolio.ledger.recorded_snapshots[0]
    assert fallback.nav_usd == 250000.0
    assert fallback.valuation_status == "cash_only"
    assert fallback.cycle_status == "failed"
    assert fallback.fallback_snapshot is True
    assert fallback.cycle_error_type == "RuntimeError"
