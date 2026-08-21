from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.alpha_extensions import MeanReversionStrategy
from inefficiency_engine.alpha_refinements import (
    BtcRelativeResidualMeanReversionStrategy,
    CrossVenueResidualMeanReversionStrategy,
    LiquidityConditionedMeanReversionStrategy,
    MultiHorizonMeanReversionStrategy,
    OnChainFactorBreadthStrategy,
    TradeFlowLeadLagStrategy,
    VolatilityConditionedMeanReversionStrategy,
)
from inefficiency_engine.memory_bounded_research_worker import (
    ResearchStageTimeoutError,
    _capture,
    run_memory_bounded_research_worker,
)
from inefficiency_engine.operating_state_read import _lane_funnel
from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane
from inefficiency_engine.source_coverage_catalog import SOURCES
from inefficiency_engine.threaded_worker import (
    RESEARCH_PROJECTION_WORKER_ID,
    RESEARCH_WORKER_ID,
    SOURCE_REFRESH_WORKER_ID,
    _research_watchdog_reason,
)


NOW = datetime.now(timezone.utc)


class FakeStore:
    backend = "test"

    def __init__(self):
        self.heartbeats = []
        self.latest = {}

    def record_worker_heartbeat(self, *, worker_id, state, **kwargs):
        self.heartbeats.append((worker_id, state, kwargs))

    def latest_worker_heartbeat(self, worker_id):
        return self.latest.get(worker_id)


@pytest.mark.asyncio
async def test_independent_source_refresh_runs_before_heavy_core_research():
    order: list[str] = []

    class FakeService:
        settings = SimpleNamespace(
            worker_error_backoff_seconds=0.0,
            shadow_cycle_interval_seconds=0.0,
        )

        async def run_shadow_cycle(self):
            order.append("core")
            return SimpleNamespace(
                cycle_id="cycle-1",
                verification_scan_id="scan-1",
                observations=[],
            )

    async def refresh():
        order.append("source_refresh")
        return {
            "source_refresh": {"state": "success"},
            "source_coverage": {"sufficient_lane_count": 13},
        }

    await run_memory_bounded_research_worker(
        FakeService(),  # type: ignore[arg-type]
        FakeStore(),  # type: ignore[arg-type]
        worker_id="research-test",
        stop_event=asyncio.Event(),
        max_cycles=1,
        source_refresh_runner=refresh,
        source_refresh_every_cycles=1,
    )

    assert order == ["source_refresh", "core"]


@pytest.mark.asyncio
async def test_research_stage_timeout_is_explicit_not_silent():
    async def wedged():
        await asyncio.sleep(0.05)
        return object()

    result = await _capture(wedged, timeout_seconds=0.005, stage_name="provider_test")
    assert isinstance(result, ResearchStageTimeoutError)
    assert "provider_test" in str(result)


def test_research_watchdog_detects_alive_but_stale_progress():
    store = FakeStore()
    settings = SimpleNamespace(
        research_stage_timeout_seconds=120.0,
        worker_heartbeat_stale_seconds=180.0,
        shadow_cycle_interval_seconds=30.0,
    )
    started = NOW - timedelta(minutes=10)
    store.latest[RESEARCH_WORKER_ID] = SimpleNamespace(
        observed_at=NOW - timedelta(minutes=5),
        state="running",
        detail={"stage": "source_refresh"},
    )
    store.latest[SOURCE_REFRESH_WORKER_ID] = SimpleNamespace(
        observed_at=NOW - timedelta(seconds=30), state="success", detail={}
    )
    store.latest[RESEARCH_PROJECTION_WORKER_ID] = SimpleNamespace(
        observed_at=NOW - timedelta(seconds=30), state="success", detail={}
    )

    reason, ages, timeout = _research_watchdog_reason(
        store,  # type: ignore[arg-type]
        settings=settings,  # type: ignore[arg-type]
        research_started_at=started,
        now=NOW,
    )

    assert reason is not None
    assert "source_refresh" in reason
    assert ages["research_heartbeat_age_seconds"] == pytest.approx(300.0)
    assert timeout >= 180.0


def test_research_watchdog_accepts_fresh_three_clock_progress():
    store = FakeStore()
    settings = SimpleNamespace(
        research_stage_timeout_seconds=120.0,
        worker_heartbeat_stale_seconds=180.0,
        shadow_cycle_interval_seconds=30.0,
    )
    started = NOW - timedelta(minutes=5)
    for worker_id in (RESEARCH_WORKER_ID, SOURCE_REFRESH_WORKER_ID, RESEARCH_PROJECTION_WORKER_ID):
        store.latest[worker_id] = SimpleNamespace(
            observed_at=NOW - timedelta(seconds=20), state="success", detail={}
        )
    reason, _, _ = _research_watchdog_reason(
        store,  # type: ignore[arg-type]
        settings=settings,  # type: ignore[arg-type]
        research_started_at=started,
        now=NOW,
    )
    assert reason is None


def test_memory_deferred_refresh_can_reuse_still_fresh_truthful_source_observation(tmp_path):
    from inefficiency_engine.evidence import EvidenceStore

    store = EvidenceStore(tmp_path / "coverage.sqlite3")
    plane = SourceCoveragePlane(store)
    plane.record(SourceCoverageObservation(
        source_id="morpho-markets",
        lane_id="yield",
        observed_at=NOW,
        healthy=True,
        item_count=10,
        evidence_classes=["yield_rate", "capacity", "exit_liquidity"],
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
    ))
    service = PrioritySourceCollectionService.__new__(PrioritySourceCollectionService)
    service.source_coverage = plane
    assert service._source_is_fresh("morpho-markets", ["yield"], 300.0) is True


def test_public_trade_flow_is_an_active_source_surface():
    row = next(item for item in SOURCES if item["id"] == "public-trade-flow")
    assert row.get("active") is True
    assert set(row["lanes"]) == {"microstructure", "liquidity_provision"}


def test_reversion_repairs_are_independent_forward_cohorts():
    strategies = [
        MeanReversionStrategy(),
        CrossVenueResidualMeanReversionStrategy(),
        MultiHorizonMeanReversionStrategy(),
        VolatilityConditionedMeanReversionStrategy(),
        LiquidityConditionedMeanReversionStrategy(),
        BtcRelativeResidualMeanReversionStrategy(),
    ]
    ids = [strategy.manifest.strategy_id for strategy in strategies]
    assert ids[0] == "mean_reversion_v1"
    assert len(ids) == len(set(ids)) == 6
    assert all(strategy.manifest.paper_only for strategy in strategies)
    assert all(strategy.manifest.allocation_authority is False for strategy in strategies)


def test_additional_refinements_have_no_allocation_authority():
    assert TradeFlowLeadLagStrategy.manifest.allocation_authority is False
    assert OnChainFactorBreadthStrategy.manifest.allocation_authority is False
    assert TradeFlowLeadLagStrategy.manifest.paper_only is True
    assert OnChainFactorBreadthStrategy.manifest.paper_only is True


def test_lane_funnel_preserves_distinct_stage_counts():
    row = {
        "authoritative_observation_count": 12,
        "stage": "profitability_certifiable",
        "forward_signal_count": 8,
        "independent_forward_outcome_count": 5,
        "current_statistically_qualified_count": 2,
        "current_promoted_count": 1,
        "settled_allocator_outcome_count": 1,
    }
    funnel = _lane_funnel(row, [])
    assert funnel == {
        "authoritative_observations": 12,
        "economic_forward_signals": 8,
        "independent_forward_outcomes": 5,
        "currently_statistically_qualified": 2,
        "currently_execution_promoted": 1,
        "settled_allocator_outcomes": 1,
        "source_layer_sufficient": True,
        "diagnostic_only": True,
        "authority": False,
    }
