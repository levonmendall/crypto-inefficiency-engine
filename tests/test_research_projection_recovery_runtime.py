from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from inefficiency_engine import lightweight_portfolio_worker
from inefficiency_engine import research_projection_recovery_runtime as recovery
from inefficiency_engine.dashboard_projection import DASHBOARD_RESEARCH_PROJECTION_WORKER_ID


class _Store:
    def __init__(self) -> None:
        self.heartbeats: list[dict[str, object]] = []

    def record_worker_heartbeat(self, **kwargs) -> None:
        self.heartbeats.append(dict(kwargs))


@pytest.mark.asyncio
async def test_projection_initializer_failure_is_retried_and_then_publishes(monkeypatch):
    store = _Store()
    stop = asyncio.Event()
    attempts = {"init": 0, "publish": 0}

    class FlakyLedger:
        def __init__(self, _store) -> None:
            attempts["init"] += 1
            if attempts["init"] == 1:
                raise RuntimeError("temporary schema inspection failure")

        def publish(self, **_kwargs):
            attempts["publish"] += 1
            stop.set()
            return {"observed_at": "2026-08-24T21:30:00+00:00"}

    settings = SimpleNamespace(
        alpha_min_forward_samples=30,
        operating_certification_min_settled_trials=20,
        shadow_horizons_seconds=(1.0, 5.0),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=10,
        worker_heartbeat_stale_seconds=180.0,
    )
    monkeypatch.setattr(recovery, "ResearchDashboardProjectionLedger", FlakyLedger)
    monkeypatch.setattr(recovery, "RESEARCH_PROJECTION_MAINTENANCE_SECONDS", 0.01)

    await recovery.resilient_research_projection_refresh_loop(
        store,
        settings=settings,
        stop_event=stop,
    )

    assert attempts == {"init": 2, "publish": 1}
    assert store.heartbeats[0]["worker_id"] == DASHBOARD_RESEARCH_PROJECTION_WORKER_ID
    assert store.heartbeats[0]["state"] == "degraded"
    assert store.heartbeats[0]["error_type"] == "RuntimeError"
    assert store.heartbeats[-1]["state"] == "success"
    assert store.heartbeats[-1]["detail"]["projection_observed_at"] == (
        "2026-08-24T21:30:00+00:00"
    )


def test_install_replaces_only_research_projection_refresh_coroutine(monkeypatch):
    marker = recovery._PATCH_MARKER
    monkeypatch.delattr(lightweight_portfolio_worker, marker, raising=False)
    original = lightweight_portfolio_worker._research_projection_refresh_loop
    try:
        recovery.install_research_projection_recovery_runtime()
        assert (
            lightweight_portfolio_worker._research_projection_refresh_loop
            is recovery.resilient_research_projection_refresh_loop
        )
    finally:
        lightweight_portfolio_worker._research_projection_refresh_loop = original
        monkeypatch.delattr(lightweight_portfolio_worker, marker, raising=False)


def test_production_wrappers_install_retry_and_expose_projection_heartbeats():
    portfolio_wrapper = Path(
        "src/inefficiency_engine/lightweight_portfolio_worker_bounded_heartbeat.py"
    ).read_text()
    api_wrapper = Path(
        "src/inefficiency_engine/read_api_bounded_heartbeat_deploy.py"
    ).read_text()

    assert "install_research_projection_recovery_runtime()" in portfolio_wrapper
    assert '"dashboard_projection": "dashboard-projection-publisher"' in api_wrapper
    assert '"research_projection": "dashboard-research-projection-publisher"' in api_wrapper
    assert "alpha_min_forward_samples" not in api_wrapper
    assert "live_execution_authority" not in api_wrapper
