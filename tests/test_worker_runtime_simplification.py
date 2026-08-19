from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import inefficiency_engine.cli as cli
import inefficiency_engine.threaded_worker as threaded_worker
import inefficiency_engine.worker_children as worker_children


def test_cli_import_does_not_eagerly_load_heavy_worker_stacks():
    code = """
import sys
import inefficiency_engine.cli
blocked = [
    'inefficiency_engine.worker_children',
    'inefficiency_engine.portfolio_stage_isolation',
    'inefficiency_engine.service',
]
loaded = [name for name in blocked if name in sys.modules]
if loaded:
    raise SystemExit('eager heavy imports: ' + ','.join(loaded))
"""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing else f"src{os.pathsep}{existing}"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_render_worker_command_keeps_main_thread_provider_free():
    source = inspect.getsource(cli.main)

    assert "settings, store = _settings_and_store()" in source
    assert "from inefficiency_engine.threaded_worker import run_threaded_worker" in source
    assert "asyncio.run(run_threaded_worker(store, settings=settings))" in source
    assert "supervise_worker_processes" not in source
    worker_block = source.split('if args.command == "worker":', 1)[1].split("service, store = _service()", 1)[0]
    assert "_service()" not in worker_block


@pytest.mark.asyncio
async def test_threaded_runtime_puts_research_and_portfolio_on_daemon_threads(monkeypatch):
    observed: list[dict[str, object]] = []
    heartbeats: list[tuple[str, str]] = []

    class FakeThread:
        def __init__(self, *, target, name, daemon):
            self.record = {"target": target, "name": name, "daemon": daemon}
            observed.append(self.record)

        def start(self):
            self.record["started"] = True

    class FakeStore:
        def record_worker_heartbeat(self, *, worker_id, state, **kwargs):
            heartbeats.append((worker_id, state))

    monkeypatch.setattr(threaded_worker.threading, "Thread", FakeThread)
    monkeypatch.setattr(
        threaded_worker,
        "recover_stale_portfolio_on_supervisor_startup",
        lambda *args, **kwargs: False,
    )

    stop_event = asyncio.Event()
    stop_event.set()
    stats = await threaded_worker.run_threaded_worker(
        FakeStore(),  # type: ignore[arg-type]
        settings=SimpleNamespace(shadow_cycle_interval_seconds=30.0),  # type: ignore[arg-type]
        stop_event=stop_event,
    )

    assert [item["name"] for item in observed] == [
        threaded_worker.RESEARCH_THREAD_NAME,
        threaded_worker.PORTFOLIO_THREAD_NAME,
    ]
    assert all(item["daemon"] is True for item in observed)
    assert all(item["started"] is True for item in observed)
    assert stats.worker_id == threaded_worker.THREAD_SUPERVISOR_WORKER_ID
    assert heartbeats[0] == (threaded_worker.THREAD_SUPERVISOR_WORKER_ID, "running")
    assert heartbeats[-1] == (threaded_worker.THREAD_SUPERVISOR_WORKER_ID, "stopped")


def test_portfolio_watchdog_distinguishes_running_budget_from_normal_idle_cadence():
    now = datetime.now(timezone.utc)
    started = now - timedelta(seconds=500)
    settings = SimpleNamespace(shadow_cycle_interval_seconds=30.0)

    class FakeStore:
        heartbeat = None

        def latest_worker_heartbeat(self, worker_id):
            assert worker_id == threaded_worker.PORTFOLIO_WORKER_ID
            return self.heartbeat

    store = FakeStore()
    store.heartbeat = SimpleNamespace(
        observed_at=now - timedelta(seconds=301),
        state="running",
    )
    reason, age, timeout = threaded_worker._portfolio_watchdog_reason(
        store,  # type: ignore[arg-type]
        settings=settings,  # type: ignore[arg-type]
        supervisor_started_at=started,
        now=now,
    )
    assert reason is not None
    assert age == pytest.approx(301.0)
    assert timeout == pytest.approx(300.0)

    store.heartbeat = SimpleNamespace(
        observed_at=now - timedelta(seconds=350),
        state="success",
    )
    reason, age, timeout = threaded_worker._portfolio_watchdog_reason(
        store,  # type: ignore[arg-type]
        settings=settings,  # type: ignore[arg-type]
        supervisor_started_at=started,
        now=now,
    )
    assert reason is None
    assert age == pytest.approx(350.0)
    assert timeout == pytest.approx(360.0)


@pytest.mark.asyncio
async def test_portfolio_child_uses_direct_bounded_services_without_stage_proxies(monkeypatch):
    created: dict[str, object] = {}

    class FakeUniversal:
        def __init__(self, service):
            created["universal"] = self

    class FakeComposite:
        def __init__(self, service, *, universal):
            created["composite"] = self

    class FakeAlpha:
        def __init__(self, service, store):
            created["alpha"] = self

    class FakePromotion:
        def __init__(self, service, composite, store):
            created["promotion"] = self

    class FakeAllocator:
        def __init__(self, service, promotion, alpha):
            created["allocator"] = self

    class FakePortfolio:
        def __init__(self, service, allocator, store):
            created["portfolio"] = self
            created["portfolio_service"] = service
            created["portfolio_allocator"] = allocator

    class FakeAllocationCertification:
        def __init__(self, service, allocator, store):
            created["allocation_certification"] = self

    class FakeOperatingCertification:
        def __init__(self, service, store, alpha, allocation_certification, *, version):
            created["operating_certification"] = self
            created["version"] = version

    async def fake_loop(service, store, **kwargs):
        created["loop_portfolio"] = kwargs["portfolio"]
        created["loop_allocation_certification"] = kwargs["allocation_certification"]
        created["loop_operating_certification"] = kwargs["operating_certification"]
        return 1

    monkeypatch.setattr(worker_children, "UniversalOpportunityService", FakeUniversal)
    monkeypatch.setattr(worker_children, "CexDexCompositeEvidenceService", FakeComposite)
    monkeypatch.setattr(worker_children, "ExpandedAlphaFactoryService", FakeAlpha)
    monkeypatch.setattr(worker_children, "CexDexPaperPromotionService", FakePromotion)
    monkeypatch.setattr(worker_children, "UnifiedPaperAllocatorService", FakeAllocator)
    monkeypatch.setattr(worker_children, "OperationallyResilientPaperPortfolioService", FakePortfolio)
    monkeypatch.setattr(worker_children, "AllocationForwardCertificationService", FakeAllocationCertification)
    monkeypatch.setattr(worker_children, "OperatingCertificationService", FakeOperatingCertification)
    monkeypatch.setattr(worker_children, "run_portfolio_operating_loop", fake_loop)
    monkeypatch.setattr(worker_children, "_stop_event", lambda: object())

    service = SimpleNamespace(settings=SimpleNamespace())
    store = object()
    attempted = await worker_children.run_portfolio_child(service, store)  # type: ignore[arg-type]

    assert attempted == 1
    assert created["portfolio_service"] is service
    assert created["portfolio_allocator"] is created["allocator"]
    assert created["loop_portfolio"] is created["portfolio"]
    assert created["loop_allocation_certification"] is created["allocation_certification"]
    assert created["loop_operating_certification"] is created["operating_certification"]
    assert created["version"] == worker_children.__version__
