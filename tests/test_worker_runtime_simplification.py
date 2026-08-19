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
async def test_threaded_runtime_bootstraps_canonical_before_single_auxiliary_thread(monkeypatch):
    observed: list[dict[str, object]] = []
    heartbeats: list[tuple[str, str]] = []
    stop_event = asyncio.Event()

    class FakeThread:
        def __init__(self, *, target, name, daemon):
            self.record = {"target": target, "name": name, "daemon": daemon, "alive": True}
            observed.append(self.record)

        def start(self):
            self.record["started"] = True
            if self.record["name"] == threaded_worker.RESEARCH_THREAD_NAME:
                stop_event.set()

        def is_alive(self):
            return bool(self.record["alive"])

    class FakeStore:
        def record_worker_heartbeat(self, *, worker_id, state, **kwargs):
            heartbeats.append((worker_id, state))

        def latest_worker_heartbeat(self, worker_id):
            return None

    async def fake_bootstrap(*args, **kwargs):
        assert [item["name"] for item in observed] == [threaded_worker.PORTFOLIO_THREAD_NAME]
        return SimpleNamespace(
            state="success",
            observed_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(threaded_worker.threading, "Thread", FakeThread)
    monkeypatch.setattr(threaded_worker, "_wait_for_canonical_bootstrap", fake_bootstrap)
    monkeypatch.setattr(
        threaded_worker,
        "recover_stale_portfolio_on_supervisor_startup",
        lambda *args, **kwargs: False,
    )

    stats = await threaded_worker.run_threaded_worker(
        FakeStore(),  # type: ignore[arg-type]
        settings=SimpleNamespace(shadow_cycle_interval_seconds=30.0),  # type: ignore[arg-type]
        stop_event=stop_event,
    )

    assert [item["name"] for item in observed] == [
        threaded_worker.PORTFOLIO_THREAD_NAME,
        threaded_worker.RESEARCH_THREAD_NAME,
    ]
    assert all(item["daemon"] is True for item in observed)
    assert all(item["started"] is True for item in observed)
    assert stats.worker_id == threaded_worker.THREAD_SUPERVISOR_WORKER_ID
    assert heartbeats[0] == (threaded_worker.THREAD_SUPERVISOR_WORKER_ID, "starting")
    assert (threaded_worker.THREAD_SUPERVISOR_WORKER_ID, "running") in heartbeats
    assert heartbeats[-1] == (threaded_worker.THREAD_SUPERVISOR_WORKER_ID, "stopped")


def test_portfolio_watchdog_covers_accounting_only_budget_and_normal_idle_cadence():
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
        observed_at=now - timedelta(seconds=181),
        state="running",
    )
    reason, age, timeout = threaded_worker._portfolio_watchdog_reason(
        store,  # type: ignore[arg-type]
        settings=settings,  # type: ignore[arg-type]
        supervisor_started_at=started,
        now=now,
    )
    assert reason is not None
    assert "accounting-only" in reason
    assert age == pytest.approx(181.0)
    assert timeout == pytest.approx(180.0)

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
async def test_portfolio_child_contains_no_forward_certification_work(monkeypatch):
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

    class FakeCanonicalAllocator:
        def __init__(self, service, promotion, alpha):
            created["canonical_allocator"] = self

    class FakePortfolio:
        def __init__(self, service, allocator, store):
            created["portfolio"] = self
            created["portfolio_allocator"] = allocator

    async def fake_canonical_loop(service, store, **kwargs):
        created["loop_portfolio"] = kwargs["portfolio"]
        return 1

    monkeypatch.setattr(worker_children, "UniversalOpportunityService", FakeUniversal)
    monkeypatch.setattr(worker_children, "CexDexCompositeEvidenceService", FakeComposite)
    monkeypatch.setattr(worker_children, "ExpandedAlphaFactoryService", FakeAlpha)
    monkeypatch.setattr(worker_children, "CexDexPaperPromotionService", FakePromotion)
    monkeypatch.setattr(worker_children, "CanonicalPortfolioAllocatorService", FakeCanonicalAllocator)
    monkeypatch.setattr(worker_children, "OperationallyResilientPaperPortfolioService", FakePortfolio)
    monkeypatch.setattr(worker_children, "run_canonical_portfolio_loop", fake_canonical_loop)
    monkeypatch.setattr(worker_children, "_stop_event", lambda: object())

    service = SimpleNamespace(settings=SimpleNamespace())
    store = object()
    attempted = await worker_children.run_portfolio_child(service, store)  # type: ignore[arg-type]

    assert attempted == 1
    assert created["portfolio_allocator"] is created["canonical_allocator"]
    assert created["loop_portfolio"] is created["portfolio"]
    source = inspect.getsource(worker_children.run_portfolio_child)
    assert "UnifiedPaperAllocatorService" not in source
    assert "AllocationForwardCertificationService" not in source
    assert "OperatingCertificationService" not in source


def test_research_auxiliary_retains_full_certification_surface_without_third_thread():
    source = inspect.getsource(worker_children.run_research_child)
    supervisor_source = inspect.getsource(threaded_worker.run_threaded_worker)

    assert "AllocationForwardCertificationService" in source
    assert "OperatingCertificationService" in source
    assert "run_certification_loop" in source
    assert "allocation_certification_runner=certification_cycle" in source
    assert "CERTIFICATION_THREAD_NAME" not in supervisor_source
    assert "certification_thread" not in supervisor_source


@pytest.mark.asyncio
async def test_manual_certification_child_still_retains_full_mechanism_surface(monkeypatch):
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

    class FakeAllocationCertification:
        def __init__(self, service, allocator, store):
            created["allocation_certification"] = self
            created["certification_allocator"] = allocator

    class FakeOperatingCertification:
        def __init__(self, service, store, alpha, allocation_certification, *, version):
            created["operating_certification"] = self
            created["version"] = version

    async def fake_certification_loop(service, store, **kwargs):
        created["loop_allocation_certification"] = kwargs["allocation_certification"]
        created["loop_operating_certification"] = kwargs["operating_certification"]
        return 1

    monkeypatch.setattr(worker_children, "UniversalOpportunityService", FakeUniversal)
    monkeypatch.setattr(worker_children, "CexDexCompositeEvidenceService", FakeComposite)
    monkeypatch.setattr(worker_children, "ExpandedAlphaFactoryService", FakeAlpha)
    monkeypatch.setattr(worker_children, "CexDexPaperPromotionService", FakePromotion)
    monkeypatch.setattr(worker_children, "UnifiedPaperAllocatorService", FakeAllocator)
    monkeypatch.setattr(worker_children, "AllocationForwardCertificationService", FakeAllocationCertification)
    monkeypatch.setattr(worker_children, "OperatingCertificationService", FakeOperatingCertification)
    monkeypatch.setattr(worker_children, "run_certification_loop", fake_certification_loop)
    monkeypatch.setattr(worker_children, "_stop_event", lambda: object())

    service = SimpleNamespace(settings=SimpleNamespace())
    store = object()
    attempted = await worker_children.run_certification_child(service, store)  # type: ignore[arg-type]

    assert attempted == 1
    assert created["certification_allocator"] is created["allocator"]
    assert created["loop_allocation_certification"] is created["allocation_certification"]
    assert created["loop_operating_certification"] is created["operating_certification"]
    assert created["version"] == worker_children.__version__
