from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

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
