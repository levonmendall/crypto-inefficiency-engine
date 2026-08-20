from __future__ import annotations

import inspect

import inefficiency_engine.memory_bounded_research_worker as memory_worker
import inefficiency_engine.worker_children as worker_children


def test_auxiliary_runtime_uses_result_releasing_sequential_worker():
    child_source = inspect.getsource(worker_children.run_research_child)
    worker_source = inspect.getsource(memory_worker.run_memory_bounded_research_worker)

    assert "run_memory_bounded_research_worker" in child_source
    assert "run_shadow_worker(" not in child_source
    assert "asyncio.gather(*tasks" not in worker_source
    assert "await _run_and_release(route_shadow_runner" in worker_source
    assert "await _run_and_release(tier_shadow_runner" in worker_source
    assert "await _run_and_release(composite_shadow_runner" in worker_source
    assert "await _run_and_release(stablecoin_shadow_runner" in worker_source
    assert "await _run_and_release(allocation_certification_runner" in worker_source
    assert "await _run_and_release(alpha_runner" in worker_source
    assert "await _run_and_release(frontier_runner" in worker_source


def test_canonical_child_does_not_construct_unused_broad_research_graphs():
    source = inspect.getsource(worker_children.run_portfolio_child)

    assert "UniversalOpportunityService(" not in source
    assert "CexDexCompositeEvidenceService(" not in source
    assert "CexDexPaperPromotionService(" not in source
    assert "CanonicalPortfolioAllocatorService" in source
    assert "alpha_factory" in source
