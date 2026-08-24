from __future__ import annotations

import asyncio
import inspect
import threading
import time
from types import SimpleNamespace

import pytest

from inefficiency_engine import canonical_control_plane_runtime
from inefficiency_engine import permanent_control_worker
from inefficiency_engine.bounded_control_evidence_runtime import _refresh_rows
from inefficiency_engine.control_cycle_runtime import (
    ControlCycleDeadlineExceeded,
    hard_control_cycle_deadline,
    hard_control_deadline_supported,
    install_control_pool_checkout_timeout,
)


def test_canonical_reconciliation_never_uses_executor_threads():
    source = inspect.getsource(canonical_control_plane_runtime.refresh_canonical_control_plane)
    worker_source = inspect.getsource(permanent_control_worker._run)

    assert "asyncio.to_thread" not in source
    assert "asyncio.wait_for" not in worker_source
    assert "hard_control_cycle_deadline" in worker_source
    assert '"reconciliation_executor_threads": 0' in worker_source


def test_hard_deadline_interrupts_blocking_reconciliation_without_orphan_thread():
    if not hard_control_deadline_supported():
        pytest.skip("SIGALRM wall-clock deadline is unavailable on this platform")

    class BlockingOperatingCertification:
        def reconcile_latest_runtime_truth(self):
            time.sleep(0.25)
            raise AssertionError("hard deadline failed to interrupt reconciliation")

    class Unused:
        pass

    settings = SimpleNamespace(
        alpha_min_forward_samples=3,
        operating_certification_min_settled_trials=20,
        shadow_horizons_seconds=(60.0,),
        shadow_cycle_interval_seconds=30.0,
        alpha_evidence_every_cycles=1,
        worker_heartbeat_stale_seconds=600.0,
    )
    baseline = {thread.ident for thread in threading.enumerate()}

    async def invoke() -> None:
        with hard_control_cycle_deadline(0.05):
            await canonical_control_plane_runtime.refresh_canonical_control_plane(
                store=Unused(),
                operating_certification=BlockingOperatingCertification(),
                qualified_bridge=Unused(),
                research_projection=Unused(),
                settings=settings,
            )

    started = time.monotonic()
    with pytest.raises(ControlCycleDeadlineExceeded):
        asyncio.run(invoke())
    elapsed = time.monotonic() - started

    assert elapsed < 0.20
    assert {thread.ident for thread in threading.enumerate()} == baseline


def test_postgres_pool_checkout_wait_is_bounded_below_control_deadline():
    pool = SimpleNamespace(_timeout=30.0)
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"), pool=pool)
    store = SimpleNamespace(engine=engine)

    assert install_control_pool_checkout_timeout(store, timeout_seconds=5.0) is True
    assert pool._timeout == 5.0


def test_outcome_cache_uses_incremental_primary_key_tail():
    source = inspect.getsource(_refresh_rows)

    assert "table.c.id > prior_tail" in source
    assert 'state["rows"].extend' in source
    assert 'state["tail"] = tail_id' in source


def test_control_installs_bounded_mechanism_and_allocator_history():
    source = inspect.getsource(permanent_control_worker._run)

    assert "install_bounded_control_outcome_ledgers()" in source
    assert '"mechanism_evidence_read_mode": "initial_exact_history_plus_incremental_tail"' in source
    assert '"database_pool_checkout_timeout_enforced"' in source
