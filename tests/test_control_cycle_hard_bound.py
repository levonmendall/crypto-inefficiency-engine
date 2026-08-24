from __future__ import annotations

import asyncio
import inspect
import threading
import time
from types import SimpleNamespace

import pytest
from pydantic import BaseModel
from sqlalchemy import Column, Integer, MetaData, Table, Text, insert

from inefficiency_engine import canonical_control_plane_runtime
from inefficiency_engine import control_cycle_executor
from inefficiency_engine import permanent_control_worker
from inefficiency_engine import bounded_control_evidence_runtime as bounded_control
from inefficiency_engine.bounded_control_evidence_runtime import (
    _bootstrap_batch_rows,
    _refresh_rows,
    bounded_control_outcome_cache_diagnostics,
)
from inefficiency_engine.control_cycle_runtime import (
    ControlCycleDeadlineExceeded,
    hard_control_cycle_deadline,
    hard_control_deadline_supported,
    install_control_pool_checkout_timeout,
)
from inefficiency_engine.evidence import EvidenceStore


def test_canonical_reconciliation_runs_in_killable_executor_process():
    source = inspect.getsource(canonical_control_plane_runtime.refresh_canonical_control_plane)
    worker_source = inspect.getsource(permanent_control_worker._run)

    assert "asyncio.to_thread" not in source
    assert "asyncio.wait_for" not in worker_source
    assert "ControlExecutorSupervisor" in worker_source
    assert "supervisor.run_cycle" in worker_source
    assert "hard_control_cycle_deadline" not in worker_source
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


def test_outcome_cache_default_batch_is_small_enough_for_disposable_preflight(monkeypatch):
    monkeypatch.delenv("CIE_CONTROL_OUTCOME_BOOTSTRAP_BATCH_ROWS", raising=False)

    assert _bootstrap_batch_rows() == 500


def test_outcome_cache_cold_start_is_batched_and_partial_history_is_never_exposed(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("CIE_CONTROL_OUTCOME_BOOTSTRAP_BATCH_ROWS", "100")
    monkeypatch.setattr(bounded_control, "_CACHE_CHECK_SECONDS", 0.0)
    store = EvidenceStore(tmp_path / "bounded-control.sqlite")
    metadata = MetaData()
    table = Table(
        "test_outcome_history",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(store.engine)
    with store.engine.begin() as db:
        db.execute(insert(table), [{"payload_json": str(index)} for index in range(250)])

    class Model:
        @classmethod
        def model_validate_json(cls, payload):
            return int(payload)

    ledger = SimpleNamespace(store=store)
    first = _refresh_rows(ledger, table, Model)
    assert first == []
    diagnostics = bounded_control_outcome_cache_diagnostics()
    row = diagnostics["tables"]["test_outcome_history"]
    assert row["bootstrap_complete"] is False
    assert row["processed_tail"] == 100
    assert row["target_tail"] == 250
    assert row["row_count"] == 100

    second = _refresh_rows(ledger, table, Model)
    assert second == []
    third = _refresh_rows(ledger, table, Model)
    assert third == list(range(250))
    row = bounded_control_outcome_cache_diagnostics()["tables"]["test_outcome_history"]
    assert row["bootstrap_complete"] is True
    assert row["processed_tail"] == 250
    assert row["row_count"] == 250


def test_outcome_cache_uses_bounded_incremental_primary_key_tail():
    source = inspect.getsource(_refresh_rows)

    assert "table.c.id > prior_tail" in source
    assert ".limit(batch_rows)" in source
    assert 'state["rows"].extend' in source
    assert 'state["bootstrap_complete"]' in source


def test_outcome_bootstrap_progress_survives_executor_process_cache_reset(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "test-control")
    monkeypatch.setenv("CIE_CONTROL_OUTCOME_BOOTSTRAP_BATCH_ROWS", "100")
    monkeypatch.setattr(bounded_control, "_CACHE_CHECK_SECONDS", 0.0)
    database = tmp_path / "durable-bounded-control.sqlite"
    bounded_control._CACHE.clear()
    store = EvidenceStore(database)
    metadata = MetaData()
    table = Table(
        "test_durable_outcome_history",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(store.engine)

    class Model(BaseModel):
        value: int

    with store.engine.begin() as db:
        db.execute(
            insert(table),
            [{"payload_json": Model(value=index).model_dump_json()} for index in range(250)],
        )
    ledger = SimpleNamespace(store=store)

    assert _refresh_rows(ledger, table, Model) == []
    first = bounded_control_outcome_cache_diagnostics()["tables"][
        "test_durable_outcome_history"
    ]
    assert first["processed_tail"] == 100

    store.engine.dispose()
    bounded_control._CACHE.clear()
    restarted_store = EvidenceStore(database)
    restarted_ledger = SimpleNamespace(store=restarted_store)
    assert _refresh_rows(restarted_ledger, table, Model) == []
    second = bounded_control_outcome_cache_diagnostics()["tables"][
        "test_durable_outcome_history"
    ]
    assert second["durable_checkpoint_loaded"] is True
    assert second["processed_tail"] == 200

    restarted_store.engine.dispose()
    bounded_control._CACHE.clear()
    final_store = EvidenceStore(database)
    rows = _refresh_rows(SimpleNamespace(store=final_store), table, Model)
    assert [row.value for row in rows] == list(range(250))


def test_partial_outcome_cache_returns_normal_fail_closed_control_payload():
    cache = {
        "complete": False,
        "strategy": {"all_caches_complete": True},
        "outcomes": {
            "all_caches_complete": False,
            "batch_rows": 500,
        },
    }

    payload = control_cycle_executor._cache_rebuilding_control_payload(cache)

    assert payload["operating_reconciliation_complete"] is False
    assert payload["qualified_bridge_publication_complete"] is False
    assert payload["research_projection_publication_complete"] is False
    assert payload["historical_cache_complete"] is False
    assert payload["control_plane_healthy"] is False
    assert payload["control_plane_errors"] == {
        "historical_evidence_cache": "HistoricalEvidenceCacheRebuilding"
    }


def test_control_installs_and_prewarms_bounded_outcome_history_before_reconciliation():
    source = inspect.getsource(control_cycle_executor.run_one_control_cycle)
    parent_source = inspect.getsource(permanent_control_worker._run)

    assert "install_bounded_control_outcome_ledgers()" in source
    assert "advance_bounded_control_outcome_caches(" in source
    assert 'stage_reporter("historical_outcome_cache_bootstrap")' in source
    assert 'write_stage("historical_outcome_cache_rebuilding"' in source
    assert "refresh_canonical_control_plane(" in source
    assert source.index("advance_bounded_control_outcome_caches(") < source.index(
        "refresh_canonical_control_plane("
    )
    assert "CIE_CONTROL_CACHE_NAMESPACE" in parent_source
    assert '"mechanism_evidence_read_mode"' in parent_source
    assert '"database_pool_checkout_timeout_enforced"' in parent_source
