from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import event

from inefficiency_engine import disposable_heavy_job, permanent_source_worker
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.provider_gap_collection import ProviderCatalogLedger
from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService
from inefficiency_engine.source_runtime_safety import (
    install_bulk_provider_catalog_runtime,
    install_research_source_delegation,
)


NOW = datetime(2026, 8, 23, 3, 30, tzinfo=timezone.utc)


def test_catalog_refresh_uses_bounded_database_round_trips(tmp_path):
    store = EvidenceStore(tmp_path / "catalog-bulk.sqlite")
    ledger = ProviderCatalogLedger(store)
    install_bulk_provider_catalog_runtime()

    statements: list[str] = []

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if "provider_gap_catalog_items" in statement:
            statements.append(statement)

    event.listen(store.engine, "before_cursor_execute", before_cursor_execute)
    try:
        items = [
            {
                "category": "spot",
                "symbol": f"ASSET{i}-USD",
                "asset": f"ASSET{i}",
                "launch_time_ms": None,
            }
            for i in range(250)
        ]
        baseline, new_items = ledger.observe(
            provider="coinbase-exchange:product-catalog",
            items=items,
            observed_at=NOW,
            source_reference="https://api.exchange.coinbase.com/products",
        )
        assert baseline is True
        assert len(new_items) == 250
        # One existing-key read plus one executemany insert; allow one dialect-level
        # extra statement without permitting the old one-query-per-product behavior.
        assert len(statements) <= 3

        statements.clear()
        baseline, new_items = ledger.observe(
            provider="coinbase-exchange:product-catalog",
            items=items,
            observed_at=NOW,
            source_reference="https://api.exchange.coinbase.com/products",
        )
        assert baseline is False
        assert new_items == []
        assert len(statements) <= 2
    finally:
        event.remove(store.engine, "before_cursor_execute", before_cursor_execute)


@pytest.mark.asyncio
async def test_research_delegates_source_refresh_to_current_permanent_owner(tmp_path):
    store = EvidenceStore(tmp_path / "source-owner.sqlite")
    store.record_worker_heartbeat(
        worker_id="canonical-source-operating-loop",
        state="running",
        detail={"stage": "provider_cycle_in_progress"},
    )
    coverage = SimpleNamespace(
        lane_count=13,
        sufficient_lane_count=6,
        insufficient_lane_count=7,
        research_eligible_lane_count=3,
        forward_test_eligible_lane_count=2,
        allocation_source_qualified_lane_count=0,
        priority_order=["volatility", "carry"],
    )
    service = object.__new__(PrioritySourceCollectionService)
    service.store = store
    service.source_coverage = SimpleNamespace(snapshot=lambda: coverage)

    install_research_source_delegation()
    payload = await service.run_cycle()

    assert payload["source_refresh"]["state"] == "delegated_to_permanent_source"
    assert payload["source_refresh"]["permanent_source_owner_current"] is True
    assert payload["source_coverage"]["lane_count"] == 13
    # Research delegation must not impersonate or overwrite the permanent source
    # worker's own source-refresh heartbeat.
    assert store.latest_worker_heartbeat("priority-source-refresh-plane") is None


def test_runtime_entrypoints_install_source_safety_contracts():
    source_worker = inspect.getsource(permanent_source_worker.run_permanent_source_worker)
    heavy_worker = inspect.getsource(disposable_heavy_job._run)

    assert "install_bulk_provider_catalog_runtime()" in source_worker
    assert "install_bulk_provider_catalog_runtime()" in heavy_worker
    assert "install_research_source_delegation()" in heavy_worker
