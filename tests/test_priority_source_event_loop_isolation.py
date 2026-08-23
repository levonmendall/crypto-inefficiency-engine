from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

from inefficiency_engine.priority_source_collection import PrioritySourceCollectionService


def test_source_freshness_reuses_point_in_time_latest_rows():
    service = object.__new__(PrioritySourceCollectionService)

    def unexpected_latest():
        raise AssertionError("per-source TTL check must not re-read PostgreSQL")

    service.source_coverage = SimpleNamespace(
        ledger=SimpleNamespace(latest=unexpected_latest),
    )
    latest = {
        ("source-a", "lane-a"): SimpleNamespace(
            healthy=True,
            observed_at=datetime.now(timezone.utc),
        )
    }

    assert service._source_is_fresh(
        "source-a",
        ["lane-a"],
        300.0,
        latest=latest,
    ) is True


def test_priority_source_cycle_offloads_blocking_reconciliation():
    source = inspect.getsource(PrioritySourceCollectionService.run_cycle)
    l2_source = inspect.getsource(PrioritySourceCollectionService._refresh_l2_source_if_due)

    assert "await asyncio.to_thread(self.source_coverage.snapshot)" in source
    assert "await asyncio.to_thread(self.source_coverage.ledger.latest)" in source
    assert "await asyncio.to_thread(\n            prioritize_source_probes" in source
    assert "await asyncio.to_thread(\n            stagnation_diagnostics" in source
    assert "await asyncio.to_thread(\n            self._record_refresh_heartbeat" in source
    assert "await asyncio.to_thread(self._l2_source_refresh_due)" in l2_source

    # Source acquisition still owns no investment or live-execution authority.
    assert '"qualification_thresholds_unchanged": True' in inspect.getsource(
        PrioritySourceCollectionService._record_refresh_heartbeat
    )
